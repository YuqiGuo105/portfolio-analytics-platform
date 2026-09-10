package site.yuqi.analytics.alerts.service;

import com.sun.net.httpserver.HttpServer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.RestClient;
import site.yuqi.analytics.alerts.dto.AlertIncident;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.dto.AlertRuleRequest;
import site.yuqi.analytics.alerts.dto.NotificationDeliveryState;
import site.yuqi.analytics.alerts.operations.OperationEventPublisher;
import site.yuqi.analytics.alerts.repo.AlertIncidentRepository;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.alerts.web.AlertEvaluationController;
import site.yuqi.analytics.alerts.web.InternalTokenFilter;
import site.yuqi.analytics.common.event.Granularity;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Real PostgreSQL regressions: H2 and mocked JdbcTemplate cannot detect parameter type inference. */
@EnabledIfEnvironmentVariable(named = "ALERT_TEST_JDBC_URL", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AlertPostgresTest {
    private JdbcTemplate jdbc;
    private AlertIncidentRepository incidents;
    private AlertRuleRepository rules;
    private AlertRule rule;
    private AlertEvaluator evaluator;
    private MockMvc http;
    private HttpServer notificationServer;
    private final AtomicInteger notificationStatus = new AtomicInteger(202);
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final List<String> tokens = new CopyOnWriteArrayList<>();

    @BeforeAll
    void start() throws Exception {
        String url = System.getenv("ALERT_TEST_JDBC_URL");
        // Never run truncation fixtures against a shared or remote database.
        assertThat(url).matches("jdbc:postgresql://(127\\.0\\.0\\.1|localhost):[0-9]+/alerts_test");
        var ds = new DriverManagerDataSource(url, System.getenv("ALERT_TEST_DB_USER"),
                System.getenv("ALERT_TEST_DB_PASSWORD"));
        jdbc = new JdbcTemplate(ds);
        for (String role : List.of("anon", "authenticated")) {
            if (!Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from pg_roles where rolname=?)",
                    Boolean.class, role))) jdbc.execute("create role " + role);
        }
        Flyway.configure().dataSource(ds).load().migrate();
        jdbc.execute("""
                create table if not exists geo_time_rollups (
                  site_id text, bucket_time timestamptz, granularity text,
                  geo_level text, geo_area_id text, event_type text, event_count bigint)
                """);
        jdbc.execute("""
                create table if not exists behavior_events (
                  event_id text primary key, site_id text, event_name text, event_time timestamptz,
                  server_time timestamptz, page_path text, device_type text, browser text,
                  country text, region text, geo_area_id text)
                """);
        jdbc.execute("""
                create table if not exists geo_areas (
                  geo_area_id text primary key, geo_level text, name text)
                """);
        incidents = new AlertIncidentRepository(jdbc);
        rules = new AlertRuleRepository(jdbc, ds);
        notificationServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        notificationServer.createContext("/api/content-events", exchange -> {
            requests.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            tokens.add(exchange.getRequestHeaders().getFirst("X-Internal-Token"));
            exchange.sendResponseHeaders(notificationStatus.get(), -1);
            exchange.close();
        });
        notificationServer.start();
        var sender = new NotificationSender(RestClient.builder()
                .baseUrl("http://127.0.0.1:" + notificationServer.getAddress().getPort()).build());
        ReflectionTestUtils.setField(sender, "token", "notification-test-token");
        evaluator = new AlertEvaluator(rules, jdbc, sender, incidents, mock(OperationEventPublisher.class),
                new AlertNotificationDetails(jdbc, "UTC"));
        ReflectionTestUtils.setField(evaluator, "evalEnabled", true);
        ReflectionTestUtils.setField(evaluator, "notificationRetrySeconds", 60L);
        ReflectionTestUtils.setField(evaluator, "notificationRetryBatchSize", 3);
        ReflectionTestUtils.setField(evaluator, "notificationLeaseSeconds", 120L);
        ReflectionTestUtils.setField(evaluator, "notificationMaxAttempts", 3);
        ReflectionTestUtils.setField(evaluator, "lookbackBuckets", 2);
        http = MockMvcBuilders.standaloneSetup(new AlertEvaluationController(evaluator))
                .addFilters(new InternalTokenFilter("scheduler-test-token")).build();
    }

    @BeforeEach
    void reset() {
        jdbc.execute("truncate incidents, alert_rules, geo_time_rollups, behavior_events, geo_areas restart identity cascade");
        jdbc.update("insert into geo_areas values ('METRO:US:TX:Austin', 'METRO', 'Austin')");
        requests.clear();
        tokens.clear();
        notificationStatus.set(202);
        rule = rules.insert(new AlertRuleRequest("alerts-test", "Texas test", "page_view", "REGION",
                "REGION:US:TX", "5m", 1, ">=", 1800));
    }

    @AfterAll
    void stop() {
        if (notificationServer != null) notificationServer.stop(0);
    }

    @Test
    void emptyRetryQueueExecutesWithoutPostgresTypeError() {
        assertThat(incidents.claimPendingNotifications(Instant.now(), 3, 120)).isEmpty();
    }

    @Test
    void incidentEvidenceCannotBeReadByAnonymousOrOrdinarySignedInDatabaseRoles() {
        assertThat(jdbc.queryForObject("select relrowsecurity from pg_class where oid = 'public.incidents'::regclass",
                Boolean.class)).isTrue();
        for (String role : List.of("anon", "authenticated")) {
            assertThat(jdbc.queryForObject("select has_table_privilege(?, 'public.incidents', 'SELECT')",
                    Boolean.class, role)).isFalse();
        }
        assertThat(insert("server-access")).isNotNull();
    }

    @Test
    void timestampsSurviveClaimFailureRetryAndSuccess() {
        AlertIncident created = insert("retry");
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        AlertIncident claimed = incidents.claimNotification(created.incidentId(), now, 120).orElseThrow();
        assertThat(claimed.notificationLeaseUntil()).isEqualTo(now.plusSeconds(120));
        assertThat(incidents.recordNotificationResult(created.incidentId(), 1, false, now, 60, 3, "HTTP 503")).isTrue();
        assertThat(incidents.findByDedupKey("retry").orElseThrow().nextNotificationAttemptAt())
                .isEqualTo(now.plusSeconds(60));
        assertThat(incidents.claimPendingNotifications(now.plusSeconds(59), 3, 120)).isEmpty();
        var retried = incidents.claimPendingNotifications(now.plusSeconds(60), 3, 120);
        assertThat(retried).hasSize(1);
        assertThat(retried.getFirst().notificationAttempts()).isEqualTo(2);
        assertThat(incidents.recordNotificationResult(created.incidentId(), 2, true, now.plusSeconds(61), 60, 3, null)).isTrue();
        var delivered = incidents.findByDedupKey("retry").orElseThrow();
        assertThat(delivered.notificationState()).isEqualTo(NotificationDeliveryState.DELIVERED);
        assertThat(delivered.notifiedAt()).isEqualTo(now.plusSeconds(61));
        assertThat(delivered.notificationLeaseUntil()).isNull();
    }

    @Test
    void expiredLeaseCanBeReclaimedButStaleWorkerCannotRecordSuccess() {
        var created = insert("expired");
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        incidents.claimPendingNotifications(now, 3, 120);
        assertThat(incidents.claimPendingNotifications(now.plusSeconds(119), 3, 120)).isEmpty();
        assertThat(incidents.claimPendingNotifications(now.plusSeconds(120), 3, 120)).hasSize(1);
        assertThat(incidents.recordNotificationResult(created.incidentId(), 1, true, now, 60, 3, null)).isFalse();
        assertThat(incidents.recordNotificationResult(created.incidentId(), 2, true, now, 60, 3, null)).isTrue();
    }

    @Test
    void exhaustedRetryBecomesDeadLetter() {
        var created = insert("dead");
        Instant now = Instant.parse("2030-01-01T00:00:00Z");
        incidents.claimNotification(created.incidentId(), now, 120);
        incidents.recordNotificationResult(created.incidentId(), 1, false, now, 60, 1, "HTTP 503");
        assertThat(incidents.findByDedupKey("dead").orElseThrow().notificationState())
                .isEqualTo(NotificationDeliveryState.DEAD_LETTER);
        assertThat(incidents.claimPendingNotifications(now.plusSeconds(600), 3, 120)).isEmpty();
    }

    @Test
    void concurrentWorkersDoNotClaimTheSameIncident() throws Exception {
        insert("worker-one");
        insert("worker-two");
        try (var pool = Executors.newFixedThreadPool(2)) {
            var results = pool.<List<AlertIncident>>invokeAll(List.of(
                    () -> incidents.claimPendingNotifications(Instant.now().plusSeconds(1), 2, 120),
                    () -> incidents.claimPendingNotifications(Instant.now().plusSeconds(1), 2, 120)));
            List<Long> ids = new java.util.ArrayList<>();
            for (var result : results) {
                result.get().forEach(i -> ids.add(i.incidentId()));
            }
            assertThat(ids).hasSize(2).doesNotHaveDuplicates();
        }
    }

    @Test
    void scheduledRequestEvaluatesTexasAndDispatchesExactlyOnce() throws Exception {
        rollup(Granularity.FIVE_MIN.floor(Instant.now()), "5m", "REGION:US:TX", 2);
        evaluate();
        evaluate();
        assertThat(requests).hasSize(1);
        assertThat(requests.getFirst()).contains("ANALYTICS_ALERT_TRIGGERED", "ADMIN_ALERTS", "incident:1");
        assertThat(tokens).containsExactly("notification-test-token");
        assertThat(jdbc.queryForObject("select notification_state from incidents", String.class)).isEqualTo("DELIVERED");
    }

    @Test
    void notificationHttpFailureIsRetriedWithTheSameIdempotencyKey() throws Exception {
        rollup(Granularity.FIVE_MIN.floor(Instant.now()), "5m", "REGION:US:TX", 1);
        notificationStatus.set(503);
        evaluate();
        assertThat(jdbc.queryForObject("select notification_state from incidents", String.class)).isEqualTo("RETRY_WAIT");
        evaluate();
        assertThat(requests).hasSize(1);
        jdbc.update("update incidents set next_notification_attempt_at = ?", Timestamp.from(Instant.now().minusSeconds(1)));
        notificationStatus.set(202);
        evaluate();
        assertThat(requests).hasSize(2);
        assertThat(requests.get(1)).isEqualTo(requests.get(0));
        assertThat(jdbc.queryForObject("select notification_attempts from incidents", Integer.class)).isEqualTo(2);
    }

    @Test
    void protectedReplayUsesOnlyNewestMatchingFiveMinuteBucketAndDeduplicates() throws Exception {
        Instant to = Instant.now().minusSeconds(60);
        Instant from = to.minusSeconds(86400);
        Instant newest = Granularity.FIVE_MIN.floor(to.minusSeconds(3600));
        rollup(newest.minusSeconds(300), "5m", "REGION:US:TX", 7);
        rollup(newest, "5m", "REGION:US:TX", 2);
        rollup(newest.plusSeconds(300), "1d", "REGION:US:TX", 100);
        rollup(newest.plusSeconds(300), "5m", "REGION:US:CA", 100);
        String body = "{\"ruleId\":%d,\"from\":\"%s\",\"to\":\"%s\"}".formatted(rule.ruleId(), from, to);
        http.perform(post("/api/internal/alert-evaluation/replay").contentType("application/json").content(body))
                .andExpect(status().isUnauthorized());
        for (int i = 0; i < 2; i++) {
            http.perform(post("/api/internal/alert-evaluation/replay").header("X-Internal-Token", "scheduler-test-token")
                    .contentType("application/json").content(body)).andExpect(status().isAccepted());
        }
        jdbc.update("update incidents set created_at = ?", Timestamp.from(Instant.now().minusSeconds(3600)));
        http.perform(post("/api/internal/alert-evaluation/replay").header("X-Internal-Token", "scheduler-test-token")
                .contentType("application/json").content(body)).andExpect(status().isAccepted());
        assertThat(requests).hasSize(1);
        assertThat(jdbc.queryForObject("select measured_value from incidents", Long.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("select bucket_time from incidents", Timestamp.class).toInstant()).isEqualTo(newest);
    }

    @Test
    void invalidReplayIsRejectedWithoutSideEffects() throws Exception {
        String body = "{\"ruleId\":1,\"from\":\"2020-01-01T00:00:00Z\",\"to\":\"2026-01-01T00:00:00Z\"}";
        http.perform(post("/api/internal/alert-evaluation/replay").header("X-Internal-Token", "scheduler-test-token")
                .contentType("application/json").content(body)).andExpect(status().isBadRequest());
        assertThat(requests).isEmpty();
    }

    @Test
    void replayWithoutMatchingDataCreatesNothingAndDisabledRuleIsRejected() {
        Instant to = Instant.now().minusSeconds(60);
        assertThat(evaluator.replay(rule.ruleId(), to.minusSeconds(3600), to)).isFalse();
        rules.setEnabled(rule.ruleId(), false);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> evaluator.replay(rule.ruleId(), to.minusSeconds(3600), to))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(requests).isEmpty();
        assertThat(jdbc.queryForObject("select count(*) from incidents", Integer.class)).isZero();
    }

    private AlertIncident insert(String key) {
        return incidents.insert(rule, Granularity.FIVE_MIN.floor(Instant.now()), 1, key).orElseThrow();
    }

    @Test
    void detailsUseExactVisitTimeAndFrozenRuleThenRemainStableDuringRetry() {
        Instant bucket = Instant.parse("2026-09-10T16:25:00Z");
        visit("correct-event", "alerts-test", "page_view", bucket.plusSeconds(97), "US", "TX", "/cv");
        visit("wrong-state", "alerts-test", "page_view", bucket.plusSeconds(110), "US", "CA", "/wrong");
        visit("wrong-site", "other-site", "page_view", bucket.plusSeconds(110), "US", "TX", "/wrong");
        visit("wrong-type", "alerts-test", "click", bucket.plusSeconds(110), "US", "TX", "/wrong");
        visit("next-window", "alerts-test", "page_view", bucket.plusSeconds(300), "US", "TX", "/wrong");
        visit("prior-window", "alerts-test", "page_view", bucket.minusSeconds(1), "US", "TX", "/wrong");
        var incident = incidents.insert(rule, bucket, 1, "details").orElseThrow();
        jdbc.update("update alert_rules set geo_area_id = 'REGION:US:CA' where rule_id = ?", rule.ruleId());
        var details = new AlertNotificationDetails(jdbc, "America/Denver");
        String first = details.summaryFor(incident);
        assertThat(first).contains("Occurred at (event time): 2026-09-10 10:26:37 America/Denver",
                "Received at (server time): 2026-09-10 10:26:39 America/Denver",
                "Approximate location: Austin, TX, US", "Page: /cv", "Event ID: correct-event",
                "Condition: count >= 1; measured 1", "Alert detected at:", "not a street address")
                .doesNotContain("wrong-state", "wrong-site", "wrong-type", "next-window", "prior-window");
        jdbc.update("delete from behavior_events");
        assertThat(details.summaryFor(incident)).isEqualTo(first);
    }

    @Test
    void detailsBoundExamplesAndHandleMissingEvidenceWithoutInventingVisitTime() {
        Instant bucket = Instant.parse("2026-09-10T16:25:00Z");
        for (int n = 0; n < 6; n++) {
            visit("visit-" + n, "alerts-test", "page_view", bucket.plusSeconds(n), "US", "TX", "/");
        }
        var details = new AlertNotificationDetails(jdbc, "UTC");
        String summary = details.summaryFor(incidents.insert(rule, bucket, 6, "bounded").orElseThrow());
        assertThat(summary).contains("Event ID: visit-5", "Event ID: visit-4", "Event ID: visit-3", "measured 6")
                .doesNotContain("Event ID: visit-2", "Visit 4");
        String missing = details.summaryFor(incidents.insert(rule, bucket.plusSeconds(300), 1, "missing").orElseThrow());
        assertThat(missing).contains("No individual matching record is available")
                .doesNotContain("Occurred at (event time)", "Approximate location:");
    }

    @Test
    void detailsMatchCountryMetroAndWildcardRulesWithoutCrossingTheCohort() {
        Instant bucket = Instant.parse("2026-09-10T16:25:00Z");
        visit("austin-event", "alerts-test", "page_view", bucket.plusSeconds(1), "US", "TX", "/");
        visit("other-country", "alerts-test", "page_view", bucket.plusSeconds(2), "CA", "TX", "/wrong");
        for (String[] scope : List.of(new String[]{"COUNTRY", "COUNTRY:US"},
                new String[]{"METRO", "METRO:US:TX:Austin"}, new String[]{"REGION", "REGION:US:TX"})) {
            var scopedRule = rules.insert(new AlertRuleRequest("alerts-test", "Scoped", "page_view", scope[0],
                    scope[1], "5m", 1, ">=", 1800));
            String summary = new AlertNotificationDetails(jdbc, "UTC").summaryFor(
                    incidents.insert(scopedRule, bucket, 1, scope[0]).orElseThrow());
            assertThat(summary).contains("austin-event").doesNotContain("other-country");
        }
        var global = rules.insert(new AlertRuleRequest("alerts-test", "Global", "page_view", "GLOBAL",
                null, "5m", 1, ">=", 1800));
        assertThat(new AlertNotificationDetails(jdbc, "UTC").summaryFor(
                incidents.insert(global, bucket, 2, "global").orElseThrow()))
                .contains("austin-event", "other-country");
    }

    private void visit(String id, String site, String event, Instant occurred, String country, String region, String path) {
        jdbc.update("insert into behavior_events values (?, ?, ?, ?, ?, ?, 'desktop', 'Chrome', ?, ?, ?)",
                id, site, event, Timestamp.from(occurred), Timestamp.from(occurred.plusSeconds(2)), path,
                country, region, "METRO:" + country + ":" + region + ":Austin");
    }

    @Test
    void replayProducesStableDetailedHttpPayloadForNotificationEndToEndTest() throws Exception {
        Instant bucket = Granularity.FIVE_MIN.floor(Instant.now().minusSeconds(21600));
        rollup(bucket, "5m", "REGION:US:TX", 1);
        visit("replayed-austin-visit", "alerts-test", "page_view", bucket.plusSeconds(97), "US", "TX", "/cv");
        notificationStatus.set(503);
        assertThat(evaluator.replay(rule.ruleId(), bucket, bucket.plusSeconds(300))).isTrue();
        assertThat(requests).hasSize(1);
        jdbc.update("delete from behavior_events");
        jdbc.update("update incidents set next_notification_attempt_at = ?", Timestamp.from(Instant.now().minusSeconds(1)));
        notificationStatus.set(202);
        evaluator.retryPendingNotifications();
        assertThat(requests).hasSize(2);
        assertThat(requests.get(1)).isEqualTo(requests.get(0))
                .contains("replayed-austin-visit", "Austin, TX, US", "Occurred at (event time)", "/admin/visitors");
        var output = java.nio.file.Path.of("target/test-artifacts/visitor-alert-payload.json");
        java.nio.file.Files.createDirectories(output.getParent());
        java.nio.file.Files.writeString(output, requests.get(0));
    }

    private void rollup(Instant bucket, String granularity, String area, long count) {
        jdbc.update("insert into geo_time_rollups values (?, ?, ?, ?, ?, ?, ?)",
                rule.siteId(), Timestamp.from(bucket), granularity, "REGION", area, "page_view", count);
    }

    private void evaluate() throws Exception {
        http.perform(post("/api/internal/alert-evaluation").header("X-Internal-Token", "scheduler-test-token"))
                .andExpect(status().isAccepted());
    }
}
