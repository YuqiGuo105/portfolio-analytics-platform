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
        evaluator = new AlertEvaluator(rules, jdbc, sender, incidents, mock(OperationEventPublisher.class));
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
        jdbc.execute("truncate incidents, alert_rules, geo_time_rollups restart identity cascade");
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

    private void rollup(Instant bucket, String granularity, String area, long count) {
        jdbc.update("insert into geo_time_rollups values (?, ?, ?, ?, ?, ?, ?)",
                rule.siteId(), Timestamp.from(bucket), granularity, "REGION", area, "page_view", count);
    }

    private void evaluate() throws Exception {
        http.perform(post("/api/internal/alert-evaluation").header("X-Internal-Token", "scheduler-test-token"))
                .andExpect(status().isAccepted());
    }
}
