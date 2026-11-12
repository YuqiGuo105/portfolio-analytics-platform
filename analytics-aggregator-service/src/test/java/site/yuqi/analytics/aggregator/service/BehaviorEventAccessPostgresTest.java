package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.support.TransactionTemplate;
import site.yuqi.analytics.aggregator.web.*;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;
import site.yuqi.analytics.aggregator.kafka.RawEventEnvelope;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.ArgumentMatchers.anyList;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Executes the real Flyway upgrade and real role-scoped SQL, never against a shared database. */
@EnabledIfEnvironmentVariable(named = "ANALYTICS_TEST_JDBC_URL", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BehaviorEventAccessPostgresTest {
    private DriverManagerDataSource ds;
    private JdbcTemplate jdbc;
    private MockMvc http;

    @BeforeAll
    void start() {
        String url = System.getenv("ANALYTICS_TEST_JDBC_URL");
        assertThat(url).matches("jdbc:postgresql://(127\\.0\\.0\\.1|localhost):[0-9]+/analytics_test");
        ds = new DriverManagerDataSource(url, System.getenv("ANALYTICS_TEST_DB_USER"),
                System.getenv("ANALYTICS_TEST_DB_PASSWORD"));
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("drop schema if exists analytics_private cascade");
        jdbc.execute("drop schema public cascade");
        jdbc.execute("create schema public");
        jdbc.execute("grant usage on schema public to public");
        for (String role : List.of("anon", "authenticated", "behavior_public_reader", "service_role")) {
            if (!Boolean.TRUE.equals(jdbc.queryForObject(
                    "select exists(select 1 from pg_roles where rolname=?)", Boolean.class, role))) {
                jdbc.execute("create role " + role + (role.equals("service_role") ? " bypassrls" : ""));
            }
        }
        // Legacy table predates aggregator-owned migrations in production.
        jdbc.execute("""
                create table visitor_logs (
                  id bigserial primary key, ip text, local_time timestamptz, event text, ua text,
                  country text, region text, city text, latitude double precision,
                  longitude double precision, created_at timestamptz default now())
                """);
        jdbc.execute("create table public.\"Projects\" (id text primary key, updated_at timestamptz)");
        jdbc.execute("create table public.\"Blogs\" (id text primary key, title text)");
        jdbc.execute("create table public.life_blogs (id text primary key, updated_at timestamptz)");
        jdbc.execute("create table public.experience (id text primary key, name text)");
        flyway("14").migrate();
        // Reproduce Supabase's old table grants plus independently granted columns.
        jdbc.execute("grant all on behavior_events to public, anon, authenticated, service_role");
        jdbc.execute("grant select(event_id), insert(event_id), update(event_id), references(event_id) "
                + "on behavior_events to public, anon, authenticated");
        assertThat(jdbc.queryForObject("select has_table_privilege('anon', 'behavior_events', 'SELECT')",
                Boolean.class)).isTrue();
        flyway(null).migrate();
        flyway(null).validate();

        var mapper = new site.yuqi.analytics.aggregator.config.AggregatorBeansConfig().objectMapper();
        var cache = new ResponseCache(mock(StringRedisTemplate.class), mapper, false, 30, false, 5, 500, 40);
        var publicController = new PublicVisitsController(jdbc, cache);
        ReflectionTestUtils.setField(publicController, "siteId", "access-test");
        ReflectionTestUtils.setField(publicController, "minBucketCount", 5);
        var admin = new AdminVisitorsController(
                new VisitorQueryService(new NamedParameterJdbcTemplate(ds), mapper),
                "access-test", 31, 100, "/admin");
        http = MockMvcBuilders.standaloneSetup(publicController, admin)
                .addFilters(new AdminTokenFilter("test-internal-token")).build();
    }

    private Flyway flyway(String target) {
        var config = Flyway.configure().dataSource(ds).table("analytics_aggregator_flyway_history")
                .baselineOnMigrate(true).baselineVersion("0");
        if (target != null) config.target(target);
        return config.load();
    }

    @BeforeEach
    void seed() {
        jdbc.execute("truncate behavior_events, analytics_private.behavior_events_raw, visitor_logs restart identity");
        jdbc.execute("truncate analytics_kafka_inbox, analytics_visit_throttle");
        Instant time = Instant.now().minusSeconds(60);
        var raw = new RawEvent("access-event", "access-test", "page_view", time, time,
                "access-session", "access-anon", "https://example.test/project?private=value",
                null, null, "Chrome", "192.0.2.1", null);
        var enriched = new EnrichedEvent("access-event", "access-test", "page_view", time, time,
                "access-session", "hashed-anon", raw.pageUrl(), null, null,
                "desktop", "Chrome", "test-os", false, 0, "hashed-ip", null);
        var writer = new VisitorLogPersistService(jdbc, new ObjectMapper(),
                new site.yuqi.analytics.aggregator.governance.EvidenceReceiptWriter(jdbc,
                        new site.yuqi.analytics.aggregator.governance.GovernancePolicy(false, false, 90, 90, "access-test")));
        new TransactionTemplate(new DataSourceTransactionManager(ds)).executeWithoutResult(status -> {
            writer.persistBatch(List.of(raw), List.of(enriched));
            writer.persistBatch(List.of(raw), List.of(enriched));
        });
    }

    private KafkaEventBatchProcessor deliveryProcessor(SessionAggregatorService sessions) {
        var mapper = new site.yuqi.analytics.aggregator.config.AggregatorBeansConfig().objectMapper();
        var inbox = new KafkaInboxRepository(jdbc);
        ReflectionTestUtils.setField(inbox, "consumerGroup", "delivery-test");
        var throttle = new DurableVisitThrottle(jdbc);
        ReflectionTestUtils.setField(throttle, "throttleSeconds", 300L);
        if (sessions == null) {
            sessions = new SessionAggregatorService(jdbc);
            ReflectionTestUtils.setField(sessions, "configuredSteps", List.of("read_progress"));
        }
        var writer = new VisitorLogPersistService(jdbc, mapper,
                new site.yuqi.analytics.aggregator.governance.EvidenceReceiptWriter(jdbc,
                        new site.yuqi.analytics.aggregator.governance.GovernancePolicy(true, false, 90, 90, "access-test")));
        return new KafkaEventBatchProcessor(inbox, throttle, writer, sessions,
                new JourneyIntentProjectionService(jdbc, mapper),
                new RollupUpsertService(jdbc, new DurableUniqueSessionCounter(jdbc)));
    }

    private RawEventEnvelope deliveryEvent(String id, String type, int progress, long offset) {
        Instant now = Instant.now().minusSeconds(30);
        Map<String, Object> properties = type.equals("read_progress") ? Map.of("progressPercent", progress) : Map.of();
        var raw = new RawEvent(id, "access-test", type, now, now, "delivery-session", "delivery-anon",
                "/work-single/delivery", null, null, "Chrome", "192.0.2.2", null,
                2, "granted", "hash", properties);
        var enriched = new EnrichedEvent(id, "access-test", type, now, now, "delivery-session", "delivery-anon",
                raw.pageUrl(), null, null, "desktop", "Chrome", "macOS", false, 0.0, "hash", null,
                2, "granted", properties);
        return new RawEventEnvelope("analytics.raw.events", 0, offset, raw, enriched);
    }

    @Test
    void duplicateKafkaDeliveryKeepsAllProgressCheckpointsOnceAndAdminGroupsMaximum() throws Exception {
        var processor = deliveryProcessor(null);
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        var records = List.of(deliveryEvent("delivery-pv", "page_view", 0, 1),
                deliveryEvent("delivery-25", "read_progress", 25, 2),
                deliveryEvent("delivery-75", "read_progress", 75, 3),
                deliveryEvent("delivery-100", "read_progress", 100, 4));
        assertThat(tx.<Integer>execute(status -> processor.process(records))).isEqualTo(4);
        var replay = records.stream().map(r -> new RawEventEnvelope(r.topic(), r.partition(), r.offset() + 100,
                r.raw(), r.enriched())).toList();
        assertThat(tx.<Integer>execute(status -> processor.process(replay))).isZero();
        assertThat(tx.<Integer>execute(status -> processor.process(records))).isZero();
        assertThat(jdbc.queryForObject("select count(*) from analytics_kafka_inbox", Integer.class)).isEqualTo(4);
        assertThat(jdbc.queryForObject("select count(*) from behavior_events where session_id='delivery-session'", Integer.class)).isEqualTo(4);
        assertThat(jdbc.queryForObject("select count(*) from analytics_private.visitor_evidence_receipts where source_event_id like 'delivery-%'", Integer.class)).isEqualTo(4);
        assertThat(jdbc.queryForList("select (properties->>'progressPercent')::int from behavior_events where event_name='read_progress' order by 1", Integer.class))
                .containsExactly(25, 75, 100);
        Long rollupBefore = jdbc.queryForObject("select coalesce(sum(event_count),0) from geo_time_rollups", Long.class);
        jdbc.execute("delete from analytics_kafka_inbox");
        assertThat(tx.<Integer>execute(status -> processor.process(replay))).isZero();
        assertThat(jdbc.queryForObject("select coalesce(sum(event_count),0) from geo_time_rollups", Long.class)).isEqualTo(rollupBefore);
        http.perform(get("/api/admin/visitor-visits").header("X-Internal-Token", "test-internal-token")
                        .param("sessionId", "delivery-session"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items.length()").value(1))
                .andExpect(jsonPath("$.items[0].visit.eventCount").value(4))
                .andExpect(jsonPath("$.items[0].visit.maxProgressPercent").value(100));
    }

    @Test
    void failedProjectionRollsBackInboxFactsAndEvidenceSoReplayCanRecover() {
        var brokenSessions = mock(SessionAggregatorService.class);
        doThrow(new IllegalStateException("simulated database failure")).when(brokenSessions).processBatch(anyList());
        var broken = deliveryProcessor(brokenSessions);
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        var event = deliveryEvent("rollback-75", "read_progress", 75, 200);
        assertThrows(IllegalStateException.class, () -> tx.execute(status -> broken.process(List.of(event))));
        for (String table : List.of("analytics_kafka_inbox", "behavior_events", "analytics_private.behavior_events_raw")) {
            assertThat(jdbc.queryForObject("select count(*) from " + table + " where event_id='rollback-75'", Integer.class)).isZero();
        }
        assertThat(jdbc.queryForObject("select count(*) from analytics_private.visitor_evidence_receipts where source_event_id='rollback-75'", Integer.class)).isZero();
        var recovered = deliveryProcessor(null);
        assertThat(tx.<Integer>execute(status -> recovered.process(List.of(event)))).isEqualTo(1);
        assertThat(tx.<Integer>execute(status -> recovered.process(List.of(event)))).isZero();
        assertThat(jdbc.queryForObject("select count(*) from behavior_events where event_id='rollback-75'", Integer.class)).isEqualTo(1);
    }

    @Test
    void outboxConflictDoesNotResetAnAlreadyDeliveredEvent() {
        String sql = "insert into analytics_event_outbox(event_id,partition_key,payload) values ('outbox-retry','access-test',?::jsonb) on conflict(event_id) do nothing";
        jdbc.update(sql, "{\"eventId\":\"outbox-retry\",\"properties\":{\"progressPercent\":75}}");
        jdbc.update("update analytics_event_outbox set status='SENT' where event_id='outbox-retry'");
        assertThat(jdbc.update(sql, "{\"eventId\":\"outbox-retry\",\"properties\":{\"progressPercent\":25}}")).isZero();
        assertThat(jdbc.queryForObject("select status from analytics_event_outbox where event_id='outbox-retry'", String.class)).isEqualTo("SENT");
        assertThat(jdbc.queryForObject("select (payload->'properties'->>'progressPercent')::int from analytics_event_outbox where event_id='outbox-retry'", Integer.class)).isEqualTo(75);
    }

    @ParameterizedTest
    @ValueSource(strings = {"anon", "authenticated", "behavior_public_reader"})
    void clientsCannotReadOrMutateFacts(String role) throws Exception {
        assertThat(jdbc.queryForObject("select relrowsecurity from pg_class where oid='behavior_events'::regclass",
                Boolean.class)).isTrue();
        for (String privilege : List.of("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")) {
            assertThat(jdbc.queryForObject("select has_table_privilege(?, 'behavior_events', ?)",
                    Boolean.class, role, privilege)).isFalse();
        }
        for (String privilege : List.of("SELECT", "INSERT", "UPDATE", "REFERENCES")) {
            assertThat(jdbc.queryForObject("select has_any_column_privilege(?, 'behavior_events', ?)",
                    Boolean.class, role, privilege)).isFalse();
        }
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            statement.execute("set role " + role);
            for (String sql : List.of("select * from behavior_events", "select event_id from behavior_events",
                    "insert into behavior_events(event_id) values ('forged')",
                    "update behavior_events set page_path='/forged'", "delete from behavior_events",
                    "truncate behavior_events")) {
                assertThat(assertThrows(SQLException.class, () -> statement.execute(sql)).getSQLState())
                        .as(role + ": " + sql).isEqualTo("42501");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"anon", "authenticated"})
    void rlsRemainsDenyByDefaultIfTableGrantsAreAccidentallyReintroduced(String role) throws Exception {
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            try {
                statement.execute("grant select, insert, update, delete on behavior_events to " + role);
                statement.execute("set local role " + role);
                try (var rows = statement.executeQuery("select count(*) from behavior_events")) {
                    rows.next();
                    assertThat(rows.getInt(1)).isZero();
                }
                assertThat(statement.executeUpdate("update behavior_events set page_path='/forged'")).isZero();
                assertThat(statement.executeUpdate("delete from behavior_events")).isZero();
                assertThat(assertThrows(SQLException.class, () -> statement.execute("""
                        insert into behavior_events(event_id,site_id,event_name,event_time,server_time)
                        values ('forged','access-test','page_view',now(),now())
                        """)).getSQLState()).isEqualTo("42501");
            } finally {
                connection.rollback();
            }
        }
    }

    @Test
    void trustedBackendIngestionIsReplaySafeAndServiceRoleAccessIsPreserved() throws Exception {
        assertThat(jdbc.queryForObject("select count(*) from behavior_events", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select page_path from behavior_events", String.class)).isEqualTo("/project");
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            statement.execute("set role service_role");
            try (var rows = statement.executeQuery("select count(*) from behavior_events")) {
                rows.next();
                assertThat(rows.getInt(1)).isEqualTo(1);
            }
        }
    }

    @Test
    void publicSummaryStillAggregatesWithoutExposingVisitorRecords() throws Exception {
        http.perform(get("/api/public/visits/summary").param("window", "7d"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totals.uniqueVisitors").value(1))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("access-session"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("access-event"))));
        http.perform(get("/api/public/visits/top-pages").param("window", "7d"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items").isEmpty());
    }

    @Test
    void onlyAuthenticatedBackendCanServeAdminRecords() throws Exception {
        http.perform(get("/api/admin/visitors")).andExpect(status().isUnauthorized());
        http.perform(get("/api/admin/visitors").header("X-Internal-Token", "wrong"))
                .andExpect(status().isUnauthorized());
        http.perform(get("/api/admin/visitors").header("X-Internal-Token", "test-internal-token"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items[0].eventId").value("access-event"))
                .andExpect(jsonPath("$.items[0].browser").value("Chrome"));
    }

    @Test
    void overviewRequiresAuthenticationAndReturnsOnlyAggregates() throws Exception {
        http.perform(get("/api/admin/visitor-overview")).andExpect(status().isUnauthorized());
        http.perform(get("/api/admin/visitor-overview").header("X-Internal-Token", "wrong"))
                .andExpect(status().isUnauthorized());
        http.perform(get("/api/admin/visitor-overview").header("X-Internal-Token", "test-internal-token"))
                .andExpect(status().isOk())
                .andExpect(result -> assertThat(result.getResponse().getContentAsString()).contains("\"events\":1"))
                .andExpect(jsonPath("$.summary.events").value(1))
                .andExpect(jsonPath("$.summary.pageViews").value(1))
                .andExpect(jsonPath("$.summary.sessions").value(1))
                .andExpect(jsonPath("$.summary.likelyPeople").value(1))
                .andExpect(jsonPath("$.rankings.pages[0].value").value("/project"))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("private=value"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("192.0.2.1"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("access-session"))));
    }

    @Test
    void overviewAndRecordsUseTheSameWindowAndBotFilters() throws Exception {
        String from = Instant.now().minusSeconds(3600).toString();
        String to = Instant.now().toString();
        for (String bot : List.of("ALL", "EXCLUDE", "ONLY")) {
            int expected = bot.equals("ONLY") ? 0 : 1;
            http.perform(get("/api/admin/visitor-overview")
                            .header("X-Internal-Token", "test-internal-token")
                            .param("from", from).param("to", to).param("bot", bot).param("path", "/project"))
                    .andExpect(status().isOk())
                    .andExpect(result -> assertThat(result.getResponse().getContentAsString()).contains("\"events\":" + expected))
                    .andExpect(jsonPath("$.summary.events").value(expected));
            http.perform(get("/api/admin/visitors")
                            .header("X-Internal-Token", "test-internal-token")
                            .param("from", from).param("to", to).param("bot", bot).param("path", "/project")
                            .param("size", "1"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.page.totalElements").value(expected));
        }
    }
}
