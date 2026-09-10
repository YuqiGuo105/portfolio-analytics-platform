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

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
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

        var mapper = new ObjectMapper().findAndRegisterModules();
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
        Instant time = Instant.now().minusSeconds(60);
        var raw = new RawEvent("access-event", "access-test", "page_view", time, time,
                "access-session", "access-anon", "https://example.test/project?private=value",
                null, null, "Chrome", "192.0.2.1", null);
        var enriched = new EnrichedEvent("access-event", "access-test", "page_view", time, time,
                "access-session", "hashed-anon", raw.pageUrl(), null, null,
                "desktop", "Chrome", "test-os", false, 0, "hashed-ip", null);
        var writer = new VisitorLogPersistService(jdbc, new ObjectMapper());
        new TransactionTemplate(new DataSourceTransactionManager(ds)).executeWithoutResult(status -> {
            writer.persistBatch(List.of(raw), List.of(enriched));
            writer.persistBatch(List.of(raw), List.of(enriched));
        });
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
}
