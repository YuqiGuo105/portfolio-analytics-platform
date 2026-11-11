package site.yuqi.analytics.aggregator.governance;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;
import org.springframework.transaction.support.TransactionTemplate;
import site.yuqi.analytics.aggregator.service.VisitorLogPersistService;
import site.yuqi.analytics.aggregator.service.AnalyticsEventOutboxRelay;
import site.yuqi.analytics.aggregator.config.GovernanceWebConfig;
import site.yuqi.analytics.aggregator.web.*;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Destructive fixtures are permitted only in a disposable loopback analytics_test DB. */
@EnabledIfEnvironmentVariable(named = "ANALYTICS_TEST_JDBC_URL", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GovernancePostgresTest {
    private DriverManagerDataSource ds;
    private JdbcTemplate jdbc;
    private DataSourceTransactionManager transactions;
    private VisitorGovernanceService service;
    private VisitorLogPersistService writer;
    private MockMvc http;
    private final GovernancePolicy policy = new GovernancePolicy(true, false, 90, 90, "governance-test");

    @BeforeAll
    void setup() {
        String url = System.getenv("ANALYTICS_TEST_JDBC_URL");
        assertThat(url).matches("jdbc:postgresql://(127\\.0\\.0\\.1|localhost):[0-9]+/analytics_test");
        ds = new DriverManagerDataSource(url, System.getenv("ANALYTICS_TEST_DB_USER"),
                System.getenv("ANALYTICS_TEST_DB_PASSWORD"));
        jdbc = new JdbcTemplate(ds);
        transactions = new DataSourceTransactionManager(ds);
        jdbc.execute("drop schema if exists analytics_private cascade");
        jdbc.execute("drop schema public cascade");
        jdbc.execute("create schema public");
        jdbc.execute("grant usage on schema public to public");
        for (String role : List.of("anon", "authenticated", "service_role")) {
            if (!Boolean.TRUE.equals(jdbc.queryForObject("select exists(select 1 from pg_roles where rolname=?)",
                    Boolean.class, role))) {
                jdbc.execute("create role " + role + (role.equals("service_role") ? " bypassrls" : ""));
            }
        }
        jdbc.execute("""
                create table visitor_logs (id bigserial primary key, ip text, local_time timestamptz, event text,
                ua text, country text, region text, city text, latitude double precision, longitude double precision,
                created_at timestamptz default now())
                """);
        jdbc.execute("create table public.\"Projects\" (id text primary key, updated_at timestamptz)");
        jdbc.execute("create table public.\"Blogs\" (id text primary key, title text)");
        jdbc.execute("create table public.life_blogs (id text primary key, updated_at timestamptz)");
        jdbc.execute("create table public.experience (id text primary key, name text)");
        Flyway.configure().dataSource(ds).table("analytics_aggregator_flyway_history")
                .baselineOnMigrate(true).baselineVersion("0").target("16").load().migrate();
        Flyway flyway = Flyway.configure().dataSource(ds).table("analytics_aggregator_flyway_history")
                .baselineOnMigrate(true).baselineVersion("0").load();
        flyway.migrate();
        flyway.validate();
        var audit = new GovernanceAudit(jdbc, policy);
        service = transactional(new VisitorGovernanceService(jdbc, policy, audit));
        writer = transactional(new VisitorLogPersistService(jdbc, new ObjectMapper(),
                transactional(new EvidenceReceiptWriter(jdbc, policy))));
        http = MockMvcBuilders.standaloneSetup(new AdminVisitorGovernanceController(service))
                .setControllerAdvice(new GovernanceErrorAdvice(), new GovernanceRequestAdvice())
                .addInterceptors(new GovernanceAuditInterceptor(policy, audit))
                .addFilters(new AdminTokenFilter("local-test-token")).build();
    }

    @BeforeEach
    void resetFixtures() {
        jdbc.execute("truncate analytics_private.visitor_evidence_receipts, analytics_private.visitor_evidence_batches, "
                + "public.behavior_events, analytics_private.behavior_events_raw, public.visitor_logs restart identity");
        jdbc.execute("truncate analytics_private.visitor_governance_maintenance");
        // Test-owner fixture reset deliberately demonstrates the documented owner bypass.
        jdbc.execute("alter table analytics_private.visitor_governance_audit disable trigger visitor_governance_audit_append_only");
        jdbc.execute("truncate analytics_private.visitor_governance_audit restart identity");
        jdbc.execute("alter table analytics_private.visitor_governance_audit enable trigger visitor_governance_audit_append_only");
    }

    @SuppressWarnings("unchecked")
    private <T> T transactional(T target) {
        ProxyFactory proxy = new ProxyFactory(target);
        proxy.setProxyTargetClass(true);
        proxy.addAdvice(new TransactionInterceptor(transactions, new AnnotationTransactionAttributeSource()));
        return (T) proxy.getProxy();
    }

    @Test
    void transactionReceiptUsesPersistedSourceAndDeduplicatesReplay() {
        ingest("same", "/mcp-guide?private=secret#fragment", "read_progress", Map.of("progressPercent", 50));
        ingest("same", "/terminal?replacement=secret", "page_view", Map.of("progressPercent", 100));
        assertThat(count("visitor_evidence_receipts")).isEqualTo(1);
        String evidence = jdbc.queryForObject("select canonical_evidence from analytics_private.visitor_evidence_receipts", String.class);
        var node = EvidenceCodec.parse(evidence);
        assertThat(node.at("/sourceIntegrityProjection/pagePath").asText()).isEqualTo("/mcp-guide");
        assertThat(node.at("/sourceIntegrityProjection/progressPercent").asInt()).isEqualTo(50);
        assertThat(node.path("siteId").asText()).isEqualTo("governance-test");
        assertThat(node.at("/provenance/eventTime").asText()).isEqualTo("browser_asserted");
        assertThat(node.at("/provenance/receivedAt").asText()).isEqualTo("server_observed");
        assertThat(evidence).doesNotContain("192.0.2.1", "private-session", "private-anon", "secret", "private-ua",
                "private@example.test", "ip-hash", "private-property");
        assertThat(jdbc.queryForObject("select evidence_sha256 from analytics_private.visitor_evidence_receipts", String.class))
                .isEqualTo(EvidenceCodec.sha256(evidence));
    }

    @Test
    void ingestionFailureRollsBackAllExistingTiersAndReceipt() {
        new TransactionTemplate(transactions).executeWithoutResult(status -> {
            ingest("rolled-back", "/", "page_view", Map.of());
            status.setRollbackOnly();
        });
        assertEmptyIngestion();
        jdbc.execute("alter table analytics_private.visitor_evidence_receipts rename to temporarily_unavailable");
        try {
            assertThatThrownBy(() -> ingest("failed", "/", "page_view", Map.of()))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class);
        } finally {
            jdbc.execute("alter table analytics_private.temporarily_unavailable rename to visitor_evidence_receipts");
        }
        assertEmptyIngestion();
    }

    private void assertEmptyIngestion() {
        assertThat(count("visitor_evidence_receipts")).isZero();
        assertThat(jdbc.queryForObject("select count(*) from public.behavior_events", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from analytics_private.behavior_events_raw", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from public.visitor_logs", Integer.class)).isZero();
    }

    @Test
    void whitelistExcludesUnknownPathsUnsafePropertiesAndInvalidCheckpoints() {
        List<String> paths = List.of("/", "/mcp-guide", "/terminal", "/works-list", "/blogs",
                "/blog-single/70a536b7-b498-4900-ad30-21595db01468", "/work-single/70a536b7-b498-4900-ad30-21595db01468");
        for (int i = 0; i < paths.size(); i++) ingest("known-" + i, paths.get(i), "page_view", Map.of("progressPercent", 25));
        for (Object value : List.of(-1, 26, 101, 25.1, "25", "NaN", true, "private@example.test")) {
            ingest(UUID.randomUUID().toString(), "/private@example.test", "private@example.test", Map.of("progressPercent", value));
        }
        var values = jdbc.queryForList("select canonical_evidence::jsonb #>> '{sourceIntegrityProjection,pagePath}' "
                + "from analytics_private.visitor_evidence_receipts where source_event_id like 'known-%'", String.class);
        assertThat(values).containsExactlyInAnyOrderElementsOf(paths);
        assertThat(jdbc.queryForObject("""
                select count(*) from analytics_private.visitor_evidence_receipts where source_event_id not like 'known-%'
                and canonical_evidence::jsonb #>> '{sourceIntegrityProjection,pagePath}' = '[other]'
                and canonical_evidence::jsonb #>> '{sourceIntegrityProjection,eventType}' = 'other'
                and canonical_evidence::jsonb #> '{sourceIntegrityProjection,progressPercent}' = 'null'::jsonb
                """, Integer.class)).isEqualTo(8);
        for (int progress : List.of(0, 25, 50, 75, 100)) {
            ingest("checkpoint-" + progress, "/blogs", "read_progress", Map.of("progressPercent", progress));
            assertThat(jdbc.queryForObject("select canonical_evidence::jsonb #>> '{sourceIntegrityProjection,progressPercent}' "
                    + "from analytics_private.visitor_evidence_receipts where source_event_id = ?", String.class,
                    "checkpoint-" + progress)).isEqualTo(String.valueOf(progress));
        }
    }

    @Test
    void outboundPlatformIsAllowlistedAndSiteScopeIsFixed() {
        ingest("social", "/mcp-guide", "outbound_link_clicked", Map.of("action", "LinkedIn"));
        ingest("unsafe-social", "/mcp-guide", "outbound_link_clicked", Map.of("action", "private@example.test"));
        String payload = jdbc.queryForObject("select canonical_evidence from analytics_private.visitor_evidence_receipts "
                + "where source_event_id='social'", String.class);
        assertThat(EvidenceCodec.parse(payload).at("/sourceIntegrityProjection/platform").asText()).isEqualTo("linkedin");
        assertThat(EvidenceCodec.parse(payload).at("/sourceIntegrityProjection/eventType").asText()).isEqualTo("outbound_link_clicked");
        assertThat(jdbc.queryForObject("select canonical_evidence::jsonb #> '{sourceIntegrityProjection,platform}' "
                + "from analytics_private.visitor_evidence_receipts where source_event_id='unsafe-social'", String.class)).isEqualTo("null");
        jdbc.update("update public.behavior_events set site_id='other-site'");
        var other = transactional(new EvidenceReceiptWriter(jdbc, new GovernancePolicy(true, false, 90, 90, "other-site")));
        new TransactionTemplate(transactions).executeWithoutResult(status -> other.append(List.of(enriched("social", "/", "page_view", Map.of()))));
        assertThat(count("visitor_evidence_receipts")).isEqualTo(2);
        assertThat(service.overview(null).counts().received()).isEqualTo(2);
    }

    @Test
    void realExportIsDeterministicVerifiableAndSavedForIndependentJsCheck() throws Exception {
        ingest("sample-read", "/blog-single/70a536b7-b498-4900-ad30-21595db01468?private=secret#fragment", "read_progress",
                Map.of("progressPercent", 75));
        ingest("sample-social", "/mcp-guide", "outbound_link_clicked", Map.of("action", "github"));
        UUID id = UUID.fromString(service.archive("verified-admin", 100).batch().batchId());
        assertThat(service.verify(null, id).status()).isEqualTo("verified");
        assertThat(service.verify(null, id).sourceIntegrity()).isEqualTo("matched");
        String exported = http.perform(get("/api/admin/visitor-governance/export").param("batchId", id.toString())
                        .header("X-Internal-Token", "local-test-token"))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.manifest.receipts.length()").value(2))
                .andReturn().getResponse().getContentAsString();
        var bundle = EvidenceCodec.parse(exported);
        assertThat(bundle.path("manifestSha256").asText()).isEqualTo(EvidenceCodec.sha256(bundle.path("canonicalManifest").asText()));
        assertThat(bundle.path("canonicalManifest").asText()).isEqualTo(EvidenceCodec.canonical(bundle.path("manifest")));
        assertThat(service.export(null, id).canonicalManifest()).isEqualTo(bundle.path("canonicalManifest").asText());
        assertThat(exported).doesNotContain("private@example.test", "192.0.2.1", "private-session", "private-ua", "secret");
        Files.writeString(Path.of("target", "visitor-governance-backend-evidence.json"), exported);
        assertThat(service.archive(null, 100).status()).isEqualTo("empty");
    }

    @Test
    void realLoopbackHttpExercisesSpringWiringAuthTransactionsAndDefaultOff() throws Exception {
        ingest("http-read", "/mcp-guide?private=secret", "read_progress", Map.of("progressPercent", 75));
        ingest("http-social", "/", "outbound_link_clicked", Map.of("action", "github"));
        try (var context = startHttp(true); var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + ((ServletWebServerApplicationContext) context).getWebServer().getPort()
                    + "/api/admin/visitor-governance";
            assertThat(send(client, base + "/overview", "GET", null, false).statusCode()).isEqualTo(401);
            var overview = send(client, base + "/overview", "GET", null, true);
            assertThat(overview.statusCode()).isEqualTo(200);
            assertThat(EvidenceCodec.parse(overview.body()).at("/counts/pending").asInt()).isEqualTo(2);
            assertThat(send(client, base + "/archive", "POST", "{\"limit\":501}", true).statusCode()).isEqualTo(400);
            var archive = send(client, base + "/archive", "POST", "{}", true);
            assertThat(archive.statusCode()).isEqualTo(200);
            String id = EvidenceCodec.parse(archive.body()).at("/batch/batchId").asText();
            String request = "{\"batchId\":\"" + id + "\"}";
            var verify = send(client, base + "/verify", "POST", request, true);
            assertThat(verify.statusCode()).isEqualTo(200);
            assertThat(EvidenceCodec.parse(verify.body()).path("status").asText()).isEqualTo("verified");
            String hold = "{\"batchId\":\"" + id + "\",\"reason\":\"incident_review\"}";
            assertThat(EvidenceCodec.parse(send(client, base + "/hold", "POST", hold, true).body())
                    .at("/batch/held").asBoolean()).isTrue();
            var exported = send(client, base + "/export?batchId=" + id, "GET", null, true);
            assertThat(exported.statusCode()).isEqualTo(200);
            assertThat(exported.headers().firstValue("cache-control")).contains("no-store");
            var bundle = EvidenceCodec.parse(exported.body());
            assertThat(bundle.path("manifestSha256").asText()).isEqualTo(EvidenceCodec.sha256(bundle.path("canonicalManifest").asText()));
            Files.writeString(Path.of("target", "visitor-governance-backend-evidence.json"), exported.body());
            assertThat(EvidenceCodec.parse(send(client, base + "/hold", "DELETE", hold, true).body())
                    .at("/batch/held").asBoolean()).isFalse();
            assertThat(EvidenceCodec.parse(send(client, base + "/overview", "GET", null, true).body())
                    .at("/counts/archived").asInt()).isEqualTo(2);
            String drain = base.replace("/api/admin/visitor-governance", "/api/internal/workers/drain");
            assertThat(send(client, drain + "?target=visitor-governance", "POST", null, false).statusCode()).isEqualTo(401);
            assertThat(send(client, drain + "?target=raw", "POST", null, true).statusCode()).isEqualTo(400);
            var maintained = send(client, drain + "?target=visitor-governance", "POST", null, true);
            assertThat(maintained.statusCode()).isEqualTo(200);
            assertThat(EvidenceCodec.parse(maintained.body()).at("/governance/status").asText()).isEqualTo("completed");
            assertThat(EvidenceCodec.parse(send(client, drain + "?target=visitor-governance", "POST", null, true).body())
                    .at("/governance/status").asText()).isEqualTo("not_due");
        }
        int before = count("visitor_evidence_receipts");
        try (var context = startHttp(false); var client = HttpClient.newHttpClient()) {
            String base = "http://127.0.0.1:" + ((ServletWebServerApplicationContext) context).getWebServer().getPort();
            var disabled = send(client, base + "/api/admin/visitor-governance/overview", "GET", null, true);
            assertThat(disabled.statusCode()).isEqualTo(404);
            assertThat(EvidenceCodec.parse(disabled.body()).path("error").asText()).isEqualTo("governance_disabled");
            var event = enriched("disabled-real-bean", "/", "page_view", Map.of());
            var raw = new RawEvent(event.eventId(), event.siteId(), event.eventType(), event.eventTime(), event.serverTime(),
                    null, null, "https://example.test/", null, null, "test", "192.0.2.2", null);
            context.getBean(VisitorLogPersistService.class).persistBatch(List.of(raw), List.of(event));
            assertThat(count("visitor_evidence_receipts")).isEqualTo(before);
            var drained = send(client, base + "/api/internal/workers/drain?target=visitor-governance", "POST", null, true);
            assertThat(EvidenceCodec.parse(drained.body())).isEqualTo(EvidenceCodec.parse("{\"status\":\"completed\",\"claimed\":0}"));
        }
    }

    private org.springframework.context.ConfigurableApplicationContext startHttp(boolean enabled) {
        return new SpringApplicationBuilder(LocalHttpApplication.class).run(
                "--server.address=127.0.0.1", "--server.port=0", "--spring.main.banner-mode=off",
                "--analytics.governance.enabled=" + enabled, "--analytics.backfill.site-id=governance-test",
                "--analytics.admin.internal-token=local-test-token", "--spring.jmx.enabled=false",
                "--management.endpoints.enabled-by-default=false");
    }

    private HttpResponse<String> send(HttpClient client, String uri, String method, String body, boolean authenticated)
            throws Exception {
        var request = HttpRequest.newBuilder(URI.create(uri)).timeout(Duration.ofSeconds(10));
        if (authenticated) request.header("X-Internal-Token", "local-test-token");
        if (body != null) request.header("Content-Type", "application/json");
        return client.send(request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {KafkaAutoConfiguration.class, RedisAutoConfiguration.class,
            RedisRepositoriesAutoConfiguration.class, FlywayAutoConfiguration.class})
    @Import({GovernancePolicy.class, GovernanceAudit.class, VisitorGovernanceService.class,
            EvidenceReceiptWriter.class, VisitorLogPersistService.class, GovernanceWebConfig.class,
            GovernanceAuditInterceptor.class, AdminVisitorGovernanceController.class,
            GovernanceErrorAdvice.class, GovernanceRequestAdvice.class, AdminTokenFilter.class,
            GovernanceWorker.class, WorkerRecoveryController.class})
    static class LocalHttpApplication {
        @Bean AnalyticsEventOutboxRelay outbox() { return mock(AnalyticsEventOutboxRelay.class); }
        @Bean
        javax.sql.DataSource dataSource() {
            String url = System.getenv("ANALYTICS_TEST_JDBC_URL");
            if (url == null || !url.matches("jdbc:postgresql://(127\\.0\\.0\\.1|localhost):[0-9]+/analytics_test")) {
                throw new IllegalArgumentException("Disposable loopback database required");
            }
            return new DriverManagerDataSource(url, System.getenv("ANALYTICS_TEST_DB_USER"),
                    System.getenv("ANALYTICS_TEST_DB_PASSWORD"));
        }
    }

    @Test
    void pendingReceiptTamperingIsRejectedBeforeArchiveAndAudited() {
        ingest("one", "/", "page_view", Map.of());
        jdbc.update("update analytics_private.visitor_evidence_receipts set canonical_evidence='{}'");
        assertThatThrownBy(() -> service.archive(null, 1)).isInstanceOf(GovernanceException.class);
        assertThat(count("visitor_evidence_batches")).isZero();
        assertThat(lastOutcome()).isEqualTo("rejected");
        jdbc.update("update analytics_private.visitor_evidence_receipts set evidence_sha256=?", EvidenceCodec.sha256("{}"));
        assertThatThrownBy(() -> service.archive(null, 1)).isInstanceOf(GovernanceException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"received_at = received_at - interval '1 day'",
            "expires_at = expires_at + interval '1 day'", "event_ref = repeat('a',64)",
            "receipt_id = receipt_id + 100", "source_event_id = null"})
    void capturedMetadataTamperingCannotBeSealedByArchive(String change) {
        ingest("capture-metadata", "/", "page_view", Map.of());
        String evidenceHash = jdbc.queryForObject("select evidence_sha256 from analytics_private.visitor_evidence_receipts", String.class);
        String captureHash = jdbc.queryForObject("select capture_sha256 from analytics_private.visitor_evidence_receipts", String.class);
        jdbc.update("update analytics_private.visitor_evidence_receipts set " + change);
        assertThat(jdbc.queryForObject("select evidence_sha256 from analytics_private.visitor_evidence_receipts", String.class)).isEqualTo(evidenceHash);
        assertThat(jdbc.queryForObject("select capture_sha256 from analytics_private.visitor_evidence_receipts", String.class)).isEqualTo(captureHash);
        assertThatThrownBy(() -> service.archive(null, 1)).isInstanceOfSatisfying(GovernanceException.class,
                e -> assertThat(e.code()).isEqualTo("integrity_check_failed"));
        assertThat(count("visitor_evidence_batches")).isZero();
        assertThat(lastOutcome()).isEqualTo("rejected");
    }

    @Test
    void detectsManifestReceiptAndSourceTamperingAndRefusesExport() {
        ingest("one", "/mcp-guide", "read_progress", Map.of("progressPercent", 25));
        UUID id = UUID.fromString(service.archive(null, 10).batch().batchId());
        String original = service.export(null, id).canonicalManifest();
        jdbc.update("update analytics_private.visitor_evidence_batches set canonical_manifest='{}' where batch_id=?", id);
        assertThat(service.verify(null, id).manifestValid()).isFalse();
        assertThatThrownBy(() -> service.export(null, id)).isInstanceOf(GovernanceException.class);
        assertThat(lastOutcome()).isEqualTo("rejected");
        jdbc.update("update analytics_private.visitor_evidence_batches set canonical_manifest=? where batch_id=?", original, id);
        jdbc.update("update public.behavior_events set properties='{\"progressPercent\":50}'::jsonb");
        var source = service.verify(null, id);
        assertThat(source.manifestValid()).isTrue();
        assertThat(source.sourceIntegrity()).isEqualTo("mismatch");
        assertThat(source.sourceMismatchCount()).isEqualTo(1);
        assertThat(source.status()).isEqualTo("tampered");
        jdbc.update("update analytics_private.visitor_evidence_receipts set canonical_evidence='{}'");
        assertThat(service.verify(null, id).receiptHashesValid()).isFalse();
    }

    @Test
    void sourceMissingIsExplicitAndExcludedFieldsAreNotOverclaimed() {
        ingest("one", "/", "page_view", Map.of());
        ingest("two", "/", "page_view", Map.of());
        UUID id = UUID.fromString(service.archive(null, 10).batch().batchId());
        jdbc.update("update public.behavior_events set ip_hash='different', session_id='different'");
        assertThat(service.verify(null, id).sourceIntegrity()).isEqualTo("matched");
        jdbc.update("delete from public.behavior_events where event_id='one'");
        assertThat(service.verify(null, id).sourceIntegrity()).isEqualTo("partial");
        assertThat(service.verify(null, id).sourceMissingCount()).isEqualTo(1);
        jdbc.update("delete from public.behavior_events");
        assertThat(service.verify(null, id).sourceIntegrity()).isEqualTo("unavailable");
        assertThat(service.verify(null, id).status()).isEqualTo("verified");
    }

    @Test
    void holdsSerializeRetentionAndReleaseIsAuditedWithoutChangingManifest() {
        ingest("held", "/", "page_view", Map.of());
        UUID id = UUID.fromString(service.archive(null, 1).batch().batchId());
        String manifest = service.export(null, id).canonicalManifest();
        assertThat(service.hold("verified-admin", id, "incident_review", true).changed()).isTrue();
        assertThat(service.hold("verified-admin", id, "incident_review", true).changed()).isTrue();
        assertThat(service.export(null, id).canonicalManifest()).isEqualTo(manifest);
        assertThat(service.overview(null).counts().held()).isEqualTo(1);
        jdbc.update("update analytics_private.visitor_evidence_batches set expires_at=now()-interval '1 day'");
        assertThat(service.expire()).isZero();
        assertThat(service.hold("verified-admin", id, "data_quality", false).changed()).isTrue();
        assertThat(service.expire()).isEqualTo(1);
        assertThat(service.verify(null, id).status()).isEqualTo("expired");
        assertThatThrownBy(() -> service.export(null, id)).isInstanceOf(GovernanceException.class);
        assertThatThrownBy(() -> service.hold(null, id, "incident_review", true)).isInstanceOf(GovernanceException.class);
        assertThat(jdbc.queryForObject("select canonical_evidence from analytics_private.visitor_evidence_receipts", String.class)).isNull();
        assertThat(jdbc.queryForObject("select source_event_id from analytics_private.visitor_evidence_receipts", String.class)).isNull();
        assertThat(service.overview(null).counts().expired()).isEqualTo(1);
        ingest("held", "/", "page_view", Map.of());
        assertThat(count("visitor_evidence_receipts")).isEqualTo(1);
        assertThat(service.overview(null).counts().pending()).isZero();
    }

    @Test
    void archiveRetentionAndPendingRetentionHaveHardBounds() {
        jdbc.execute("""
                insert into public.behavior_events(event_id,site_id,event_name,event_time,server_time)
                select 'bound-' || n, 'governance-test', 'page_view', now(), now() from generate_series(1, 1100) n
                """);
        List<EnrichedEvent> ids = java.util.stream.IntStream.rangeClosed(1, 1100)
                .mapToObj(i -> enriched("bound-" + i, "/", "page_view", Map.of())).toList();
        var receipts = transactional(new EvidenceReceiptWriter(jdbc, policy));
        new TransactionTemplate(transactions).executeWithoutResult(status -> receipts.append(ids));
        assertThat(service.archive(null, 500).archivedCount()).isEqualTo(500);
        assertThat(service.archive(null, 1).archivedCount()).isEqualTo(1);
        jdbc.update("update analytics_private.visitor_evidence_receipts set expires_at=now()-interval '1 day' where status='pending'");
        jdbc.update("update analytics_private.visitor_evidence_batches set expires_at=now()-interval '1 day'");
        assertThat(service.expire()).isBetween(201, 700);
        assertThat(service.overview(null).counts().pending()).isEqualTo(399);
        assertThat(service.overview(null).counts().received()).isEqualTo(1100);
        assertThat(jdbc.queryForObject("select count(*) from analytics_private.visitor_evidence_batches where status='expired'", Integer.class))
                .isEqualTo(1);
    }

    @Test
    void concurrentWorkersCannotArchiveTheSameReceipts() throws Exception {
        for (int i = 0; i < 10; i++) ingest("concurrent-" + i, "/", "page_view", Map.of());
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var first = pool.submit(() -> { start.await(); return service.archive("worker-one", 5); });
            var second = pool.submit(() -> { start.await(); return service.archive("worker-two", 5); });
            start.countDown();
            assertThat(first.get(10, TimeUnit.SECONDS).archivedCount() + second.get(10, TimeUnit.SECONDS).archivedCount()).isEqualTo(10);
        }
        assertThat(jdbc.queryForObject("select count(distinct batch_id) from analytics_private.visitor_evidence_receipts", Integer.class)).isEqualTo(2);
        assertThat(count("visitor_evidence_receipts")).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(strings = {"anon", "authenticated"})
    void browserRolesHaveNoPrivatePrivilegesAndRlsDeniesAccidentalGrants(String role) throws Exception {
        ingest("rls-visible-to-owner", "/", "page_view", Map.of());
        service.archive(null, 1);
        for (String table : List.of("visitor_evidence_receipts", "visitor_evidence_batches", "visitor_governance_audit")) {
            assertThat(count(table)).isPositive();
            for (String privilege : List.of("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE")) {
                assertThat(jdbc.queryForObject("select has_table_privilege(?, ?, ?)", Boolean.class, role,
                        "analytics_private." + table, privilege)).isFalse();
            }
            try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
                connection.setAutoCommit(false);
                try {
                    statement.execute("grant usage on schema analytics_private to " + role);
                    statement.execute("grant select on analytics_private." + table + " to " + role);
                    statement.execute("set local role " + role);
                    try (var result = statement.executeQuery("select count(*) from analytics_private." + table)) {
                        result.next();
                        assertThat(result.getInt(1)).isZero();
                    }
                } finally { connection.rollback(); }
            }
        }
    }

    @Test
    void auditIsAppendOnlyEvenForOrdinaryOwnerStatementsAndServiceGrantsAreNarrow() {
        service.overview(null);
        for (String sql : List.of("delete from analytics_private.visitor_governance_audit",
                "update analytics_private.visitor_governance_audit set actor='forged'",
                "truncate analytics_private.visitor_governance_audit")) {
            assertThatThrownBy(() -> jdbc.execute(sql)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        }
        for (String privilege : List.of("UPDATE", "DELETE", "TRUNCATE")) {
            assertThat(jdbc.queryForObject("select has_table_privilege('service_role', 'analytics_private.visitor_governance_audit', ?)",
                    Boolean.class, privilege)).isFalse();
        }
        assertThat(jdbc.queryForObject("select actor from analytics_private.visitor_governance_audit", String.class)).isEqualTo("internal-service");
    }

    @Test
    void auditFailureRollsBackHoldAndDestructiveRetention() {
        ingest("atomic-audit", "/", "page_view", Map.of());
        UUID id = UUID.fromString(service.archive(null, 1).batch().batchId());
        jdbc.update("update analytics_private.visitor_evidence_batches set expires_at=now()-interval '1 day'");
        jdbc.execute("alter table analytics_private.visitor_governance_audit rename to temporarily_unavailable_audit");
        try {
            assertThatThrownBy(() -> service.hold(null, id, "incident_review", true))
                    .isInstanceOf(org.springframework.dao.DataAccessException.class);
            assertThatThrownBy(() -> service.expire()).isInstanceOf(org.springframework.dao.DataAccessException.class);
        } finally {
            jdbc.execute("alter table analytics_private.temporarily_unavailable_audit rename to visitor_governance_audit");
        }
        assertThat(jdbc.queryForObject("select held from analytics_private.visitor_evidence_batches", Boolean.class)).isFalse();
        assertThat(jdbc.queryForObject("select status from analytics_private.visitor_evidence_batches", String.class)).isEqualTo("archived");
        assertThat(jdbc.queryForObject("select canonical_evidence from analytics_private.visitor_evidence_receipts", String.class)).isNotNull();
    }

    @Test
    void governanceFunctionsHaveFixedSearchPathsAfterV16Upgrade() {
        for (String signature : List.of("reject_governance_audit_mutation()",
                "capture_visitor_receipt_metadata()", "check_governance_delete()",
                "check_governance_delete_bound()", "stamp_governance_expiry()",
                "visitor_evidence_v1(public.behavior_events,text)",
                "visitor_receipt_capture_v1(analytics_private.visitor_evidence_receipts)",
                "purge_visitor_governance(text,text,integer)")) {
            assertThat(jdbc.queryForObject("""
                    select 'search_path=pg_catalog, analytics_private' = any(proconfig)
                    from pg_catalog.pg_proc where oid = ?::regprocedure
                    """, Boolean.class, "analytics_private." + signature)).as(signature).isTrue();
            assertThat(jdbc.queryForObject("""
                    select prosecdef from pg_catalog.pg_proc where oid = ?::regprocedure
                    """, Boolean.class, "analytics_private." + signature)).as(signature)
                    .isEqualTo(signature.startsWith("purge_visitor_governance("));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"receipts", "batches", "audit"})
    void fixedSearchPathPreservesTransitionTableDeleteBounds(String target) throws Exception {
        String table;
        String key;
        switch (target) {
            case "receipts" -> {
                seedExpiredReceipts(501, 181, "governance-test");
                table = "visitor_evidence_receipts";
                key = "receipt_id";
            }
            case "batches" -> {
                jdbc.execute("""
                        insert into analytics_private.visitor_evidence_batches(batch_id,site_id,created_at,
                            expires_at,receipt_count,manifest_sha256,status,expired_at)
                        select gen_random_uuid(),'governance-test',now()-interval '300 days',
                            now()-interval '210 days',1,repeat('a',64),'expired',now()-interval '181 days'
                        from generate_series(1,501)
                        """);
                table = "visitor_evidence_batches";
                key = "batch_id";
            }
            case "audit" -> {
                seedOldAudit(501);
                table = "visitor_governance_audit";
                key = "audit_id";
            }
            default -> throw new IllegalArgumentException("Unexpected test target");
        }
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            statement.execute("set search_path = public");
            // The trigger must read its transition rows, not a same-named caller relation.
            statement.execute("create temporary table deleted_rows (id integer)");
            var rejected = Assertions.assertThrows(java.sql.SQLException.class,
                    () -> statement.executeUpdate("delete from analytics_private." + table));
            assertThat(rejected.getSQLState()).isEqualTo("22023");
            assertThat(rejected.getMessage()).contains("governance purge exceeds 500 rows");
            assertThat(count(table)).isEqualTo(501);
            assertThat(statement.executeUpdate("delete from analytics_private." + table + " where " + key
                    + " in (select " + key + " from analytics_private." + table + " order by " + key + " limit 500)"))
                    .isEqualTo(500);
        }
        assertThat(count(table)).isEqualTo(1);
        assertThat(purge(target, 500)).isEqualTo(1);
        assertThat(jdbc.queryForObject("""
                select affected_count from analytics_private.visitor_governance_audit where action=?
                """, Integer.class, "purge_" + target)).isEqualTo(1);
    }

    @Test
    void auditPurgeHasExact180DayBoundaryAndAppRoleCannotDeleteDirectly() throws Exception {
        jdbc.execute("""
                do $$ begin
                  insert into analytics_private.visitor_governance_audit(site_id,actor,action,target,outcome,occurred_at)
                    values ('governance-test','internal-service','overview','governance','requested',statement_timestamp()-interval '180 days'),
                    ('governance-test','internal-service','overview','governance','requested',statement_timestamp()-interval '180 days'+interval '1 microsecond');
                  if analytics_private.purge_visitor_governance('governance-test','audit',500) <> 1 then
                    raise exception 'audit boundary incorrect';
                  end if;
                end $$
                """);
        assertThat(jdbc.queryForObject("select affected_count from analytics_private.visitor_governance_audit where action='purge_audit'", Integer.class)).isEqualTo(1);
        seedOldAudit(3);
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            statement.execute("set role service_role");
            try (var rows = statement.executeQuery("select analytics_private.purge_visitor_governance('governance-test','audit',2)")) {
                rows.next();
                assertThat(rows.getInt(1)).isEqualTo(2);
            }
            assertThatThrownBy(() -> statement.execute("delete from analytics_private.visitor_governance_audit"))
                    .isInstanceOf(java.sql.SQLException.class);
        }
        for (String role : List.of("anon", "authenticated")) {
            assertThat(jdbc.queryForObject("select has_function_privilege(?, 'analytics_private.purge_visitor_governance(text,text,integer)', 'EXECUTE')", Boolean.class, role)).isFalse();
        }
        assertThatThrownBy(() -> purge("raw", 1)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThatThrownBy(() -> purge("audit", 501)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThatThrownBy(() -> purge("audit", 0)).isInstanceOf(org.springframework.dao.DataAccessException.class);
    }

    @Test
    void tombstonesHaveFiniteRetentionAndPurgeIsAuditedAndSiteScoped() {
        UUID batch = UUID.randomUUID();
        jdbc.update("""
                insert into analytics_private.visitor_evidence_batches(batch_id,site_id,created_at,expires_at,receipt_count,
                  manifest_sha256,status,expired_at) values (?,'governance-test',now()-interval '300 days',
                  now()-interval '210 days',1,repeat('a',64),'expired',now()-interval '181 days')
                """, batch);
        seedExpiredReceipts(1, 181, "governance-test");
        jdbc.update("update analytics_private.visitor_evidence_receipts set batch_id=?", batch);
        seedExpiredReceipts(1, 179, "governance-test");
        seedExpiredReceipts(1, 181, "other-site");
        assertThat(purge("batches", 10)).isZero();
        assertThat(purge("receipts", 10)).isEqualTo(1);
        assertThat(purge("batches", 10)).isEqualTo(1);
        assertThat(count("visitor_evidence_receipts")).isEqualTo(2);
        assertThat(jdbc.queryForList("select action from analytics_private.visitor_governance_audit", String.class))
                .contains("purge_receipts", "purge_batches");
        assertThatThrownBy(() -> jdbc.update("delete from analytics_private.visitor_evidence_receipts where site_id='governance-test'"))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
    }

    @Test
    void holdRenewalIsBoundedAndExpiredHoldIsAutomaticallyAuditedAndCleared() {
        ingest("hold-clock", "/", "page_view", Map.of());
        UUID id = UUID.fromString(service.archive(null, 1).batch().batchId());
        var first = service.hold(null, id, "incident_review", true);
        var renewed = service.hold(null, id, "incident_review", true);
        assertThat(Instant.parse(renewed.batch().holdExpiresAt())).isAfterOrEqualTo(Instant.parse(first.batch().holdExpiresAt()));
        assertThat(jdbc.queryForObject("select hold_expires_at-hold_set_at=interval '90 days' from analytics_private.visitor_evidence_batches", Boolean.class)).isTrue();
        assertThat(jdbc.queryForObject("select count(*) from analytics_private.visitor_governance_audit where action='renew_hold'", Integer.class)).isEqualTo(1);
        jdbc.execute("""
                update analytics_private.visitor_evidence_batches set hold_set_at=statement_timestamp()-interval '90 days',
                    hold_expires_at=statement_timestamp(), expires_at=statement_timestamp()-interval '1 day'
                """);
        var result = worker(false).runIfDue();
        assertThat(result.affectedCounts().releasedHolds()).isEqualTo(1);
        assertThat(result.affectedCounts().expiredArchivedReceipts()).isEqualTo(1);
        assertThat(service.verify(null, id).status()).isEqualTo("expired");
        assertThat(jdbc.queryForList("select action from analytics_private.visitor_governance_audit", String.class))
                .contains("expire_hold", "expire_batch");
        assertThatThrownBy(() -> service.hold(null, id, "incident_review", true)).isInstanceOf(GovernanceException.class);
    }

    @Test
    void maintenanceIsHourlyDurableAndSkipsConcurrentLease() throws Exception {
        var first = worker(false).runIfDue();
        assertThat(first.status()).isEqualTo("completed");
        assertThat(first.maintenance().lastSuccessAt()).isNotNull();
        assertThat(worker(false).runIfDue().status()).isEqualTo("not_due");
        jdbc.update("update analytics_private.visitor_governance_maintenance set last_success_at=now()-interval '1 hour'");
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("select pg_advisory_xact_lock(hashtextextended('visitor-governance:governance-test',0))");
            assertThat(worker(false).runIfDue().status()).isEqualTo("busy");
            connection.rollback();
        }
        assertThat(worker(false).runIfDue().status()).isEqualTo("completed");
        assertThat(service.overview(null).maintenance().intervalSeconds()).isEqualTo(3600);
    }

    @Test
    void poisonArchiveDoesNotRollBackCleanupOrItsAudit() {
        seedExpiredReceipts(1, 181, "governance-test");
        ingest("due-payload", "/", "page_view", Map.of());
        jdbc.update("update analytics_private.visitor_evidence_receipts set expires_at=now()-interval '1 day' where status='pending'");
        ingest("poison", "/", "page_view", Map.of());
        jdbc.update("update analytics_private.visitor_evidence_receipts set canonical_evidence='{}' where source_event_id='poison'");
        assertThatThrownBy(() -> worker(true).runIfDue()).isInstanceOfSatisfying(GovernanceException.class,
                e -> assertThat(e.code()).isEqualTo("governance_archive_failed"));
        var health = service.maintenanceHealth();
        assertThat(health.status()).isEqualTo("failed");
        assertThat(health.lastSuccessAt()).isNotNull();
        assertThat(health.lastFailureAt()).isNotNull();
        assertThat(health.lastAffectedCounts().path("purgedReceipts").asInt()).isEqualTo(1);
        assertThat(health.lastAffectedCounts().path("expiredPendingReceipts").asInt()).isEqualTo(1);
        assertThat(jdbc.queryForList("select action from analytics_private.visitor_governance_audit", String.class))
                .contains("purge_receipts", "expire_pending", "archive");
        assertThat(count("visitor_evidence_receipts")).isEqualTo(2);
        assertThat(count("visitor_evidence_batches")).isZero();
        assertThat(worker(true).runIfDue().status()).isEqualTo("not_due");
    }

    @Test
    void cleanupFailureDoesNotLosePriorSuccessCheckpoint() {
        worker(false).runIfDue();
        jdbc.update("update analytics_private.visitor_governance_maintenance set last_success_at=now()-interval '2 hours'");
        String previous = service.maintenanceHealth().lastSuccessAt();
        jdbc.execute("alter table analytics_private.visitor_governance_audit rename to maintenance_audit_unavailable");
        try {
            assertThatThrownBy(() -> worker(false).runIfDue()).isInstanceOf(GovernanceException.class);
        } finally {
            jdbc.execute("alter table analytics_private.maintenance_audit_unavailable rename to visitor_governance_audit");
        }
        assertThat(service.maintenanceHealth().lastSuccessAt()).isEqualTo(previous);
        assertThat(service.maintenanceHealth().lastFailureAt()).isNotNull();
        assertThat(service.maintenanceHealth().status()).isEqualTo("failed");
    }

    @Test
    void maintenanceReservesCleanupCapacityAndArchivesAtMost100() {
        seedOldAudit(600);
        seedExpiredReceipts(600, 181, "governance-test");
        seedSourceReceipts(350, "quota");
        jdbc.update("update analytics_private.visitor_evidence_receipts set expires_at=now()-interval '1 day' where source_event_id like 'quota-%' and receipt_id in (select receipt_id from analytics_private.visitor_evidence_receipts where status='pending' order by receipt_id limit 250)");
        var result = worker(true).runIfDue();
        assertThat(result.affectedCounts().purgedAudit()).isEqualTo(100);
        assertThat(result.affectedCounts().purgedReceipts()).isEqualTo(100);
        assertThat(result.affectedCounts().expiredPendingReceipts()).isEqualTo(200);
        assertThat(result.affectedCounts().archivedReceipts()).isEqualTo(100);
        assertThat(result.affectedCounts().cleanupRows()).isLessThanOrEqualTo(1000);
        assertThat(service.overview(null).counts().pending()).isEqualTo(50);
        assertThat(service.overview(null).latestBatches()).hasSize(1);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ANALYTICS_GOVERNANCE_BENCHMARK", matches = "true")
    void localSyntheticCaptureAnd100ReceiptArchiveTiming() {
        var captureMs = new java.util.ArrayList<Double>();
        var archiveMs = new java.util.ArrayList<Double>();
        for (int run = 0; run < 9; run++) {
            String prefix = "bench-" + run;
            jdbc.update("""
                    insert into public.behavior_events(event_id,site_id,event_name,event_time,server_time,page_path)
                    select ? || '-' || n,'governance-test','page_view',now(),now(),'/mcp-guide' from generate_series(1,100) n
                    """, prefix);
            List<EnrichedEvent> events = java.util.stream.IntStream.rangeClosed(1,100)
                    .mapToObj(i -> enriched(prefix + "-" + i,"/mcp-guide","page_view",Map.of())).toList();
            var receipts = transactional(new EvidenceReceiptWriter(jdbc, policy));
            long start = System.nanoTime();
            new TransactionTemplate(transactions).executeWithoutResult(status -> receipts.append(events));
            double captured = (System.nanoTime()-start)/1_000_000.0;
            start = System.nanoTime();
            assertThat(service.archive(null,100).archivedCount()).isEqualTo(100);
            double archived = (System.nanoTime()-start)/1_000_000.0;
            if (run >= 2) { captureMs.add(captured); archiveMs.add(archived); }
        }
        captureMs.sort(Double::compareTo); archiveMs.sort(Double::compareTo);
        System.out.printf(java.util.Locale.ROOT,
                "LOCAL_GOVERNANCE_BENCHMARK n=7 capture100_median_ms=%.3f capture100_max_ms=%.3f archive100_median_ms=%.3f archive100_max_ms=%.3f%n",
                captureMs.get(3),captureMs.get(6),archiveMs.get(3),archiveMs.get(6));
    }

    private GovernanceWorker worker(boolean autoArchive) {
        return new GovernanceWorker(new GovernancePolicy(true, autoArchive, 90, 90, "governance-test"),
                service, jdbc, transactions, 3600);
    }

    private int purge(String target, int limit) {
        return jdbc.queryForObject("select analytics_private.purge_visitor_governance('governance-test',?,?)", Integer.class, target, limit);
    }

    private void seedOldAudit(int count) {
        jdbc.update("""
                insert into analytics_private.visitor_governance_audit(site_id,actor,action,target,outcome,occurred_at)
                select 'governance-test','internal-service','overview','governance','requested',now()-interval '181 days'
                from generate_series(1,?)
                """, count);
    }

    private void seedExpiredReceipts(int count, int days, String site) {
        jdbc.update("""
                insert into analytics_private.visitor_evidence_receipts(site_id,event_ref,received_at,expires_at,
                    evidence_sha256,status,expired_at)
                select ?, encode(sha256(convert_to(? || '-' || n,'UTF8')),'hex'),now()-interval '300 days',
                    now()-interval '210 days',repeat('a',64),'expired',now()-make_interval(days=>?)
                from generate_series(1,?) n
                """, site, UUID.randomUUID().toString(), days, count);
    }

    private void seedSourceReceipts(int count, String prefix) {
        jdbc.update("""
                insert into public.behavior_events(event_id,site_id,event_name,event_time,server_time)
                select ? || '-' || n,'governance-test','page_view',now(),now() from generate_series(1,?) n
                """, prefix, count);
        List<EnrichedEvent> events = java.util.stream.IntStream.rangeClosed(1,count)
                .mapToObj(i -> enriched(prefix + "-" + i,"/","page_view",Map.of())).toList();
        var receipts = transactional(new EvidenceReceiptWriter(jdbc, policy));
        new TransactionTemplate(transactions).executeWithoutResult(status -> receipts.append(events));
    }

    private int count(String table) {
        return jdbc.queryForObject("select count(*) from analytics_private." + table, Integer.class);
    }

    private String lastOutcome() {
        return jdbc.queryForObject("select outcome from analytics_private.visitor_governance_audit order by audit_id desc limit 1", String.class);
    }

    private void ingest(String id, String path, String type, Map<String, Object> properties) {
        var enriched = enriched(id, path, type, properties);
        var raw = new RawEvent(id, "governance-test", type, enriched.eventTime(), enriched.serverTime(),
                "private-session", "private-anon", "https://example.test" + path, "https://private.example.test/",
                "https://private.example.test/?email=private@example.test", "private-ua", "192.0.2.1", null,
                1, "unknown", "ip-hash", Map.of("action", "private-property"));
        writer.persistBatch(List.of(raw), List.of(enriched));
    }

    private EnrichedEvent enriched(String id, String path, String type, Map<String, Object> properties) {
        return new EnrichedEvent(id, "governance-test", type, Instant.parse("2026-11-10T20:00:00Z"),
                Instant.parse("2026-11-10T20:00:01Z"), "private-session", "private-anon",
                "https://example.test" + path, "https://private.example.test/", "https://private.example.test/",
                "desktop", "Chrome", "OS", false, 0, "ip-hash", null, 1, "unknown", properties);
    }
}
