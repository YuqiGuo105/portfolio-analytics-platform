package site.yuqi.analytics.aggregator.governance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.mockito.ArgumentCaptor;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class GovernanceCoreTest {
    static GovernancePolicy policy(boolean enabled) {
        return new GovernancePolicy(enabled, false, 90, 90, "governance-test");
    }

    @Test
    void canonicalManifestIsStableAcrossMapOrderingAndUsesStandardSha256() {
        Map<String, Object> reversed = new LinkedHashMap<>();
        reversed.put("z", List.of(Map.of("b", 2, "a", "quoted\"\\\n")));
        reversed.put("a", true);
        assertThat(EvidenceCodec.canonical(reversed)).isEqualTo(
                EvidenceCodec.canonical(Map.of("a", true, "z", List.of(Map.of("a", "quoted\"\\\n", "b", 2)))));
        assertThat(EvidenceCodec.canonical(Map.of("z", 2, "a", 1))).isEqualTo("{\"a\":1,\"z\":2}");
        assertThat(EvidenceCodec.sha256("abc")).isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    @Test
    void flagOffAndEmptyBatchDoNotTouchDatabaseOrWorkerServices() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        var event = event("one");
        new EvidenceReceiptWriter(jdbc, policy(false)).append(List.of(event));
        new EvidenceReceiptWriter(jdbc, policy(true)).append(List.of());
        new GovernanceAudit(jdbc, policy(false)).append(null, GovernanceAudit.Action.OVERVIEW,
                GovernanceAudit.Target.GOVERNANCE, null, null, 0, GovernanceAudit.Outcome.REQUESTED);
        VisitorGovernanceService service = mock(VisitorGovernanceService.class);
        new GovernanceWorker(policy(false), service, jdbc, mock(PlatformTransactionManager.class), 3600).runIfDue();
        verifyNoInteractions(jdbc, service);
    }

    @Test
    void enabledReceiptAppendUsesOneStatementAndDoesNotSendEventProperties() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        new EvidenceReceiptWriter(jdbc, policy(true)).append(List.of(event("one"), event("one"), event("two")));
        verify(jdbc).update(contains("insert into analytics_private.visitor_evidence_receipts"),
                eq(90), eq("governance-test"), eq("[\"one\",\"two\"]"), eq("governance-test"));
        verifyNoMoreInteractions(jdbc);
    }

    @Test
    void busyWorkerDoesNotRunMaintenanceOrArchive() {
        VisitorGovernanceService service = mock(VisitorGovernanceService.class);
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
        when(transactions.getTransaction(any())).thenReturn(new org.springframework.transaction.support.SimpleTransactionStatus());
        var result = new GovernanceWorker(policy(true), service, jdbc, transactions, 3600).runIfDue();
        assertThat(result.status()).isEqualTo("busy");
        verify(service).maintenanceHealth();
        verifyNoMoreInteractions(service);
    }

    @Test
    void workerCannotBeConfiguredMoreFrequentlyThanHourly() {
        VisitorGovernanceService service = mock(VisitorGovernanceService.class);
        assertThatThrownBy(() -> new GovernanceWorker(policy(true), service, mock(JdbcTemplate.class),
                mock(PlatformTransactionManager.class), 3599)).isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(service);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "x@y.test", "192.0.2.1", "bad name", "newline\n", "a/b"})
    void invalidActorsAreRejected(String actor) {
        assertThatThrownBy(() -> GovernanceAudit.actor(actor)).isInstanceOf(GovernanceException.class);
    }

    @Test
    void optionalAndPseudonymousActorsWork() {
        assertThat(GovernanceAudit.actor(null)).isEqualTo("internal-service");
        assertThat(GovernanceAudit.actor("verified-admin")).isEqualTo("verified-admin");
        assertThat(GovernanceAudit.actor("a".repeat(64))).hasSize(64);
        assertThatThrownBy(() -> GovernanceAudit.actor("a".repeat(65))).isInstanceOf(GovernanceException.class);
    }

    @Test
    void serviceBoundsRejectBeforeDatabaseAccess() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        var service = new VisitorGovernanceService(jdbc, policy(true), mock(GovernanceAudit.class));
        for (int limit : List.of(-1, 0, 501, Integer.MAX_VALUE)) {
            assertThatThrownBy(() -> service.archive(null, limit)).isInstanceOf(GovernanceException.class);
        }
        verifyNoInteractions(jdbc);
    }

    @Test
    void disabledServiceRejectsWithoutDatabaseAccess() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        var service = new VisitorGovernanceService(jdbc, policy(false), mock(GovernanceAudit.class));
        assertThatThrownBy(() -> service.overview(null)).isInstanceOfSatisfying(GovernanceException.class,
                e -> assertThat(e.code()).isEqualTo("governance_disabled"));
        assertThat(service.expire()).isZero();
        verifyNoInteractions(jdbc);
    }

    @Test
    void disabledFeatureDoesNotAddRestrictionsToExistingSiteConfiguration() {
        assertThatCode(() -> new GovernancePolicy(false, false, 0, 0, "legacy site"))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> new GovernancePolicy(true, false, 0, 90, "test"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void overviewLoadsSummaryColumnsAndHasTransactionDeadline() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        new VisitorGovernanceService(jdbc, policy(true), mock(GovernanceAudit.class)).overview(null);
        var sql = ArgumentCaptor.forClass(String.class);
        verify(jdbc, times(3)).query(sql.capture(), any(RowMapper.class), eq("governance-test"));
        assertThat(sql.getAllValues().getFirst()).contains("select batch_id,")
                .doesNotContain("canonical_manifest", "select *");
        var attribute = new AnnotationTransactionAttributeSource().getTransactionAttribute(
                VisitorGovernanceService.class.getMethod("overview", String.class), VisitorGovernanceService.class);
        assertThat(attribute).isNotNull();
        assertThat(attribute.getTimeout()).isEqualTo(10);
    }

    private static EnrichedEvent event(String id) {
        return new EnrichedEvent(id, "governance-test", "page_view", Instant.EPOCH, Instant.EPOCH,
                "private-session", "private-anon", "https://example.test/?secret=one", null, null,
                "desktop", "Chrome", "OS", false, 0, "ip-hash", null);
    }
}
