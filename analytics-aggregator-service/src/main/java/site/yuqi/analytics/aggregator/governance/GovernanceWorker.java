package site.yuqi.analytics.aggregator.governance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.transaction.support.TransactionTemplate;

import static site.yuqi.analytics.aggregator.governance.GovernanceModels.*;

/** Reuses the authenticated drain request; no independent scheduler or public traffic work. */
@Component
@Slf4j
public class GovernanceWorker {
    private final GovernancePolicy policy;
    private final VisitorGovernanceService service;
    private final JdbcTemplate jdbc;
    private final int intervalSeconds;
    private final TransactionTemplate work;
    private final TransactionTemplate archiveWork;
    private final TransactionTemplate failureCheckpoint;

    public GovernanceWorker(GovernancePolicy policy, VisitorGovernanceService service, JdbcTemplate jdbc,
                            PlatformTransactionManager transactions,
                            @Value("${analytics.governance.worker-interval-seconds:3600}") int intervalSeconds) {
        if (policy.enabled() && (intervalSeconds < 3600 || intervalSeconds > 86400)) {
            throw new IllegalArgumentException("Governance worker interval must be 3600..86400 seconds");
        }
        this.policy = policy;
        this.service = service;
        this.jdbc = jdbc;
        this.intervalSeconds = intervalSeconds;
        work = new TransactionTemplate(transactions);
        work.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        work.setTimeout(6);
        archiveWork = new TransactionTemplate(transactions);
        archiveWork.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        archiveWork.setTimeout(3);
        failureCheckpoint = new TransactionTemplate(transactions);
        failureCheckpoint.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        failureCheckpoint.setTimeout(1);
    }

    public MaintenanceResult runIfDue() {
        if (!policy.enabled()) return null;
        MaintenanceResult cleaned;
        try {
            cleaned = work.execute(status -> maintain());
        } catch (RuntimeException failure) {
            recordFailure(failure, false);
            throw new GovernanceException(503, "governance_maintenance_failed", "Governance maintenance did not complete");
        }
        if (!"completed".equals(cleaned.status()) || !policy.scheduledArchiveEnabled()) return cleaned;
        // Cleanup and its audit are already committed; a poison receipt cannot block retention.
        try {
            return archiveWork.execute(status -> {
                jdbc.execute("set local statement_timeout = '3s'");
                jdbc.execute("set local lock_timeout = '1s'");
                int archived = service.archive("internal-service", GovernancePolicy.DEFAULT_BATCH).archivedCount();
                WorkCounts before = cleaned.affectedCounts();
                WorkCounts counts = new WorkCounts(before.releasedHolds(), before.expiredPendingReceipts(),
                        before.expiredArchivedReceipts(), before.expiredBatches(), before.purgedReceipts(),
                        before.purgedBatches(), before.purgedAudit(), archived);
                jdbc.update("""
                        update analytics_private.visitor_governance_maintenance
                        set status='completed', failure_code=null, last_affected_counts=?::jsonb where site_id=?
                        """, EvidenceCodec.canonical(counts), policy.siteId());
                return result("completed", counts);
            });
        } catch (RuntimeException failure) {
            recordFailure(failure, true);
            throw new GovernanceException(503, "governance_archive_failed", "Cleanup completed but optional archive did not complete");
        }
    }

    private void recordFailure(RuntimeException failure, boolean archive) {
        String code = archive ? (failure instanceof GovernanceException ? "archive_rejected" : "archive_error") : failureCode(failure);
        try {
            failureCheckpoint.executeWithoutResult(status -> {
                    jdbc.execute("set local statement_timeout = '1s'");
                    jdbc.execute("set local lock_timeout = '500ms'");
                    jdbc.update("""
                            insert into analytics_private.visitor_governance_maintenance
                                (site_id, last_failure_at, status, failure_code)
                            values (?, statement_timestamp(), 'failed', ?)
                            on conflict (site_id) do update set last_failure_at=excluded.last_failure_at,
                                status='failed', failure_code=excluded.failure_code
                            """, policy.siteId(), code);
                if (archive) jdbc.update("""
                        insert into analytics_private.visitor_governance_audit
                            (site_id, actor, action, target, affected_count, outcome)
                        values (?, 'internal-service', 'archive', 'governance', 0, 'rejected')
                        """, policy.siteId());
            });
        } catch (RuntimeException checkpointUnavailable) {
            log.warn("Governance maintenance failure checkpoint unavailable");
        }
        log.warn("Governance maintenance failed: {}", code);
    }

    private MaintenanceResult maintain() {
        jdbc.execute("set local statement_timeout = '6s'");
        jdbc.execute("set local lock_timeout = '1s'");
        boolean locked = Boolean.TRUE.equals(jdbc.queryForObject(
                "select pg_try_advisory_xact_lock(hashtextextended(?::text, 0))", Boolean.class,
                "visitor-governance:" + policy.siteId()));
        if (!locked) return result("busy", WorkCounts.empty());
        boolean recent = Boolean.TRUE.equals(jdbc.queryForObject("""
                select exists(select 1 from analytics_private.visitor_governance_maintenance
                where site_id=? and last_success_at > statement_timestamp() - make_interval(secs => ?))
                """, Boolean.class, policy.siteId(), intervalSeconds));
        if (recent) return result("not_due", WorkCounts.empty());

        // Fixed quotas reserve capacity for every stage even under sustained backlog.
        int audit = purge("audit", 100);
        int receipts = purge("receipts", 100);
        int batches = purge("batches", 49);
        int released = service.releaseExpiredHolds(50);
        PayloadExpiry expired = service.expirePayloads(200, 501);
        WorkCounts counts = new WorkCounts(released, expired.pendingReceipts(), expired.archivedReceipts(),
                expired.batches(), receipts, batches, audit, 0);
        if (counts.cleanupRows() > 1000 || counts.archivedReceipts() > 100) {
            throw new IllegalStateException("Governance maintenance bound exceeded");
        }
        jdbc.update("""
                insert into analytics_private.visitor_governance_maintenance
                    (site_id, last_success_at, status, last_affected_counts)
                values (?, statement_timestamp(), 'completed', ?::jsonb)
                on conflict (site_id) do update set last_success_at=excluded.last_success_at,
                    status='completed', failure_code=null, last_affected_counts=excluded.last_affected_counts
                """, policy.siteId(), EvidenceCodec.canonical(counts));
        return result("completed", counts);
    }

    private int purge(String target, int limit) {
        return jdbc.queryForObject("select analytics_private.purge_visitor_governance(?, ?, ?)",
                Integer.class, policy.siteId(), target, limit);
    }

    private MaintenanceResult result(String status, WorkCounts counts) {
        return new MaintenanceResult(status, service.maintenanceHealth(), counts);
    }

    private static String failureCode(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof QueryTimeoutException || cause instanceof TransactionTimedOutException) return "deadline_exceeded";
        }
        return error instanceof DataAccessException ? "database_error" : "maintenance_error";
    }
}
