package site.yuqi.analytics.aggregator.governance;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static site.yuqi.analytics.aggregator.governance.GovernanceAudit.*;
import static site.yuqi.analytics.aggregator.governance.GovernanceModels.*;

@Service
@Transactional(timeout = 10, noRollbackFor = GovernanceException.class)
public class VisitorGovernanceService {
    private static final String RECEIPTS_WITH_SOURCE = """
            select r.*, case when e.event_id is null then null
                else analytics_private.visitor_evidence_v1(e, ?) end as current_source,
                (r.capture_sha256 = analytics_private.visitor_receipt_capture_v1(r)
                 and r.event_ref = encode(sha256(convert_to(r.source_event_id, 'UTF8')), 'hex')) as capture_valid
            from analytics_private.visitor_evidence_receipts r
            left join public.behavior_events e on e.event_id = r.source_event_id and e.site_id = r.site_id
            where r.site_id = ?
            """;
    private final JdbcTemplate jdbc;
    private final GovernancePolicy policy;
    private final GovernanceAudit audit;
    @Value("${analytics.governance.worker-interval-seconds:3600}")
    private int workerIntervalSeconds = 3600;

    public VisitorGovernanceService(JdbcTemplate jdbc, GovernancePolicy policy, GovernanceAudit audit) {
        this.jdbc = jdbc;
        this.policy = policy;
        this.audit = audit;
    }

    public void requireEnabled() {
        if (!policy.enabled()) throw new GovernanceException(404, "governance_disabled", "Visitor governance is disabled");
    }

    public Overview overview(String actor) {
        requireEnabled();
        GovernanceAudit.actor(actor);
        audit.append(actor, Action.OVERVIEW, Target.GOVERNANCE, null, null, 0, Outcome.REQUESTED);
        Counts counts = jdbc.queryForObject("""
                select count(*) as received,
                    count(*) filter (where r.status = 'pending') as pending,
                    count(*) filter (where r.status = 'archived') as archived,
                    count(*) filter (where r.status = 'expired') as expired,
                    count(*) filter (where r.status = 'archived' and b.held and b.hold_expires_at > statement_timestamp()) as held
                from analytics_private.visitor_evidence_receipts r
                left join analytics_private.visitor_evidence_batches b on b.batch_id = r.batch_id
                where r.site_id = ?
                """, (rs, n) -> new Counts(rs.getLong("received"), rs.getLong("pending"),
                rs.getLong("archived"), rs.getLong("expired"), rs.getLong("held")), policy.siteId());
        List<Batch> batches = jdbc.query("""
                select batch_id, created_at, expires_at, receipt_count, manifest_sha256, status, held, hold_reason, hold_expires_at
                from analytics_private.visitor_evidence_batches where site_id = ?
                order by created_at desc, batch_id desc limit 20
                """, (rs, n) -> batch(rs), policy.siteId());
        List<Audit> latest = jdbc.query("""
                select * from analytics_private.visitor_governance_audit where site_id = ?
                order by audit_id desc limit 20
                """, (rs, n) -> new Audit(rs.getString("audit_id"), instant(rs, "occurred_at"),
                rs.getString("actor"), rs.getString("action"), rs.getString("target"),
                rs.getString("batch_id"), rs.getString("reason"), rs.getInt("affected_count"),
                rs.getString("outcome")), policy.siteId());
        return new Overview(true, policy.contract(), counts, batches, latest, GovernancePolicy.CONTROLS, maintenanceHealth());
    }

    public MaintenanceHealth maintenanceHealth() {
        requireEnabled();
        List<MaintenanceHealth> rows = jdbc.query("""
                select last_success_at, last_failure_at, status, last_affected_counts::text as counts
                from analytics_private.visitor_governance_maintenance where site_id = ?
                """, (rs, n) -> new MaintenanceHealth(instant(rs, "last_success_at"), instant(rs, "last_failure_at"),
                rs.getString("status"), workerIntervalSeconds, EvidenceCodec.parse(rs.getString("counts"))), policy.siteId());
        return rows.isEmpty() ? new MaintenanceHealth(null, null, "never_run", workerIntervalSeconds,
                EvidenceCodec.parse("{}")) : rows.getFirst();
    }

    public ArchiveResult archive(String actor, int limit) {
        requireEnabled();
        GovernanceAudit.actor(actor);
        if (limit < 1 || limit > GovernancePolicy.MAX_BATCH) throw GovernanceException.invalid();
        List<StoredReceipt> rows = jdbc.query(RECEIPTS_WITH_SOURCE + """
                and r.status = 'pending' and r.expires_at > statement_timestamp()
                order by r.receipt_id limit ? for update of r skip locked
                """, (rs, n) -> receipt(rs), policy.siteId(), policy.siteId(), limit);
        if (rows.isEmpty()) {
            audit.append(actor, Action.ARCHIVE, Target.GOVERNANCE, null, null, 0, Outcome.EMPTY);
            return new ArchiveResult("empty", null, 0);
        }
        if (!validReceipts(rows) || rows.stream().anyMatch(r -> r.currentSource() != null
                && !r.currentSource().equals(r.value().canonicalEvidence()))) {
            audit.append(actor, Action.ARCHIVE, Target.GOVERNANCE, null, null, rows.size(), Outcome.REJECTED);
            throw integrityFailure();
        }
        UUID id = UUID.randomUUID();
        Instant created = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Instant expires = created.plus(Duration.ofDays(policy.archiveRetentionDays()));
        for (StoredReceipt row : rows) {
            Instant receiptExpiry = Instant.parse(row.value().expiresAt());
            if (receiptExpiry.isBefore(expires)) expires = receiptExpiry;
        }
        String canonical = manifest(id.toString(), created.toString(), expires.toString(), rows);
        String digest = EvidenceCodec.sha256(canonical);
        jdbc.update("""
                insert into analytics_private.visitor_evidence_batches
                    (batch_id, site_id, created_at, expires_at, receipt_count, canonical_manifest, manifest_sha256)
                values (?, ?, ?, ?, ?, ?, ?)
                """, id, policy.siteId(), Timestamp.from(created), Timestamp.from(expires), rows.size(), canonical, digest);
        jdbc.update("""
                update analytics_private.visitor_evidence_receipts set status = 'archived', batch_id = ?
                where site_id = ? and receipt_id in (select value::bigint from jsonb_array_elements_text(?::jsonb))
                """, id, policy.siteId(), EvidenceCodec.canonical(rows.stream().map(r -> r.value().receiptId()).toList()));
        audit.append(actor, Action.ARCHIVE, Target.GOVERNANCE, id, null, rows.size(), Outcome.COMPLETED);
        return new ArchiveResult("archived", new Batch(id.toString(), created.toString(), expires.toString(),
                rows.size(), digest, "archived", false, null, null), rows.size());
    }

    public Verification verify(String actor, UUID batchId) {
        requireEnabled();
        GovernanceAudit.actor(actor);
        StoredBatch stored = lockBatch(batchId);
        Verification result = inspect(stored);
        audit.append(actor, Action.VERIFY, Target.GOVERNANCE, batchId, null,
                stored.value().receiptCount(), Outcome.valueOf(result.status().toUpperCase(java.util.Locale.ROOT)));
        return result;
    }

    public ExportResult export(String actor, UUID batchId) {
        requireEnabled();
        GovernanceAudit.actor(actor);
        StoredBatch stored = lockBatch(batchId);
        Verification result = inspect(stored);
        if (result.status().equals("expired")) {
            audit.append(actor, Action.EXPORT, Target.GOVERNANCE, batchId, null, 0, Outcome.REJECTED);
            throw expired();
        }
        if (!result.status().equals("verified")) {
            audit.append(actor, Action.EXPORT, Target.GOVERNANCE, batchId, null, 0, Outcome.REJECTED);
            throw integrityFailure();
        }
        audit.append(actor, Action.EXPORT, Target.GOVERNANCE, batchId, null,
                stored.value().receiptCount(), Outcome.REQUESTED);
        return new ExportResult(batchId.toString(), stored.value().manifestSha256(), stored.canonical(),
                EvidenceCodec.parse(stored.canonical()));
    }

    public HoldResult hold(String actor, UUID batchId, String reason, boolean hold) {
        requireEnabled();
        GovernanceAudit.actor(actor);
        if (reason == null || !Set.of("incident_review", "legal_preservation", "data_quality").contains(reason)) {
            throw GovernanceException.invalid();
        }
        StoredBatch stored = lockBatch(batchId);
        Instant now = jdbc.queryForObject("select statement_timestamp()", Timestamp.class).toInstant();
        if (stored.value().held() && !activeHold(stored.value(), now)) {
            releaseExpiredHold(batchId, stored.value().holdReason(), stored.value().receiptCount());
            stored = lockBatch(batchId);
        }
        Action action = hold ? (stored.value().held() ? Action.RENEW_HOLD : Action.HOLD) : Action.RELEASE_HOLD;
        if (stored.value().status().equals("expired")
                || (!activeHold(stored.value(), now) && !Instant.parse(stored.value().expiresAt()).isAfter(now))) {
            audit.append(actor, action, Target.GOVERNANCE, batchId, reason, 0, Outcome.REJECTED);
            throw expired();
        }
        boolean changed = hold || stored.value().held();
        if (hold) jdbc.update("""
                update analytics_private.visitor_evidence_batches set held = true, hold_reason = ?,
                    hold_set_at = statement_timestamp(), hold_expires_at = statement_timestamp() + interval '90 days'
                where batch_id = ? and site_id = ?
                """, reason, batchId, policy.siteId());
        else if (changed) jdbc.update("""
                update analytics_private.visitor_evidence_batches set held = false, hold_reason = null,
                    hold_set_at = null, hold_expires_at = null where batch_id = ? and site_id = ?
                """, batchId, policy.siteId());
        audit.append(actor, action, Target.GOVERNANCE, batchId, reason,
                stored.value().receiptCount(), changed ? Outcome.COMPLETED : Outcome.UNCHANGED);
        return new HoldResult(lockBatch(batchId).value(), changed);
    }

    /** Compatibility helper; scheduled maintenance uses explicit fixed quotas. */
    public int expire() {
        return expirePayloads(200, 501).receipts();
    }

    public int releaseExpiredHolds(int limit) {
        if (!policy.enabled()) return 0;
        if (limit < 1 || limit > 500) throw GovernanceException.invalid();
        List<Batch> rows = jdbc.query("""
                select batch_id, created_at, expires_at, receipt_count, manifest_sha256, status, held, hold_reason, hold_expires_at
                from analytics_private.visitor_evidence_batches where site_id = ? and held
                    and hold_expires_at <= statement_timestamp()
                order by hold_expires_at, batch_id limit ? for update skip locked
                """, (rs, n) -> batch(rs), policy.siteId(), limit);
        for (Batch row : rows) releaseExpiredHold(UUID.fromString(row.batchId()), row.holdReason(), row.receiptCount());
        return rows.size();
    }

    private void releaseExpiredHold(UUID id, String reason, int receiptCount) {
        jdbc.update("""
                update analytics_private.visitor_evidence_batches set held=false, hold_reason=null,
                    hold_set_at=null, hold_expires_at=null where batch_id=? and site_id=?
                """, id, policy.siteId());
        audit.append("internal-service", Action.EXPIRE_HOLD, Target.GOVERNANCE, id, reason,
                receiptCount, Outcome.COMPLETED);
    }

    public PayloadExpiry expirePayloads(int pendingLimit, int batchRowBudget) {
        if (!policy.enabled()) return new PayloadExpiry(0, 0, 0);
        if (pendingLimit < 1 || pendingLimit > 500 || batchRowBudget < 1 || batchRowBudget > 501) {
            throw GovernanceException.invalid();
        }
        int pending = jdbc.update("""
                update analytics_private.visitor_evidence_receipts
                set status = 'expired', canonical_evidence = null, source_event_id = null
                where receipt_id in (
                    select receipt_id from analytics_private.visitor_evidence_receipts
                    where site_id = ? and status = 'pending' and expires_at <= statement_timestamp()
                    order by expires_at, receipt_id limit ? for update skip locked)
                """, policy.siteId(), pendingLimit);
        if (pending > 0) audit.append("internal-service", Action.EXPIRE_PENDING, Target.GOVERNANCE,
                null, null, pending, Outcome.COMPLETED);
        List<BatchExpiry> batches = jdbc.query("""
                select batch_id, receipt_count from analytics_private.visitor_evidence_batches
                where site_id = ? and status = 'archived' and not held and expires_at <= statement_timestamp()
                order by expires_at, batch_id limit 10 for update skip locked
                """, (rs, n) -> new BatchExpiry(rs.getObject("batch_id", UUID.class), rs.getInt("receipt_count")), policy.siteId());
        int archived = 0;
        int expiredBatches = 0;
        for (BatchExpiry batch : batches) {
            if (batch.receipts() + 1 > batchRowBudget - archived - expiredBatches) break;
            int actual = jdbc.queryForObject("""
                    select count(*) from (select receipt_id from analytics_private.visitor_evidence_receipts
                    where batch_id=? and site_id=? and status='archived' limit 501) bounded_members
                    """, Integer.class, batch.id(), policy.siteId());
            if (actual != batch.receipts()) throw new IllegalStateException("Invalid governance batch membership");
            int changed = jdbc.update("""
                    update analytics_private.visitor_evidence_receipts
                    set status = 'expired', canonical_evidence = null, source_event_id = null
                    where batch_id = ? and site_id = ? and status = 'archived'
                    """, batch.id(), policy.siteId());
            jdbc.update("""
                    update analytics_private.visitor_evidence_batches set status = 'expired', canonical_manifest = null
                    where batch_id = ? and site_id = ?
                    """, batch.id(), policy.siteId());
            audit.append("internal-service", Action.EXPIRE_BATCH, Target.GOVERNANCE, batch.id(), null,
                    changed, Outcome.COMPLETED);
            archived += changed;
            expiredBatches++;
        }
        return new PayloadExpiry(pending, archived, expiredBatches);
    }

    private Verification inspect(StoredBatch stored) {
        Batch b = stored.value();
        String checkedAt = Instant.now().toString();
        if (b.status().equals("expired") || (!activeHold(b, Instant.parse(checkedAt))
                && !Instant.parse(b.expiresAt()).isAfter(Instant.parse(checkedAt)))) {
            return new Verification(b.batchId(), checkedAt, "expired", null,
                null, "not_checked", 0, 0, 0);
        }
        List<StoredReceipt> rows = jdbc.query(RECEIPTS_WITH_SOURCE + """
                and r.batch_id = ? order by r.receipt_id limit 501
                """, (rs, n) -> receipt(rs), policy.siteId(), policy.siteId(), UUID.fromString(b.batchId()));
        boolean receiptsValid = rows.size() == b.receiptCount() && validReceipts(rows)
                && rows.stream().allMatch(r -> "archived".equals(r.status()));
        boolean manifestValid = stored.canonical() != null
                && EvidenceCodec.sha256(stored.canonical()).equals(b.manifestSha256())
                && stored.canonical().equals(manifest(b.batchId(), b.createdAt(), b.expiresAt(), rows));
        int present = 0;
        int mismatches = 0;
        for (StoredReceipt row : rows) {
            if (row.currentSource() == null) continue;
            present++;
            if (!Objects.equals(row.currentSource(), row.value().canonicalEvidence())) mismatches++;
        }
        int missing = rows.size() - present;
        String source = mismatches > 0 ? "mismatch" : present == 0 ? "unavailable"
                : missing > 0 ? "partial" : "matched";
        String status = manifestValid && receiptsValid && mismatches == 0 ? "verified" : "tampered";
        return new Verification(b.batchId(), checkedAt, status, manifestValid, receiptsValid,
                source, present, missing, mismatches);
    }

    private StoredBatch lockBatch(UUID id) {
        if (id == null) throw GovernanceException.invalid();
        List<StoredBatch> rows = jdbc.query("""
                select * from analytics_private.visitor_evidence_batches where batch_id = ? and site_id = ? for update
                """, (rs, n) -> new StoredBatch(batch(rs), rs.getString("canonical_manifest")), id, policy.siteId());
        if (rows.isEmpty()) throw new GovernanceException(404, "batch_not_found", "Governance batch not found");
        return rows.getFirst();
    }

    private static boolean validReceipts(List<StoredReceipt> rows) {
        return rows.stream().allMatch(r -> r.captureValid() && r.value().canonicalEvidence() != null
                && EvidenceCodec.sha256(r.value().canonicalEvidence()).equals(r.value().evidenceSha256()));
    }

    private static String manifest(String id, String created, String expires, List<StoredReceipt> rows) {
        return EvidenceCodec.canonical(new Manifest(1, GovernancePolicy.VERSION, id, created, expires,
                GovernancePolicy.CONTROLS, rows.stream().map(StoredReceipt::value).toList()));
    }

    private static Batch batch(ResultSet rs) throws SQLException {
        return new Batch(rs.getString("batch_id"), instant(rs, "created_at"), instant(rs, "expires_at"),
                rs.getInt("receipt_count"), rs.getString("manifest_sha256"), rs.getString("status"),
                rs.getBoolean("held"), rs.getString("hold_reason"), instant(rs, "hold_expires_at"));
    }

    private static StoredReceipt receipt(ResultSet rs) throws SQLException {
        return new StoredReceipt(new Receipt(rs.getString("receipt_id"), rs.getString("event_ref"),
                instant(rs, "received_at"), instant(rs, "expires_at"), rs.getString("canonical_evidence"),
                rs.getString("evidence_sha256")), rs.getString("current_source"), rs.getString("status"),
                rs.getBoolean("capture_valid"));
    }

    private static String instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant().toString();
    }

    private static boolean activeHold(Batch batch, Instant now) {
        return batch.held() && batch.holdExpiresAt() != null && Instant.parse(batch.holdExpiresAt()).isAfter(now);
    }

    private static GovernanceException integrityFailure() {
        return new GovernanceException(409, "integrity_check_failed", "Evidence integrity check failed");
    }

    private static GovernanceException expired() {
        return new GovernanceException(409, "batch_expired", "Governance batch has expired");
    }

    private record StoredBatch(Batch value, String canonical) {}
    private record StoredReceipt(Receipt value, String currentSource, String status, boolean captureValid) {}
    private record BatchExpiry(UUID id, int receipts) {}
}
