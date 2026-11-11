package site.yuqi.analytics.aggregator.governance;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.UUID;

@Service
public class GovernanceAudit {
    public enum Action { VISITOR_READ, VISITOR_EXPORT, OVERVIEW, ARCHIVE, VERIFY, EXPORT,
        HOLD, RELEASE_HOLD, RENEW_HOLD, EXPIRE_HOLD, EXPIRE_PENDING, EXPIRE_BATCH,
        PURGE_RECEIPTS, PURGE_BATCHES, PURGE_AUDIT }
    public enum Target { VISITORS, VISITOR_VISITS, VISITOR_OVERVIEW, VISITOR_INTELLIGENCE, GOVERNANCE }
    public enum Outcome { REQUESTED, COMPLETED, EMPTY, VERIFIED, TAMPERED, EXPIRED, UNCHANGED, REJECTED }

    private final JdbcTemplate jdbc;
    private final GovernancePolicy policy;

    public GovernanceAudit(JdbcTemplate jdbc, GovernancePolicy policy) {
        this.jdbc = jdbc;
        this.policy = policy;
    }

    public static String actor(String value) {
        if (value == null) return "internal-service";
        if (!value.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new GovernanceException(400, "invalid_actor", "Actor must be a bounded pseudonymous identifier");
        }
        return value;
    }

    public void append(String actor, Action action, Target target, UUID batchId, String reason,
                       int count, Outcome outcome) {
        append(actor, action, target, batchId, reason, count, outcome, null, null);
    }

    public void append(String actor, Action action, Target target, UUID batchId, String reason,
                       int count, Outcome outcome, UUID requestId, String exportFormat) {
        if (!policy.enabled()) return;
        jdbc.update("""
                insert into analytics_private.visitor_governance_audit
                  (site_id, actor, action, target, batch_id, reason, affected_count, outcome, request_id, export_format)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, policy.siteId(), actor(actor), lower(action), lower(target).replace('_', '-'),
                batchId, reason, count, lower(outcome), requestId, exportFormat);
    }

    private static String lower(Enum<?> value) { return value.name().toLowerCase(Locale.ROOT); }
}
