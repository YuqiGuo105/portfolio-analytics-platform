package site.yuqi.analytics.aggregator.governance;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public record GovernancePolicy(
        @Value("${analytics.governance.enabled:false}") boolean enabled,
        @Value("${analytics.governance.scheduled-archive-enabled:false}") boolean scheduledArchiveEnabled,
        @Value("${analytics.governance.receipt-retention-days:90}") int receiptRetentionDays,
        @Value("${analytics.governance.archive-retention-days:90}") int archiveRetentionDays,
        @Value("${analytics.backfill.site-id:yuqi.site}") String siteId) {
    public static final String VERSION = "visitor-evidence-v1";
    public static final int MAX_BATCH = 500;
    public static final int DEFAULT_BATCH = 100;
    public static final Map<String, String> CONTROLS = Map.of(
            "independentTimestamp", "not_configured", "storageProtection", "local_database",
            "authenticity", "local_integrity_only");

    public GovernancePolicy {
        if (enabled && (receiptRetentionDays < 1 || receiptRetentionDays > 90
                || archiveRetentionDays < 1 || archiveRetentionDays > 90)) {
            throw new IllegalArgumentException("Governance payload retention must be 1..90 days");
        }
        if (enabled && (siteId == null || !siteId.matches("[A-Za-z0-9._-]{1,100}"))) {
            throw new IllegalArgumentException("Governance site ID must be a bounded configured identifier");
        }
    }

    public Map<String, Object> contract() {
        return Map.ofEntries(Map.entry("policyVersion", VERSION), Map.entry("schemaVersion", 1),
                Map.entry("receiptRetentionDays", receiptRetentionDays), Map.entry("archiveRetentionDays", archiveRetentionDays),
                Map.entry("maxBatchSize", MAX_BATCH), Map.entry("defaultBatchSize", DEFAULT_BATCH),
                Map.entry("scheduledArchiveEnabled", scheduledArchiveEnabled), Map.entry("holdScope", "governance_batch_only"),
                Map.entry("auditRetentionDays", 180), Map.entry("tombstoneRetentionDays", 180), Map.entry("holdDays", 90));
    }
}
