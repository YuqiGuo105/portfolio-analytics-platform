package site.yuqi.analytics.aggregator.governance;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

public final class GovernanceModels {
    private GovernanceModels() {}

    public record Batch(String batchId, String createdAt, String expiresAt, int receiptCount,
                        String manifestSha256, String status, boolean held, String holdReason, String holdExpiresAt) {}
    public record Audit(String auditId, String occurredAt, String actor, String action, String target,
                        String batchId, String reason, int affectedCount, String outcome) {}
    public record Counts(long received, long pending, long archived, long expired, long held) {}
    public record Overview(boolean enabled, Map<String, Object> policy, Counts counts,
                           List<Batch> latestBatches, List<Audit> latestAudit,
                           Map<String, String> externalControls, MaintenanceHealth maintenance) {}
    public record MaintenanceHealth(String lastSuccessAt, String lastFailureAt, String status,
                                    int intervalSeconds, JsonNode lastAffectedCounts) {}
    public record MaintenanceResult(String status, MaintenanceHealth maintenance, WorkCounts affectedCounts) {}
    public record PayloadExpiry(int pendingReceipts, int archivedReceipts, int batches) {
        public int receipts() { return pendingReceipts + archivedReceipts; }
    }
    public record WorkCounts(int releasedHolds, int expiredPendingReceipts, int expiredArchivedReceipts,
                             int expiredBatches, int purgedReceipts, int purgedBatches, int purgedAudit,
                             int archivedReceipts) {
        public int cleanupRows() { return releasedHolds + expiredPendingReceipts + expiredArchivedReceipts
                + expiredBatches + purgedReceipts + purgedBatches + purgedAudit; }
        public static WorkCounts empty() { return new WorkCounts(0, 0, 0, 0, 0, 0, 0, 0); }
    }
    public record ArchiveResult(String status, Batch batch, int archivedCount) {}
    public record Verification(String batchId, String verifiedAt, String status, Boolean manifestValid,
                               Boolean receiptHashesValid, String sourceIntegrity,
                               int sourceCheckedCount, int sourceMissingCount, int sourceMismatchCount) {}
    public record Receipt(String receiptId, String eventRef, String receivedAt, String expiresAt,
                          String canonicalEvidence, String evidenceSha256) {}
    public record Manifest(int schemaVersion, String policyVersion, String batchId,
                           String createdAt, String expiresAt, Map<String, String> externalControls,
                           List<Receipt> receipts) {}
    public record ExportResult(String batchId, String manifestSha256, String canonicalManifest,
                               JsonNode manifest) {}
    public record HoldResult(Batch batch, boolean changed) {}
}
