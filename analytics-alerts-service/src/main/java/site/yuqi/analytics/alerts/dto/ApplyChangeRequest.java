package site.yuqi.analytics.alerts.dto;

/**
 * Request body for applying a previously prepared change.
 */
public record ApplyChangeRequest(
        String changeId,
        String idempotencyKey
) {}
