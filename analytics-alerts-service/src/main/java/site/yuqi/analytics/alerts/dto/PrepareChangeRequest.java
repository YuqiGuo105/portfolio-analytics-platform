package site.yuqi.analytics.alerts.dto;

/**
 * Request body for the prepare endpoint.
 */
public record PrepareChangeRequest(
        String action,    // CREATE, UPDATE, SET_ENABLED
        Long ruleId,      // null for CREATE
        AlertRulePatch patch,
        String reason,
        String actor
) {}
