package site.yuqi.analytics.alerts.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Represents a prepared change, returned by the prepare endpoint.
 * The changeId must be passed back to apply to ensure what the user confirmed
 * is exactly what gets executed.
 */
public record PreparedChange(
        String changeId,
        String action,       // CREATE, UPDATE, SET_ENABLED
        Long ruleId,
        Map<String, Object> before,
        Map<String, Object> after,
        Map<String, Object> diff,
        List<String> warnings,
        int expectedVersion,
        Instant expiresAt
) {}
