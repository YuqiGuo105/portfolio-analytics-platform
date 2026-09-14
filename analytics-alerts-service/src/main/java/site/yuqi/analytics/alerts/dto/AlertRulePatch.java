package site.yuqi.analytics.alerts.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;

/**
 * Partial patch for an alert rule change. Fields left null are not modified.
 */
public record AlertRulePatch(
        String name,
        String eventType,
        String geoLevel,
        String geoAreaId,
        String granularity,
        Long threshold,
        String comparator,
        Integer cooldownSeconds,
        Boolean enabled,
        String siteId,
        AlertRuleFilters filters
) {
    public AlertRulePatch(String name, String eventType, String geoLevel, String geoAreaId,
                          String granularity, Long threshold, String comparator,
                          Integer cooldownSeconds, Boolean enabled, String siteId) {
        this(name, eventType, geoLevel, geoAreaId, granularity, threshold, comparator,
                cooldownSeconds, enabled, siteId, null);
    }

    @JsonAnySetter
    public void rejectUnknown(String name, Object value) {
        throw new IllegalArgumentException("Unknown rule patch field: " + name);
    }
}
