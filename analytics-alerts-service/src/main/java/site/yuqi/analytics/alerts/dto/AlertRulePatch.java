package site.yuqi.analytics.alerts.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Partial patch for an alert rule change. Fields left null are not modified.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AlertRulePatch(
        String name,
        String eventType,
        String geoLevel,
        String geoAreaId,
        String granularity,
        Long threshold,
        String comparator,
        Integer cooldownSeconds,
        Boolean enabled
) {}
