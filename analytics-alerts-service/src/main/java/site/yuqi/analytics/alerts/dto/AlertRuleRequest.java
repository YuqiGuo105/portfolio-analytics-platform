package site.yuqi.analytics.alerts.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record AlertRuleRequest(
        @NotBlank String siteId,
        @NotBlank String name,
        @NotBlank String eventType,
        @NotBlank @Pattern(regexp = "GLOBAL|COUNTRY|REGION|METRO") String geoLevel,
        String geoAreaId,
        @NotBlank @Pattern(regexp = "5m|1d") String granularity,
        @Min(0) long threshold,
        @NotBlank @Pattern(regexp = ">=|<=") String comparator,
        @Min(60) int cooldownSeconds,
        AlertRuleFilters filters
) {
    public AlertRuleRequest(String siteId, String name, String eventType, String geoLevel,
                            String geoAreaId, String granularity, long threshold, String comparator,
                            int cooldownSeconds) {
        this(siteId, name, eventType, geoLevel, geoAreaId, granularity, threshold,
                comparator, cooldownSeconds, null);
    }
}
