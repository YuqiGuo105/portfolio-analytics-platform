package site.yuqi.analytics.alerts.dto;

public record AlertRule(
        Long ruleId,
        String siteId,
        String name,
        String eventType,
        String geoLevel,
        String geoAreaId,
        String granularity,
        long threshold,
        String comparator,
        int cooldownSeconds,
        boolean enabled,
        int version,
        AlertRuleFilters filters
) {
    public AlertRule {
        filters = filters == null ? AlertRuleFilters.all() : filters;
    }

    public AlertRule(Long ruleId, String siteId, String name, String eventType, String geoLevel,
                     String geoAreaId, String granularity, long threshold, String comparator,
                     int cooldownSeconds, boolean enabled, int version) {
        this(ruleId, siteId, name, eventType, geoLevel, geoAreaId, granularity, threshold,
                comparator, cooldownSeconds, enabled, version, AlertRuleFilters.all());
    }
}
