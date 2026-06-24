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
        boolean enabled
) {}
