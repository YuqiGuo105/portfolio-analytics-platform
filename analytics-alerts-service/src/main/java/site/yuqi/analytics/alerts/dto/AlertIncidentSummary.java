package site.yuqi.analytics.alerts.dto;

public record AlertIncidentSummary(
        long total,
        long notified,
        long pendingNotification
) {}
