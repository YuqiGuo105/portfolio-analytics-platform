package site.yuqi.analytics.alerts.dto;

import java.time.Instant;

public record AlertIncident(
        long incidentId,
        long ruleId,
        String ruleName,
        String siteId,
        String geoAreaId,
        Instant bucketTime,
        String granularity,
        long measuredValue,
        long threshold,
        String comparator,
        boolean notified,
        Instant notifiedAt,
        int notificationAttempts,
        Instant lastNotificationAttemptAt,
        Instant createdAt
) {}
