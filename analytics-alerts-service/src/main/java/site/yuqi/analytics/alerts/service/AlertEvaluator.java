package site.yuqi.analytics.alerts.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.alerts.dto.AlertIncident;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertIncidentRepository;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.common.event.Granularity;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Evaluates every enabled alert rule once per minute against the
 * {@code geo_time_rollups} table. Incidents and delivery attempts are durable,
 * so a transient notification failure can be retried without opening a second
 * incident.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AlertEvaluator {

    private final AlertRuleRepository rules;
    private final JdbcTemplate jdbc;
    private final NotificationSender sender;
    private final AlertIncidentRepository incidents;

    @Value("${analytics.alerts.enabled:true}")
    private boolean evalEnabled;

    @Value("${analytics.alerts.notification-retry-seconds:60}")
    private long notificationRetrySeconds;

    @Value("${analytics.alerts.notification-retry-batch-size:25}")
    private int notificationRetryBatchSize;

    @Value("${analytics.alerts.lookback-buckets:2}")
    private int lookbackBuckets;

    @Scheduled(cron = "${analytics.alerts.eval-cron:0 * * * * *}")
    public void tick() {
        if (!evalEnabled) {
            return;
        }
        retryPendingNotifications();
        for (AlertRule r : rules.findEnabled()) {
            try {
                evaluate(r);
            } catch (RuntimeException e) {
                log.warn("{\"event\":\"rule_eval_failed\",\"ruleId\":{},\"err\":\"{}\"}", r.ruleId(), e.getMessage());
            }
        }
    }

    void evaluate(AlertRule r) {
        Granularity g = "1d".equals(r.granularity()) ? Granularity.ONE_DAY : Granularity.FIVE_MIN;
        Instant newest = g.floor(Instant.now());
        int buckets = Math.max(1, lookbackBuckets);
        for (int i = 0; i < buckets; i++) {
            Instant bucket = shiftBack(newest, g, i);
            long count = countMatching(r, bucket);
            if (fires(count, r.threshold(), r.comparator())) {
                openIncident(r, g, bucket, count);
                return;
            }
        }
    }

    private void openIncident(AlertRule r, Granularity g, Instant bucket, long count) {
        Instant cooldownStart = Instant.now().minusSeconds(r.cooldownSeconds());
        if (incidents.existsWithinCooldown(r, cooldownStart)) {
            return;
        }

        String dedupKey = "%d|%s|%s|%d".formatted(
                r.ruleId(),
                r.geoAreaId() == null ? "" : r.geoAreaId(),
                g.code(),
                bucket.getEpochSecond());

        incidents.insert(r, bucket, count, dedupKey).ifPresent(this::deliver);
    }

    private static Instant shiftBack(Instant newest, Granularity granularity, int buckets) {
        if (buckets == 0) return newest;
        return "1d".equals(granularity.code())
                ? newest.minus(Duration.ofDays(buckets))
                : newest.minus(Duration.ofMinutes(5L * buckets));
    }

    void retryPendingNotifications() {
        long retrySeconds = Math.max(1, notificationRetrySeconds);
        int batchSize = Math.max(1, notificationRetryBatchSize);
        Instant retryBefore = Instant.now().minus(Duration.ofSeconds(retrySeconds));
        for (AlertIncident incident : incidents.findPendingNotifications(retryBefore, batchSize)) {
            deliver(incident);
        }
    }

    private void deliver(AlertIncident incident) {
        String alertBody = "%s %s threshold %d (measured %d, bucket %s)".formatted(
                incident.comparator(),
                incident.ruleName(),
                incident.threshold(),
                incident.measuredValue(),
                incident.bucketTime());
        boolean ok = sender.send(Map.of(
                "eventType", "ANALYTICS_ALERT_TRIGGERED",
                "topic", "ADMIN_ALERTS",
                "title", "Alert: " + incident.ruleName(),
                "summary", alertBody,
                "sourceType", "ALERT",
                "sourceId", String.valueOf(incident.ruleId()),
                "idempotencyKey", "incident:" + incident.incidentId(),
                "metadata", Map.of(
                        "siteId", incident.siteId(),
                        "ruleId", incident.ruleId(),
                        "incidentId", incident.incidentId(),
                        "geoAreaId", incident.geoAreaId() == null ? "" : incident.geoAreaId(),
                        "measuredValue", incident.measuredValue(),
                        "threshold", incident.threshold())));
        incidents.recordNotificationResult(incident.incidentId(), ok);
    }

    long countMatching(AlertRule r, Instant bucket) {
        String sql = """
                select coalesce(sum(event_count), 0)
                from geo_time_rollups
                where site_id = ?
                  and granularity = ?
                  and bucket_time = ?
                  and event_type = ?
                  and geo_level = ?
                  and (? = '' or geo_area_id = ?)
                """;
        Long v = jdbc.queryForObject(sql, Long.class,
                r.siteId(), r.granularity(), Timestamp.from(bucket), r.eventType(), r.geoLevel(),
                r.geoAreaId() == null ? "" : r.geoAreaId(),
                r.geoAreaId() == null ? "" : r.geoAreaId());
        return v == null ? 0L : v;
    }

    static boolean fires(long count, long threshold, String cmp) {
        return ">=".equals(cmp) ? count >= threshold : count <= threshold;
    }
}
