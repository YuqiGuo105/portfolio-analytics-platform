package site.yuqi.analytics.alerts.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.alerts.dto.AlertIncident;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertIncidentRepository;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.alerts.operations.OperationEventPublisher;
import site.yuqi.analytics.common.event.Granularity;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final OperationEventPublisher operations;

    @Value("${analytics.alerts.enabled:true}")
    private boolean evalEnabled;

    @Value("${analytics.alerts.notification-retry-seconds:60}")
    private long notificationRetrySeconds;

    @Value("${analytics.alerts.notification-retry-batch-size:25}")
    private int notificationRetryBatchSize;

    @Value("${analytics.alerts.notification-lease-seconds:120}")
    private long notificationLeaseSeconds;

    @Value("${analytics.alerts.notification-max-attempts:8}")
    private int notificationMaxAttempts;

    @Value("${analytics.alerts.lookback-buckets:2}")
    private int lookbackBuckets;

    @Scheduled(cron = "${analytics.alerts.eval-cron:0 * * * * *}")
    public void tick() {
        if (!evalEnabled) {
            return;
        }
        retryPendingNotifications();
        Map<String, List<AlertRule>> byGranularity = new HashMap<>();
        for (AlertRule rule : rules.findEnabled()) {
            try {
                compile(rule);
                byGranularity.computeIfAbsent(rule.granularity(), ignored -> new ArrayList<>()).add(rule);
            } catch (IllegalArgumentException e) {
                log.warn("{\"event\":\"rule_compile_failed\",\"ruleId\":{},\"version\":{},\"err\":\"{}\"}",
                        rule.ruleId(), rule.version(), e.getMessage());
            }
        }
        for (Map.Entry<String, List<AlertRule>> entry : byGranularity.entrySet()) {
            try {
                evaluateBatch(entry.getValue());
            } catch (RuntimeException e) {
                log.warn("{\"event\":\"rule_batch_eval_failed\",\"granularity\":\"{}\",\"rules\":{},\"err\":\"{}\"}",
                        entry.getKey(), entry.getValue().size(), e.getMessage());
            }
        }
    }

    void evaluateBatch(List<AlertRule> candidates) {
        if (candidates.isEmpty()) return;
        Granularity granularity = "1d".equals(candidates.getFirst().granularity())
                ? Granularity.ONE_DAY : Granularity.FIVE_MIN;
        Instant newest = granularity.floor(Instant.now());
        Set<Long> pending = candidates.stream().map(AlertRule::ruleId).collect(Collectors.toCollection(HashSet::new));
        Map<Long, AlertRule> byId = candidates.stream().collect(Collectors.toMap(AlertRule::ruleId, rule -> rule));
        for (int i = 0; i < Math.max(1, lookbackBuckets) && !pending.isEmpty(); i++) {
            Instant bucket = shiftBack(newest, granularity, i);
            Map<Long, Long> measurements = countMatchingBatch(
                    pending.stream().map(byId::get).toList(), bucket);
            for (Long ruleId : List.copyOf(pending)) {
                AlertRule rule = byId.get(ruleId);
                long measured = measurements.getOrDefault(ruleId, 0L);
                if (fires(measured, rule.threshold(), rule.comparator())) {
                    openIncident(rule, granularity, bucket, measured);
                    pending.remove(ruleId);
                }
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

        incidents.insert(r, bucket, count, dedupKey).ifPresent(incident -> {
            operations.publish(incident, "visitor.alert.triggered", "completed", 1);
            incidents.claimNotification(incident.incidentId(), Instant.now(), notificationLeaseSeconds)
                    .ifPresent(this::deliver);
        });
    }

    private static Instant shiftBack(Instant newest, Granularity granularity, int buckets) {
        if (buckets == 0) return newest;
        return "1d".equals(granularity.code())
                ? newest.minus(Duration.ofDays(buckets))
                : newest.minus(Duration.ofMinutes(5L * buckets));
    }

    void retryPendingNotifications() {
        int batchSize = Math.max(1, notificationRetryBatchSize);
        Instant now = Instant.now();
        for (AlertIncident incident : incidents.claimPendingNotifications(
                now, batchSize, Math.max(1, notificationLeaseSeconds))) {
            deliver(incident);
        }
    }

    private void deliver(AlertIncident incident) {
        int attempt = incident.notificationAttempts();
        String correlationId = "visitor-alert:" + incident.incidentId();
        String alertBody = "%s %s threshold %d (measured %d, bucket %s)".formatted(
                incident.comparator(),
                incident.ruleName(),
                incident.threshold(),
                incident.measuredValue(),
                incident.bucketTime());
        boolean ok = sender.send(Map.ofEntries(
                Map.entry("eventType", "ANALYTICS_ALERT_TRIGGERED"),
                Map.entry("topic", "ADMIN_ALERTS"),
                Map.entry("title", "Alert: " + incident.ruleName()),
                Map.entry("summary", alertBody),
                Map.entry("sourceType", "ALERT"),
                Map.entry("sourceId", String.valueOf(incident.ruleId())),
                Map.entry("schemaVersion", 1),
                Map.entry("correlationId", correlationId),
                Map.entry("causationId", "incident:" + incident.incidentId()),
                Map.entry("idempotencyKey", "incident:" + incident.incidentId()),
                Map.entry("metadata", Map.of(
                        "siteId", incident.siteId(),
                        "ruleId", incident.ruleId(),
                        "incidentId", incident.incidentId(),
                        "geoAreaId", incident.geoAreaId() == null ? "" : incident.geoAreaId(),
                        "measuredValue", incident.measuredValue(),
                        "threshold", incident.threshold()))));
        boolean recorded = incidents.recordNotificationResult(
                incident.incidentId(),
                attempt,
                ok,
                Instant.now(),
                Math.max(1, notificationRetrySeconds),
                Math.max(1, notificationMaxAttempts),
                ok ? null : "Notification service rejected delivery");
        if (!recorded) {
            log.warn("{\"event\":\"stale_notification_result\",\"incidentId\":{},\"attempt\":{}}",
                    incident.incidentId(), attempt);
            return;
        }
        operations.publish(
                incident,
                ok ? "visitor.alert.notification_dispatched" : "visitor.alert.notification_failed",
                ok ? "completed" : "failed",
                attempt);
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

    Map<Long, Long> countMatchingBatch(List<AlertRule> batch, Instant bucket) {
        if (batch.isEmpty()) return Map.of();
        String values = batch.stream().map(ignored -> "(?, ?, ?, ?, ?, ?)")
                .collect(Collectors.joining(", "));
        String sql = """
                with requested(rule_id, site_id, event_type, geo_level, geo_area_id, granularity) as (
                    values %s
                )
                select requested.rule_id, coalesce(sum(r.event_count), 0) as measured
                from requested
                left join geo_time_rollups r
                  on r.site_id = requested.site_id
                 and r.granularity = requested.granularity
                 and r.bucket_time = ?
                 and r.event_type = requested.event_type
                 and r.geo_level = requested.geo_level
                 and (requested.geo_area_id = '' or r.geo_area_id = requested.geo_area_id)
                group by requested.rule_id
                """.formatted(values);
        List<Object> args = new ArrayList<>(batch.size() * 6 + 1);
        for (AlertRule rule : batch) {
            args.add(rule.ruleId());
            args.add(rule.siteId());
            args.add(rule.eventType());
            args.add(rule.geoLevel());
            args.add(rule.geoAreaId() == null ? "" : rule.geoAreaId());
            args.add(rule.granularity());
        }
        args.add(Timestamp.from(bucket));
        Map<Long, Long> result = new HashMap<>();
        RowCallbackHandler collector = rs ->
                result.put(rs.getLong("rule_id"), rs.getLong("measured"));
        jdbc.query(sql, collector, args.toArray());
        return result;
    }

    private static void compile(AlertRule rule) {
        if (!"5m".equals(rule.granularity()) && !"1d".equals(rule.granularity())) {
            throw new IllegalArgumentException("Unsupported granularity: " + rule.granularity());
        }
        ComparatorPolicy.parse(rule.comparator());
    }

    static boolean fires(long count, long threshold, String cmp) {
        return ComparatorPolicy.parse(cmp).test(count, threshold);
    }

    enum ComparatorPolicy {
        AT_LEAST(">=") {
            @Override boolean test(long measured, long threshold) { return measured >= threshold; }
        },
        AT_MOST("<=") {
            @Override boolean test(long measured, long threshold) { return measured <= threshold; }
        };

        private final String wireValue;

        ComparatorPolicy(String wireValue) {
            this.wireValue = wireValue;
        }

        abstract boolean test(long measured, long threshold);

        static ComparatorPolicy parse(String value) {
            for (ComparatorPolicy policy : values()) {
                if (policy.wireValue.equals(value)) return policy;
            }
            throw new IllegalArgumentException("Unsupported comparator: " + value);
        }
    }
}
