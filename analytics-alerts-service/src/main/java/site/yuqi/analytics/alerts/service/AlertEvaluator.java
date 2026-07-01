package site.yuqi.analytics.alerts.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.common.event.Granularity;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

/**
 * Evaluates every enabled alert rule once per minute against the
 * {@code geo_time_rollups} table. When a rule trips, we insert an
 * incidents row (UNIQUE dedup_key gives us idempotency under retries)
 * and best-effort POST to portfolio-notification-service.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AlertEvaluator {

    private final AlertRuleRepository rules;
    private final JdbcTemplate jdbc;
    private final NotificationSender sender;

    @Value("${analytics.alerts.enabled:true}")
    private boolean evalEnabled;

    @Scheduled(cron = "${analytics.alerts.eval-cron:0 * * * * *}")
    public void tick() {
        if (!evalEnabled) {
            return;
        }
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
        Instant bucket = g.floor(Instant.now());
        long count = countMatching(r, bucket);
        if (!fires(count, r.threshold(), r.comparator())) {
            return;
        }
        String dedupKey = "%d|%s|%s|%d".formatted(
                r.ruleId(),
                r.geoAreaId() == null ? "" : r.geoAreaId(),
                g.code(),
                bucket.getEpochSecond());

        int inserted = jdbc.update("""
                insert into incidents
                    (rule_id, site_id, geo_area_id, bucket_time, granularity,
                     measured_value, threshold, comparator, dedup_key)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (dedup_key) do nothing
                """,
                r.ruleId(), r.siteId(), r.geoAreaId(),
                Timestamp.from(bucket), g.code(),
                count, r.threshold(), r.comparator(), dedupKey);

        if (inserted == 0) {
            // Already fired for this bucket — cooldown handled implicitly by
            // bucket boundaries (one fire per bucket per rule per area).
            return;
        }

        String alertBody = "%s %d %s threshold %d (bucket %s)".formatted(
                r.eventType(), count, r.comparator(), r.threshold(), bucket);
        boolean ok = sender.send(Map.of(
                "eventType", "FEATURE_RELEASED",
                "topic", "FEATURE_UPDATES",
                "title", "Alert: " + r.name(),
                "summary", alertBody,
                "sourceType", "ALERT",
                "sourceId", String.valueOf(r.ruleId()),
                "idempotencyKey", dedupKey,
                "metadata", Map.of(
                        "siteId", r.siteId(),
                        "ruleId", r.ruleId(),
                        "geoAreaId", r.geoAreaId() == null ? "" : r.geoAreaId(),
                        "measuredValue", count,
                        "threshold", r.threshold())));

        if (ok) {
            jdbc.update("update incidents set notified = true, notified_at = now() where dedup_key = ?", dedupKey);
        }
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
