package site.yuqi.analytics.alerts.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.dto.AlertRuleRequest;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.common.event.Granularity;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class VisitorIntelligenceService {
    private final JdbcTemplate jdbc;
    private final AlertRuleRepository rules;
    private final AlertEvaluator evaluator;

    public List<Map<String, Object>> segmentPreview(String siteId, String eventType,
                                                     String geoLevel, String geoAreaId,
                                                     int hours, int limit) {
        String area = geoAreaId == null ? "" : geoAreaId.trim();
        String event = eventType == null || eventType.isBlank() ? "page_view"
                : eventType.trim().toLowerCase(java.util.Locale.ROOT);
        if (hours < 1 || hours > 2160) throw new IllegalArgumentException("hours must be between 1 and 2160");
        Instant to = Instant.now();
        Instant from = to.minusSeconds(3600L * hours);
        return jdbc.queryForList("""
                select geo_level, geo_area_id, coalesce(sum(event_count), 0) as event_count,
                       min(bucket_time) as first_bucket, max(bucket_time) as last_bucket,
                       '5m' as granularity, true as includes_bot_traffic
                from geo_time_rollups
                where site_id = ? and event_type = ? and geo_level = ?
                  and granularity = '5m'
                  and bucket_time >= ? and bucket_time < ? and (? = '' or geo_area_id = ?)
                group by geo_level, geo_area_id
                order by event_count desc
                limit ?
                """, siteId, event, geoLevel, Timestamp.from(from), Timestamp.from(to),
                area, area, Math.max(1, Math.min(limit, 100)));
    }

    public Map<String, Object> test(AlertRuleRequest request) {
        AlertRule draft = new AlertRule(0L, request.siteId(), request.name(), request.eventType(),
                request.geoLevel(), request.geoAreaId(), request.granularity(), request.threshold(),
                request.comparator(), request.cooldownSeconds(), false, 0);
        Instant bucket = ("1d".equals(draft.granularity()) ? Granularity.ONE_DAY : Granularity.FIVE_MIN)
                .floor(Instant.now());
        long measured = evaluator.countMatching(draft, bucket);
        boolean matched = AlertEvaluator.fires(measured, draft.threshold(), draft.comparator());
        return explanation(draft, bucket, measured, matched, "draft");
    }

    public Map<String, Object> explain(long ruleId) {
        AlertRule rule = rules.findById(ruleId)
                .orElseThrow(() -> new IllegalArgumentException("Rule not found: " + ruleId));
        Instant bucket = ("1d".equals(rule.granularity()) ? Granularity.ONE_DAY : Granularity.FIVE_MIN)
                .floor(Instant.now());
        long measured = evaluator.countMatching(rule, bucket);
        return explanation(rule, bucket, measured,
                AlertEvaluator.fires(measured, rule.threshold(), rule.comparator()), "persisted");
    }

    private Map<String, Object> explanation(AlertRule rule, Instant bucket, long measured,
                                             boolean matched, String source) {
        return Map.ofEntries(
                Map.entry("ruleSource", source), Map.entry("ruleId", rule.ruleId()),
                Map.entry("ruleName", rule.name()), Map.entry("matched", matched),
                Map.entry("measured", measured), Map.entry("comparator", rule.comparator()),
                Map.entry("threshold", rule.threshold()), Map.entry("bucket", bucket),
                Map.entry("dimensions", Map.of("siteId", rule.siteId(), "eventType", rule.eventType(),
                        "geoLevel", rule.geoLevel(), "geoAreaId", rule.geoAreaId() == null ? "" : rule.geoAreaId(),
                        "granularity", rule.granularity())),
                Map.entry("reason", "Measured %d %s threshold %d: %s".formatted(
                        measured, rule.comparator(), rule.threshold(), matched ? "matched" : "not matched")));
    }
}
