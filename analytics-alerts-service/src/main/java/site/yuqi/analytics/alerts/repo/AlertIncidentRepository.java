package site.yuqi.analytics.alerts.repo;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import site.yuqi.analytics.alerts.dto.AlertIncident;
import site.yuqi.analytics.alerts.dto.AlertIncidentPage;
import site.yuqi.analytics.alerts.dto.AlertIncidentSummary;
import site.yuqi.analytics.alerts.dto.AlertRule;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class AlertIncidentRepository {

    private final JdbcTemplate jdbc;

    private static final String SELECT = """
            select i.incident_id, i.rule_id, r.name as rule_name, i.site_id,
                   i.geo_area_id, i.bucket_time, i.granularity, i.measured_value,
                   i.threshold, i.comparator, i.notified, i.notified_at,
                   i.notification_attempts, i.last_notification_attempt_at, i.created_at
            from incidents i
            join alert_rules r on r.rule_id = i.rule_id
            """;

    private static final RowMapper<AlertIncident> MAPPER = (rs, rowNum) -> new AlertIncident(
            rs.getLong("incident_id"),
            rs.getLong("rule_id"),
            rs.getString("rule_name"),
            rs.getString("site_id"),
            rs.getString("geo_area_id"),
            rs.getTimestamp("bucket_time").toInstant(),
            rs.getString("granularity"),
            rs.getLong("measured_value"),
            rs.getLong("threshold"),
            rs.getString("comparator"),
            rs.getBoolean("notified"),
            timestamp(rs.getTimestamp("notified_at")),
            rs.getInt("notification_attempts"),
            timestamp(rs.getTimestamp("last_notification_attempt_at")),
            rs.getTimestamp("created_at").toInstant());

    public boolean existsWithinCooldown(AlertRule rule, Instant since) {
        Boolean found = jdbc.queryForObject("""
                select exists(
                    select 1 from incidents
                    where rule_id = ?
                      and coalesce(geo_area_id, '') = ?
                      and created_at >= ?
                )
                """, Boolean.class, rule.ruleId(), area(rule.geoAreaId()), Timestamp.from(since));
        return Boolean.TRUE.equals(found);
    }

    public Optional<AlertIncident> insert(AlertRule rule, Instant bucket, long measuredValue, String dedupKey) {
        int inserted = jdbc.update("""
                insert into incidents
                    (rule_id, site_id, geo_area_id, bucket_time, granularity,
                     measured_value, threshold, comparator, dedup_key)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (dedup_key) do nothing
                """,
                rule.ruleId(), rule.siteId(), rule.geoAreaId(), Timestamp.from(bucket),
                rule.granularity(), measuredValue, rule.threshold(), rule.comparator(), dedupKey);
        return inserted == 0 ? Optional.empty() : findByDedupKey(dedupKey);
    }

    public Optional<AlertIncident> findByDedupKey(String dedupKey) {
        return jdbc.query(SELECT + " where i.dedup_key = ?", MAPPER, dedupKey).stream().findFirst();
    }

    public List<AlertIncident> findPendingNotifications(Instant retryBefore, int limit) {
        return jdbc.query(SELECT + """
                where i.notified = false
                  and (i.last_notification_attempt_at is null or i.last_notification_attempt_at <= ?)
                order by i.created_at
                limit ?
                """, MAPPER, Timestamp.from(retryBefore), limit);
    }

    public void recordNotificationResult(long incidentId, boolean notified) {
        jdbc.update("""
                update incidents
                set notification_attempts = notification_attempts + 1,
                    last_notification_attempt_at = now(),
                    notified = case when ? then true else notified end,
                    notified_at = case when ? then now() else notified_at end
                where incident_id = ?
                """, notified, notified, incidentId);
    }

    public AlertIncidentPage findRecent(
            Instant since, String siteId, Long ruleId, Boolean notified, int limit) {
        QueryParts filters = filters(since, siteId, ruleId, notified);
        List<Object> itemArgs = new ArrayList<>(filters.args());
        itemArgs.add(limit);
        List<AlertIncident> items = jdbc.query(
                SELECT + filters.where() + " order by i.created_at desc limit ?",
                MAPPER,
                itemArgs.toArray());

        AlertIncidentSummary summary = jdbc.queryForObject("""
                        select count(*) as total,
                               count(*) filter (where i.notified = true) as notified,
                               count(*) filter (where i.notified = false) as pending_notification
                        from incidents i
                        """ + filters.where(),
                (rs, rowNum) -> new AlertIncidentSummary(
                        rs.getLong("total"),
                        rs.getLong("notified"),
                        rs.getLong("pending_notification")),
                filters.args().toArray());
        return new AlertIncidentPage(items, summary);
    }

    private QueryParts filters(Instant since, String siteId, Long ruleId, Boolean notified) {
        StringBuilder where = new StringBuilder(" where i.created_at >= ?");
        List<Object> args = new ArrayList<>();
        args.add(Timestamp.from(since));
        if (siteId != null && !siteId.isBlank()) {
            where.append(" and i.site_id = ?");
            args.add(siteId.trim());
        }
        if (ruleId != null) {
            where.append(" and i.rule_id = ?");
            args.add(ruleId);
        }
        if (notified != null) {
            where.append(" and i.notified = ?");
            args.add(notified);
        }
        return new QueryParts(where.toString(), args);
    }

    private static Instant timestamp(Timestamp value) {
        return value == null ? null : value.toInstant();
    }

    private static String area(String value) {
        return value == null ? "" : value;
    }

    private record QueryParts(String where, List<Object> args) {}
}
