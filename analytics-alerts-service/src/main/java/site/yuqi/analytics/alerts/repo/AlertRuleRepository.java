package site.yuqi.analytics.alerts.repo;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.dto.AlertRuleRequest;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class AlertRuleRepository {

    private final JdbcTemplate jdbc;
    private final DataSource dataSource;

    private static final RowMapper<AlertRule> MAPPER = (rs, n) -> new AlertRule(
            rs.getLong("rule_id"),
            rs.getString("site_id"),
            rs.getString("name"),
            rs.getString("event_type"),
            rs.getString("geo_level"),
            rs.getString("geo_area_id"),
            rs.getString("granularity"),
            rs.getLong("threshold"),
            rs.getString("comparator"),
            rs.getInt("cooldown_seconds"),
            rs.getBoolean("enabled"),
            rs.getInt("version"));

    public List<AlertRule> findAll() {
        return jdbc.query("select * from alert_rules order by rule_id", MAPPER);
    }

    public List<AlertRule> findEnabled() {
        return jdbc.query("select * from alert_rules where enabled = true order by rule_id", MAPPER);
    }

    public Optional<AlertRule> findById(long id) {
        return jdbc.query("select * from alert_rules where rule_id = ?", MAPPER, id).stream().findFirst();
    }

    public AlertRule insert(AlertRuleRequest r) {
        SimpleJdbcInsert ins = new SimpleJdbcInsert(dataSource)
                .withTableName("alert_rules")
                .usingColumns("site_id", "name", "event_type", "geo_level", "geo_area_id",
                        "granularity", "threshold", "comparator", "cooldown_seconds", "enabled")
                .usingGeneratedKeyColumns("rule_id");
        Number id = ins.executeAndReturnKey(Map.ofEntries(
                Map.entry("site_id", r.siteId()),
                Map.entry("name", r.name()),
                Map.entry("event_type", r.eventType()),
                Map.entry("geo_level", r.geoLevel()),
                Map.entry("geo_area_id", r.geoAreaId() == null ? "" : r.geoAreaId()),
                Map.entry("granularity", r.granularity()),
                Map.entry("threshold", r.threshold()),
                Map.entry("comparator", r.comparator()),
                Map.entry("cooldown_seconds", r.cooldownSeconds()),
                Map.entry("enabled", true)
        ));
        return findById(id.longValue()).orElseThrow();
    }

    public Optional<AlertRule> update(long id, AlertRuleRequest r) {
        int rows = jdbc.update("""
                update alert_rules set
                    site_id = ?, name = ?, event_type = ?, geo_level = ?, geo_area_id = ?,
                    granularity = ?, threshold = ?, comparator = ?, cooldown_seconds = ?,
                    version = version + 1, updated_at = now()
                where rule_id = ?
                """,
                r.siteId(), r.name(), r.eventType(), r.geoLevel(),
                r.geoAreaId() == null ? "" : r.geoAreaId(),
                r.granularity(), r.threshold(), r.comparator(), r.cooldownSeconds(),
                id);
        return rows == 0 ? Optional.empty() : findById(id);
    }

    /**
     * Optimistic-locking update: only succeeds if current version matches expectedVersion.
     */
    public Optional<AlertRule> updateWithVersion(long id, AlertRuleRequest r, int expectedVersion) {
        int rows = jdbc.update("""
                update alert_rules set
                    site_id = ?, name = ?, event_type = ?, geo_level = ?, geo_area_id = ?,
                    granularity = ?, threshold = ?, comparator = ?, cooldown_seconds = ?,
                    version = version + 1, updated_at = now()
                where rule_id = ? and version = ?
                """,
                r.siteId(), r.name(), r.eventType(), r.geoLevel(),
                r.geoAreaId() == null ? "" : r.geoAreaId(),
                r.granularity(), r.threshold(), r.comparator(), r.cooldownSeconds(),
                id, expectedVersion);
        if (rows == 0) return Optional.empty();
        return findById(id);
    }

    public boolean setEnabled(long id, boolean enabled) {
        return jdbc.update(
                "update alert_rules set enabled = ?, version = version + 1, updated_at = now() where rule_id = ?",
                enabled, id) > 0;
    }

    /**
     * Optimistic-locking setEnabled.
     */
    public boolean setEnabledWithVersion(long id, boolean enabled, int expectedVersion) {
        return jdbc.update(
                "update alert_rules set enabled = ?, version = version + 1, updated_at = now() where rule_id = ? and version = ?",
                enabled, id, expectedVersion) > 0;
    }

    public boolean delete(long id) {
        return jdbc.update("delete from alert_rules where rule_id = ?", id) > 0;
    }
}
