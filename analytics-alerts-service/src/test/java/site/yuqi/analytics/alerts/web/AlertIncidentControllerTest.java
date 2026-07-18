package site.yuqi.analytics.alerts.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import site.yuqi.analytics.alerts.dto.AlertIncidentPage;
import site.yuqi.analytics.alerts.repo.AlertIncidentRepository;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AlertIncidentControllerTest {

    private AlertIncidentController controller;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        EmbeddedDatabase db = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .setName("incidents-controller-test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .addScript("classpath:test-schema.sql")
                .build();
        jdbc = new JdbcTemplate(db);
        controller = new AlertIncidentController(new AlertIncidentRepository(jdbc));
    }

    @Test
    void listsRecentIncidentsWithDeliverySummary() {
        long ruleId = insertRule();
        insertIncident(ruleId, "incident-1", true);
        insertIncident(ruleId, "incident-2", false);

        AlertIncidentPage result = controller.list(24, "yuqi.site", ruleId, null, 20);

        assertThat(result.items()).hasSize(2);
        assertThat(result.summary().total()).isEqualTo(2);
        assertThat(result.summary().notified()).isEqualTo(1);
        assertThat(result.summary().pendingNotification()).isEqualTo(1);
        assertThat(result.items().get(0).ruleName()).isEqualTo("Traffic spike");
    }

    @Test
    void filtersByNotificationState() {
        long ruleId = insertRule();
        insertIncident(ruleId, "incident-1", true);
        insertIncident(ruleId, "incident-2", false);

        AlertIncidentPage result = controller.list(24, null, null, false, 20);

        assertThat(result.items()).singleElement().satisfies(incident ->
                assertThat(incident.notified()).isFalse());
        assertThat(result.summary().pendingNotification()).isEqualTo(1);
    }

    private long insertRule() {
        jdbc.update("""
                insert into alert_rules
                    (site_id, name, event_type, geo_level, granularity, threshold,
                     comparator, cooldown_seconds, enabled, version)
                values ('yuqi.site', 'Traffic spike', 'page_view', 'GLOBAL', '5m',
                        20, '>=', 300, true, 1)
                """);
        return jdbc.queryForObject("select rule_id from alert_rules", Long.class);
    }

    private void insertIncident(long ruleId, String dedupKey, boolean notified) {
        Instant now = Instant.now();
        jdbc.update("""
                insert into incidents
                    (rule_id, site_id, bucket_time, granularity, measured_value,
                     threshold, comparator, dedup_key, notified, notified_at)
                values (?, 'yuqi.site', ?, '5m', 42, 20, '>=', ?, ?, ?)
                """,
                ruleId,
                Timestamp.from(now),
                dedupKey,
                notified,
                notified ? Timestamp.from(now) : null);
    }
}
