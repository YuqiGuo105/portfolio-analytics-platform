package site.yuqi.analytics.alerts.repo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.dto.AlertRuleRequest;

import static org.assertj.core.api.Assertions.assertThat;

class AlertRuleRepositoryTest {

    private EmbeddedDatabase db;
    private AlertRuleRepository repo;

    @BeforeEach
    void setUp() {
        db = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .setName("alerts-repo-test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .addScript("classpath:test-schema.sql")
                .build();
        repo = new AlertRuleRepository(new JdbcTemplate(db), db);
    }

    @Test
    void insertFindUpdateDisableDeleteRoundTrip() {
        AlertRule created = repo.insert(new AlertRuleRequest(
                "yuqi.site", "spike", "page_view", "GLOBAL", null,
                "5m", 100, ">=", 1800));

        assertThat(created.ruleId()).isPositive();
        assertThat(created.enabled()).isTrue();
        assertThat(repo.findAll()).hasSize(1);
        assertThat(repo.findEnabled()).hasSize(1);

        AlertRule updated = repo.update(created.ruleId(),
                new AlertRuleRequest("yuqi.site", "spike-v2", "page_view", "COUNTRY", "COUNTRY:US",
                        "5m", 200, ">=", 1800)).orElseThrow();
        assertThat(updated.name()).isEqualTo("spike-v2");
        assertThat(updated.threshold()).isEqualTo(200);
        assertThat(updated.geoAreaId()).isEqualTo("COUNTRY:US");

        assertThat(repo.setEnabled(created.ruleId(), false)).isTrue();
        assertThat(repo.findEnabled()).isEmpty();
        assertThat(repo.findAll()).hasSize(1);

        assertThat(repo.delete(created.ruleId())).isTrue();
        assertThat(repo.findAll()).isEmpty();
    }

    @Test
    void notFoundReturnsEmpty() {
        assertThat(repo.findById(999L)).isEmpty();
        assertThat(repo.update(999L, new AlertRuleRequest(
                "s", "n", "page_view", "GLOBAL", null, "5m", 1, ">=", 60))).isEmpty();
        assertThat(repo.setEnabled(999L, false)).isFalse();
        assertThat(repo.delete(999L)).isFalse();
    }
}
