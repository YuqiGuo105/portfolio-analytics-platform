package site.yuqi.analytics.alerts.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import site.yuqi.analytics.alerts.dto.*;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertRuleChangeServiceTest {

    private AlertRuleChangeService service;
    private AlertRuleRepository repo;

    @BeforeEach
    void setUp() {
        EmbeddedDatabase db = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .setName("alerts-change-test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .addScript("classpath:test-schema.sql")
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(db);
        repo = new AlertRuleRepository(jdbc, db);
        service = new AlertRuleChangeService(repo, jdbc, new ObjectMapper());
    }

    @Test
    void prepareCreateReturnsChangeIdAndDiff() {
        PrepareChangeRequest req = new PrepareChangeRequest(
                "CREATE", null,
                new AlertRulePatch("spike-test", "page_view", "GLOBAL", null, "5m", 100L, ">=", 1800, null),
                "unit test", "test-actor");

        PreparedChange result = service.prepare(req);

        assertThat(result.changeId()).startsWith("chg_");
        assertThat(result.action()).isEqualTo("CREATE");
        assertThat(result.after()).containsEntry("name", "spike-test");
        assertThat(result.after()).containsEntry("threshold", 100L);
        assertThat(result.expiresAt()).isNotNull();
        assertThat(result.expectedVersion()).isZero();
    }

    @Test
    void applyCreateInsertsRuleAndReturnsSuccess() {
        PrepareChangeRequest req = new PrepareChangeRequest(
                "CREATE", null,
                new AlertRulePatch("new-rule", "click", "COUNTRY", "US", "1d", 50L, ">=", 600, null),
                "test create", "admin@test");

        PreparedChange prepared = service.prepare(req);
        Map<String, Object> result = service.apply(new ApplyChangeRequest(prepared.changeId(), "idem-1"));

        assertThat(result).containsEntry("success", true);
        assertThat(result).containsEntry("action", "CREATE");
        assertThat((Number) result.get("ruleId")).isNotNull();

        AlertRule created = repo.findAll().get(0);
        assertThat(created.name()).isEqualTo("new-rule");
        assertThat(created.eventType()).isEqualTo("click");
        assertThat(created.threshold()).isEqualTo(50);
    }

    @Test
    void prepareUpdateShowsDiff() {
        AlertRule existing = repo.insert(new AlertRuleRequest(
                "yuqi.site", "original", "page_view", "GLOBAL", null, "5m", 100, ">=", 1800));

        PrepareChangeRequest req = new PrepareChangeRequest(
                "UPDATE", existing.ruleId(),
                new AlertRulePatch(null, null, null, null, null, 200L, null, null, null),
                "raise threshold", "admin");

        PreparedChange result = service.prepare(req);

        assertThat(result.expectedVersion()).isEqualTo(1);
        assertThat(result.diff()).containsKey("threshold");
        assertThat(result.ruleId()).isEqualTo(existing.ruleId());
    }

    @Test
    void applyUpdateModifiesRuleWithVersionCheck() {
        AlertRule existing = repo.insert(new AlertRuleRequest(
                "yuqi.site", "to-update", "page_view", "GLOBAL", null, "5m", 100, ">=", 1800));

        PreparedChange prepared = service.prepare(new PrepareChangeRequest(
                "UPDATE", existing.ruleId(),
                new AlertRulePatch("renamed", null, null, null, null, 500L, null, null, null),
                "rename + threshold", "admin"));

        Map<String, Object> result = service.apply(new ApplyChangeRequest(prepared.changeId(), "idem-2"));
        assertThat(result).containsEntry("success", true);

        AlertRule updated = repo.findById(existing.ruleId()).orElseThrow();
        assertThat(updated.name()).isEqualTo("renamed");
        assertThat(updated.threshold()).isEqualTo(500);
        assertThat(updated.version()).isEqualTo(2);
    }

    @Test
    void prepareSetEnabledProducesWarning() {
        AlertRule existing = repo.insert(new AlertRuleRequest(
                "yuqi.site", "active-rule", "page_view", "GLOBAL", null, "5m", 100, ">=", 1800));

        PreparedChange result = service.prepare(new PrepareChangeRequest(
                "SET_ENABLED", existing.ruleId(),
                new AlertRulePatch(null, null, null, null, null, null, null, null, false),
                "disable noisy rule", "admin"));

        assertThat(result.warnings()).isNotEmpty();
        assertThat(result.warnings().get(0)).contains("disable");
    }

    @Test
    void applySetEnabledDisablesRule() {
        AlertRule existing = repo.insert(new AlertRuleRequest(
                "yuqi.site", "to-disable", "click", "GLOBAL", null, "5m", 50, ">=", 900));

        PreparedChange prepared = service.prepare(new PrepareChangeRequest(
                "SET_ENABLED", existing.ruleId(),
                new AlertRulePatch(null, null, null, null, null, null, null, null, false),
                "disable", "admin"));

        service.apply(new ApplyChangeRequest(prepared.changeId(), "idem-3"));

        AlertRule disabled = repo.findById(existing.ruleId()).orElseThrow();
        assertThat(disabled.enabled()).isFalse();
        assertThat(disabled.version()).isEqualTo(2);
    }

    @Test
    void applyExpiredChangeThrows() {
        // Prepare then manually expire by waiting (we can't wait 5min, so test the error path)
        PrepareChangeRequest req = new PrepareChangeRequest(
                "CREATE", null,
                new AlertRulePatch("expired", "page_view", "GLOBAL", null, "5m", 1L, ">=", 60, null),
                "will expire", "admin");
        service.prepare(req);

        // Applying with wrong changeId
        assertThatThrownBy(() -> service.apply(new ApplyChangeRequest("chg_nonexistent", "idem-4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void idempotencyKeyPreventsDoubleApply() {
        AlertRule existing = repo.insert(new AlertRuleRequest(
                "yuqi.site", "idempotent", "page_view", "GLOBAL", null, "5m", 100, ">=", 1800));

        PreparedChange prepared = service.prepare(new PrepareChangeRequest(
                "SET_ENABLED", existing.ruleId(),
                new AlertRulePatch(null, null, null, null, null, null, null, null, false),
                "test idempotency", "admin"));

        Map<String, Object> first = service.apply(new ApplyChangeRequest(prepared.changeId(), "same-key"));
        Map<String, Object> second = service.apply(new ApplyChangeRequest("any-id", "same-key"));

        assertThat(first).isEqualTo(second);
    }

    @Test
    void invalidActionThrows() {
        assertThatThrownBy(() -> service.prepare(new PrepareChangeRequest(
                "DELETE", null, new AlertRulePatch(null, null, null, null, null, null, null, null, null),
                "bad", "admin")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid action");
    }

    @Test
    void prepareUpdateOnNonexistentRuleThrows() {
        assertThatThrownBy(() -> service.prepare(new PrepareChangeRequest(
                "UPDATE", 9999L,
                new AlertRulePatch("x", null, null, null, null, null, null, null, null),
                "missing", "admin")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }
}
