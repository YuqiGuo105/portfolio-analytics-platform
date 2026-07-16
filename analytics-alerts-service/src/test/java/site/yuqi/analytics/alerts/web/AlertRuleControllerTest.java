package site.yuqi.analytics.alerts.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.http.ResponseEntity;
import site.yuqi.analytics.alerts.dto.*;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.alerts.service.AlertRuleChangeService;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AlertRuleControllerTest {

    private AlertRuleController controller;
    private AlertRuleRepository repo;

    @BeforeEach
    void setUp() {
        EmbeddedDatabase db = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .setName("alerts-ctrl-test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
                .addScript("classpath:test-schema.sql")
                .build();
        JdbcTemplate jdbc = new JdbcTemplate(db);
        repo = new AlertRuleRepository(jdbc, db);
        AlertRuleChangeService changeService = new AlertRuleChangeService(repo, jdbc, new ObjectMapper());
        controller = new AlertRuleController(repo, changeService);
    }

    @Test
    void listWithFilters() {
        repo.insert(new AlertRuleRequest("s", "alpha", "page_view", "GLOBAL", null, "5m", 10, ">=", 60));
        repo.insert(new AlertRuleRequest("s", "beta", "click", "GLOBAL", null, "5m", 20, ">=", 60));

        List<AlertRule> all = controller.list(null, null, null);
        assertThat(all).hasSize(2);

        List<AlertRule> byName = controller.list("alpha", null, null);
        assertThat(byName).hasSize(1).first().extracting(AlertRule::name).isEqualTo("alpha");

        List<AlertRule> byType = controller.list(null, "click", null);
        assertThat(byType).hasSize(1).first().extracting(AlertRule::eventType).isEqualTo("click");
    }

    @Test
    void getExistingAndMissing() {
        AlertRule created = repo.insert(new AlertRuleRequest("s", "r", "page_view", "GLOBAL", null, "5m", 1, ">=", 60));
        assertThat(controller.get(created.ruleId()).getStatusCode().value()).isEqualTo(200);
        assertThat(controller.get(9999).getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void prepareAndApplyViaController() {
        repo.insert(new AlertRuleRequest("s", "rule1", "page_view", "GLOBAL", null, "5m", 100, ">=", 1800));
        AlertRule rule = repo.findAll().get(0);

        ResponseEntity<?> prepResp = controller.prepareChange(new PrepareChangeRequest(
                "SET_ENABLED", rule.ruleId(),
                new AlertRulePatch(null, null, null, null, null, null, null, null, false),
                "disable via controller", "test"));
        assertThat(prepResp.getStatusCode().value()).isEqualTo(200);

        PreparedChange prepared = (PreparedChange) prepResp.getBody();
        ResponseEntity<?> applyResp = controller.applyChange(
                new ApplyChangeRequest(prepared.changeId(), "ctrl-idem-1"));
        assertThat(applyResp.getStatusCode().value()).isEqualTo(200);

        AlertRule updated = repo.findById(rule.ruleId()).orElseThrow();
        assertThat(updated.enabled()).isFalse();
    }

    @Test
    void prepareInvalidActionReturns400() {
        ResponseEntity<?> resp = controller.prepareChange(new PrepareChangeRequest(
                "INVALID", null, new AlertRulePatch(null, null, null, null, null, null, null, null, null),
                "bad", "test"));
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
    }
}
