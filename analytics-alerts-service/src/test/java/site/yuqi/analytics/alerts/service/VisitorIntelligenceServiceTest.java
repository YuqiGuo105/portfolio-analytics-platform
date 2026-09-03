package site.yuqi.analytics.alerts.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VisitorIntelligenceServiceTest {
    @Test
    void explainsWhyPersistedRuleMatched() {
        AlertRuleRepository repository = mock(AlertRuleRepository.class);
        AlertEvaluator evaluator = mock(AlertEvaluator.class);
        AlertRule rule = new AlertRule(7L, "portfolio", "Texas interest", "PAGE_VIEW",
                "REGION", "US-TX", "5m", 3, ">=", 1800, true, 2);
        when(repository.findById(7L)).thenReturn(Optional.of(rule));
        when(evaluator.countMatching(any(), any(Instant.class))).thenReturn(5L);
        VisitorIntelligenceService service = new VisitorIntelligenceService(
                mock(JdbcTemplate.class), repository, evaluator);

        var result = service.explain(7L);

        assertThat(result).containsEntry("matched", true).containsEntry("measured", 5L);
        assertThat(result.get("reason")).asString().contains("5 >= threshold 3");
    }
}
