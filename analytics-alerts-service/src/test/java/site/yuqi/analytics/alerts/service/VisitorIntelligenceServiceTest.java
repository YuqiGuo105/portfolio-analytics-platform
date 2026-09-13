package site.yuqi.analytics.alerts.service;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import java.time.Instant;
import java.util.Optional;
import java.sql.Timestamp;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockingDetails;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VisitorIntelligenceServiceTest {
    @Test void previewUsesCanonicalEventNameAndOneGranularityInsteadOfDoubleCounting() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        VisitorIntelligenceService service = new VisitorIntelligenceService(jdbc,
                mock(AlertRuleRepository.class), mock(AlertEvaluator.class));
        service.segmentPreview("yuqi.site", "PAGE_VIEW", "REGION", "REGION:US:TX", 168, 20);
        var args = mockingDetails(jdbc).getInvocations().iterator().next().getRawArguments();
        assertThat((String) args[0]).contains("granularity = '5m'", "bucket_time < ?", "includes_bot_traffic");
        Object[] parameters = (Object[]) args[1];
        assertThat(parameters[1]).isEqualTo("page_view");
        assertThat(Duration.between(((Timestamp) parameters[3]).toInstant(), ((Timestamp) parameters[4]).toInstant()))
                .isEqualTo(Duration.ofHours(168));
        assertThat(parameters[parameters.length - 1]).isEqualTo(20);
        assertThatThrownBy(() -> service.segmentPreview("yuqi.site", null, "REGION", null, 2161, 20))
                .isInstanceOf(IllegalArgumentException.class);
    }

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
