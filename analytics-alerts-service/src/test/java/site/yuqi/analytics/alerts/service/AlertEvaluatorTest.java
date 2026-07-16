package site.yuqi.analytics.alerts.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlertEvaluatorTest {

    private AlertRuleRepository repo;
    private JdbcTemplate jdbc;
    private NotificationSender sender;
    private AlertEvaluator eval;

    @BeforeEach
    void setUp() {
        repo = mock(AlertRuleRepository.class);
        jdbc = mock(JdbcTemplate.class);
        sender = mock(NotificationSender.class);
        when(sender.send(anyMap())).thenReturn(true);
        eval = new AlertEvaluator(repo, jdbc, sender);
    }

    @Test
    void belowThresholdDoesNotInsertIncidentOrSend() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(50L);

        eval.evaluate(rule);

        verify(jdbc, never()).update(anyString(), any(Object[].class));
        verify(sender, never()).send(anyMap());
    }

    @Test
    void atOrAboveThresholdInsertsIncidentAndNotifies() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(250L);
        // First update is the incident insert (returns 1 = newly inserted).
        // Second update is the notified=true mark.
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1, 1);

        eval.evaluate(rule);

        verify(jdbc, times(2)).update(anyString(), any(Object[].class));
        verify(sender, times(1)).send(anyMap());
    }

    @Test
    void duplicateIncidentInsertSkipsNotification() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(250L);
        // dedup_key collision → insert returns 0.
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(0);

        eval.evaluate(rule);

        verify(jdbc, times(1)).update(anyString(), any(Object[].class));
        verify(sender, never()).send(anyMap());
    }

    @Test
    void failedNotificationLeavesIncidentRowUnmarked() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(250L);
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        when(sender.send(anyMap())).thenReturn(false);

        eval.evaluate(rule);

        // Only the insert ran — no mark-notified update.
        verify(jdbc, times(1)).update(anyString(), any(Object[].class));
        verify(sender, times(1)).send(anyMap());
    }

    @Test
    void firesHelperRespectsComparator() {
        assertThat(AlertEvaluator.fires(10, 10, ">=")).isTrue();
        assertThat(AlertEvaluator.fires(9, 10, ">=")).isFalse();
        assertThat(AlertEvaluator.fires(5, 10, "<=")).isTrue();
        assertThat(AlertEvaluator.fires(11, 10, "<=")).isFalse();
    }

    private void stubCount(long value) {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(value);
    }

    private static AlertRule sampleRule(long threshold, String cmp) {
        return new AlertRule(
                42L, "yuqi.site", "spike", "page_view", "GLOBAL", null,
                "5m", threshold, cmp, 60, true, 1);
    }
}
