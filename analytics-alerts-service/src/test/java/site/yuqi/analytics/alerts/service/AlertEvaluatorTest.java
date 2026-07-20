package site.yuqi.analytics.alerts.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import site.yuqi.analytics.alerts.dto.AlertIncident;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.repo.AlertIncidentRepository;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AlertEvaluatorTest {

    private AlertRuleRepository repo;
    private JdbcTemplate jdbc;
    private NotificationSender sender;
    private AlertIncidentRepository incidents;
    private AlertEvaluator eval;

    @BeforeEach
    void setUp() {
        repo = mock(AlertRuleRepository.class);
        jdbc = mock(JdbcTemplate.class);
        sender = mock(NotificationSender.class);
        incidents = mock(AlertIncidentRepository.class);
        when(sender.send(anyMap())).thenReturn(true);
        when(incidents.existsWithinCooldown(any(), any())).thenReturn(false);
        eval = new AlertEvaluator(repo, jdbc, sender, incidents);
    }

    @Test
    void belowThresholdDoesNotInsertIncidentOrSend() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(50L);

        eval.evaluate(rule);

        verify(incidents, never()).insert(any(), any(), anyLong(), anyString());
        verify(sender, never()).send(anyMap());
    }

    @Test
    void atOrAboveThresholdInsertsIncidentAndNotifies() {
        AlertRule rule = sampleRule(100, ">=");
        AlertIncident incident = sampleIncident();
        stubCount(250L);
        when(incidents.insert(eq(rule), any(), eq(250L), anyString())).thenReturn(Optional.of(incident));

        eval.evaluate(rule);

        ArgumentCaptor<java.util.Map<String, Object>> payload = ArgumentCaptor.forClass(java.util.Map.class);
        verify(sender).send(payload.capture());
        assertThat(payload.getValue())
                .containsEntry("eventType", "ANALYTICS_ALERT_TRIGGERED")
                .containsEntry("topic", "ADMIN_ALERTS");
        verify(incidents).recordNotificationResult(incident.incidentId(), true);
    }

    @Test
    void duplicateIncidentInsertSkipsNotification() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(250L);
        when(incidents.insert(eq(rule), any(), eq(250L), anyString())).thenReturn(Optional.empty());

        eval.evaluate(rule);

        verify(sender, never()).send(anyMap());
    }

    @Test
    void failedNotificationRemainsPendingForRetry() {
        AlertRule rule = sampleRule(100, ">=");
        AlertIncident incident = sampleIncident();
        stubCount(250L);
        when(incidents.insert(eq(rule), any(), eq(250L), anyString())).thenReturn(Optional.of(incident));
        when(sender.send(anyMap())).thenReturn(false);

        eval.evaluate(rule);

        verify(sender).send(anyMap());
        verify(incidents).recordNotificationResult(incident.incidentId(), false);
    }

    @Test
    void activeCooldownSuppressesDuplicateIncident() {
        AlertRule rule = sampleRule(100, ">=");
        stubCount(250L);
        when(incidents.existsWithinCooldown(eq(rule), any())).thenReturn(true);

        eval.evaluate(rule);

        verify(incidents, never()).insert(any(), any(), anyLong(), anyString());
        verify(sender, never()).send(anyMap());
    }

    @Test
    void pendingNotificationIsRetriedAndRecorded() {
        AlertIncident incident = sampleIncident();
        when(incidents.findPendingNotifications(any(), anyInt())).thenReturn(List.of(incident));

        eval.retryPendingNotifications();

        verify(sender).send(anyMap());
        verify(incidents).recordNotificationResult(incident.incidentId(), true);
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

    private static AlertRule sampleRule(long threshold, String comparator) {
        return new AlertRule(
                42L, "yuqi.site", "spike", "page_view", "GLOBAL", null,
                "5m", threshold, comparator, 60, true, 1);
    }

    private static AlertIncident sampleIncident() {
        Instant now = Instant.now();
        return new AlertIncident(
                9L, 42L, "spike", "yuqi.site", null, now, "5m",
                250L, 100L, ">=", false, null, 0, null, now);
    }
}
