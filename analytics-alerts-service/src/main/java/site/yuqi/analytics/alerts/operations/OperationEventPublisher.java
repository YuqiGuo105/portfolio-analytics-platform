package site.yuqi.analytics.alerts.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.alerts.dto.AlertIncident;
import site.yuqi.analytics.common.event.OperationEvent;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class OperationEventPublisher {
    private static final HttpClient HTTP = HttpClient.newBuilder().build();

    private final ObjectMapper objectMapper;

    @Value("${analytics.operations.ingest-url:}")
    private String ingestUrl;
    @Value("${analytics.operations.internal-token:}")
    private String internalToken;
    @Value("${analytics.operations.timeout-ms:750}")
    private long timeoutMs;

    @Value("${spring.profiles.active:default}")
    private String environment;

    public String publish(AlertIncident incident, String eventType, String status, int attempt) {
        String eventId = UUID.randomUUID().toString();
        String correlationId = "visitor-alert:" + incident.incidentId();
        OperationEvent event = new OperationEvent(
                eventId,
                eventType,
                1,
                Instant.now(),
                environment,
                null,
                null,
                null,
                correlationId,
                "incident:" + incident.incidentId(),
                correlationId + ":" + eventType + ":" + attempt,
                Map.of("type", "service", "id", "analytics-alerts-service"),
                Map.of("type", "alert_incident", "id", String.valueOf(incident.incidentId())),
                "analytics-alerts-service",
                status,
                attempt,
                null,
                Map.of(
                        "ruleId", incident.ruleId(),
                        "siteId", incident.siteId(),
                        "granularity", incident.granularity()));
        send(event);
        return eventId;
    }

    private void send(OperationEvent event) {
        if (ingestUrl == null || ingestUrl.isBlank() || internalToken == null || internalToken.isBlank()) return;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(ingestUrl))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("Content-Type", "application/json")
                    .header("X-Internal-Token", internalToken)
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(event)))
                    .build();
            HTTP.sendAsync(request, HttpResponse.BodyHandlers.discarding()).whenComplete((response, error) -> {
                if (error != null || response.statusCode() >= 300) {
                    log.warn("operation_event_ingest_failed type={} status={}", event.eventType(),
                            error == null ? response.statusCode() : error.getClass().getSimpleName());
                }
            });
        } catch (Exception error) {
            log.warn("operation_event_ingest_failed type={} eventId={}", event.eventType(), event.eventId(), error);
        }
    }
}
