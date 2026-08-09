package site.yuqi.analytics.aggregator.operations;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.common.event.EnrichedEvent;
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

    public void ingestionCompleted(EnrichedEvent event, Instant startedAt) {
        String eventId = UUID.randomUUID().toString();
        String correlationId = hasText(event.sessionId()) ? event.sessionId() : event.eventId();
        OperationEvent operation = new OperationEvent(
                eventId,
                "analytics.ingestion.completed",
                1,
                Instant.now(),
                environment,
                null,
                null,
                null,
                correlationId,
                event.eventId(),
                "analytics:" + event.eventId(),
                Map.of("type", "service", "id", "analytics-aggregator-service"),
                Map.of("type", "visitor_event", "id", event.eventId()),
                "analytics-aggregator-service",
                "completed",
                1,
                Math.max(0, Duration.between(startedAt, Instant.now()).toMillis()),
                Map.of(
                        "sessionId", value(event.sessionId()),
                        "eventType", value(event.eventType()),
                        "siteId", value(event.siteId()),
                        "geoLevel", event.geo() == null ? "unknown" : String.valueOf(event.geo().geoLevel())));
        publish(operation);
    }

    private void publish(OperationEvent event) {
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

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}
