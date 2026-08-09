package site.yuqi.analytics.aggregator.operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.OperationEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class OperationEventPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;

    @Value("${analytics.topics.operations:platform.operation.events.v1}")
    private String topic;

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
        publish(operation, correlationId);
    }

    private void publish(OperationEvent event, String key) {
        try {
            kafka.send(topic, key, objectMapper.writeValueAsString(event))
                    .whenComplete((ignored, error) -> {
                        if (error != null) {
                            log.warn("operation_event_publish_failed type={} eventId={}",
                                    event.eventType(), event.eventId(), error);
                        }
                    });
        } catch (JsonProcessingException error) {
            log.warn("operation_event_serialize_failed type={} eventId={}",
                    event.eventType(), event.eventId(), error);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}
