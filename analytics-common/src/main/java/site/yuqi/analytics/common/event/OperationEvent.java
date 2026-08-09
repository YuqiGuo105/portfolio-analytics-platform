package site.yuqi.analytics.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/** Sanitized cross-service event used by the operations timeline projection. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OperationEvent(
        String eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String environment,
        String traceId,
        String spanId,
        String runId,
        String correlationId,
        String causationId,
        String idempotencyKey,
        Map<String, Object> actor,
        Map<String, Object> subject,
        String sourceService,
        String status,
        Integer attempt,
        Long durationMs,
        Map<String, Object> attributes) {
}
