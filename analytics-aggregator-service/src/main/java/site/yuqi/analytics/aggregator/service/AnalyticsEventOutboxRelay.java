package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Relays database-backed visitor events to Kafka with at-least-once delivery.
 * A crash after broker acknowledgement but before {@code markSent} produces a
 * duplicate by design; the transactional Kafka inbox consumes it exactly once.
 */
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "analytics.outbox.enabled", havingValue = "true", matchIfMissing = true)
@Slf4j
public class AnalyticsEventOutboxRelay {

    private final AnalyticsEventOutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;

    @Value("${analytics.topics.raw}")
    private String rawTopic;

    @Value("${analytics.outbox.batch-size:50}")
    private int batchSize;

    @Value("${analytics.outbox.lease-seconds:60}")
    private int leaseSeconds;

    @Value("${analytics.outbox.send-timeout-seconds:15}")
    private int sendTimeoutSeconds;

    @Scheduled(
            fixedDelayString = "${analytics.outbox.relay-interval-ms:5000}",
            initialDelayString = "${analytics.outbox.initial-delay-ms:15000}")
    public void relay() {
        drainOnce();
    }

    public int drainOnce() {
        var events = repository.claimReady(batchSize, leaseSeconds);
        if (!events.isEmpty()) {
            log.info("{\"event\":\"analytics_outbox_claimed\",\"count\":{}}", events.size());
        }

        for (var event : events) {
            try {
                kafka.send(rawTopic, event.partitionKey(), event.payload())
                        .get(sendTimeoutSeconds, TimeUnit.SECONDS);
                repository.markSent(event.id());
                log.info("{\"event\":\"analytics_outbox_sent\",\"event_id\":\"{}\",\"attempt\":{}}",
                        event.eventId(), event.attemptCount());
            } catch (Exception failure) {
                String message = rootMessage(failure);
                repository.markFailed(event.id(), message);
                log.warn("{\"event\":\"analytics_outbox_retry\",\"event_id\":\"{}\",\"attempt\":{},\"error\":\"{}\"}",
                        event.eventId(), event.attemptCount(), sanitize(message));
            }
        }
        return events.size();
    }

    private static String rootMessage(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    private static String sanitize(String value) {
        return value == null ? "unknown" : value.replace('"', '\'').replace('\n', ' ');
    }
}
