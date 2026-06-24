package site.yuqi.analytics.common.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Best-effort DLQ publisher, modelled directly on
 * {@code portfolio-notification-service}'s {@code DlqProducer}.
 *
 * <p>Contract: <b>never throw</b>. If publishing fails we log loudly but
 * still allow the caller to ack the source offset, because the alternative
 * (infinite redelivery of a poison pill) is strictly worse than losing one
 * malformed record while we investigate.
 *
 * <p>Default DLQ topic is {@link Topics#DLQ}; callers may pass a different
 * topic to the per-call ctor for testing.
 */
public class DlqProducer {

    private static final Logger log = LoggerFactory.getLogger(DlqProducer.class);

    private final KafkaTemplate<String, String> kafka;
    private final String dlqTopic;

    public DlqProducer(KafkaTemplate<String, String> kafka) {
        this(kafka, Topics.DLQ);
    }

    public DlqProducer(KafkaTemplate<String, String> kafka, String dlqTopic) {
        this.kafka = kafka;
        this.dlqTopic = dlqTopic;
    }

    /**
     * Publish a single bad record to the DLQ with the provided reason
     * recorded in the structured log line.
     *
     * @param key     Original Kafka key (may be null).
     * @param payload Original Kafka value (must not be null; pass {@code ""} if unknown).
     * @param reason  Short human-readable reason — appears in monitoring.
     */
    public void publish(String key, String payload, String reason) {
        try {
            kafka.send(dlqTopic, key, payload == null ? "" : payload)
                    .get(5, TimeUnit.SECONDS);
            log.warn("{\"event\":\"dlq_published\",\"topic\":\"{}\",\"key\":\"{}\",\"reason\":\"{}\"}",
                    dlqTopic, key, reason);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.error("{\"event\":\"dlq_interrupted\",\"reason\":\"{}\"}", reason);
        } catch (ExecutionException | TimeoutException e) {
            log.error("{\"event\":\"dlq_publish_failed\",\"reason\":\"{}\",\"err\":\"{}\"}",
                    reason, e.getMessage());
        }
    }

    public String dlqTopic() {
        return dlqTopic;
    }
}
