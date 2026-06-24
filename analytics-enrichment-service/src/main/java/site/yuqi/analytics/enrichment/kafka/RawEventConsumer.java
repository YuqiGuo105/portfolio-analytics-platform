package site.yuqi.analytics.enrichment.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.common.kafka.Outcome;
import site.yuqi.analytics.enrichment.service.EnrichmentProcessor;

/**
 * Kafka entry point. The DONE/DLQ/RETRY switch and 2-second cool-down on
 * RETRY are copied verbatim from {@code ContentEventConsumer} in
 * {@code portfolio-notification-service} — same Kafka container config
 * (manual ack, {@code missing-topics-fatal:false}), same operational
 * playbook.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RawEventConsumer {

    private final EnrichmentProcessor processor;

    @KafkaListener(
            topics = "${analytics.topics.raw}",
            groupId = "${spring.kafka.consumer.group-id:portfolio-analytics-enrichment}",
            autoStartup = "${analytics.consumer-enabled:true}"
    )
    public void onMessage(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        String value = rec.value();
        String topic = rec.topic();
        int partition = rec.partition();
        long offset = rec.offset();

        log.info("{\"event\":\"consume_raw\",\"topic\":\"{}\",\"partition\":{},\"offset\":{}}",
                topic, partition, offset);

        Outcome outcome;
        try {
            outcome = processor.process(value, topic, partition, String.valueOf(offset));
        } catch (Exception unexpected) {
            log.error("{\"event\":\"processor_threw\",\"offset\":{},\"err\":\"{}\"}",
                    offset, unexpected.getMessage());
            outcome = Outcome.RETRY;
        }

        switch (outcome) {
            case DONE -> ack.acknowledge();
            case DLQ -> {
                // Processor has already published to DLQ with full context;
                // we just ack the source offset so the bad record stops blocking.
                ack.acknowledge();
            }
            case RETRY -> {
                // Do not ack — container will redeliver. Sleep briefly to
                // avoid a tight loop during sustained downstream outage.
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
