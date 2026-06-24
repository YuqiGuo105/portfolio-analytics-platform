package site.yuqi.analytics.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.kafka.DlqProducer;
import site.yuqi.analytics.common.kafka.Outcome;

/**
 * Reads from {@code analytics.raw.events}, runs the in-process enrichment
 * pipeline, then UPSERTs into Postgres rollups. Same DONE / DLQ / RETRY
 * 3-state contract as the rest of the platform.
 *
 * <p>This single consumer replaced the previous two-stage
 * raw → enriched-topic → aggregator setup so that the platform fits inside
 * Aiven Kafka's 2-topic free tier (raw + dlq).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RawEventConsumer {

    private final EnrichmentPipeline pipeline;
    private final RollupUpsertService rollup;
    private final DlqProducer dlq;

    @KafkaListener(
            topics = "${analytics.topics.raw}",
            groupId = "${spring.kafka.consumer.group-id:portfolio-analytics-aggregator}",
            autoStartup = "${analytics.consumer-enabled:true}"
    )
    public void onMessage(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        Outcome outcome = process(rec.value(), rec.key(), rec.topic(), rec.partition(), rec.offset());
        switch (outcome) {
            case DONE, DLQ -> ack.acknowledge();
            case RETRY -> {
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    Outcome process(String value, String key, String topic, int partition, long offset) {
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        EnrichedEvent enriched;
        try {
            enriched = pipeline.parseAndEnrich(value, pr);
        } catch (RuntimeException unexpected) {
            log.error("{\"event\":\"pipeline_threw\",\"offset\":{},\"err\":\"{}\"}",
                    offset, unexpected.getMessage());
            return Outcome.RETRY;
        }

        if (enriched == null) {
            if (pr.duplicate) {
                // Already processed — ack and move on.
                return Outcome.DONE;
            }
            // Empty payload / parse error / missing required field → DLQ.
            dlq.publish(key, value == null ? "" : value,
                    "%s at %s-%d@%d".formatted(pr.parseError, topic, partition, offset));
            return Outcome.DLQ;
        }

        try {
            rollup.upsert(enriched);
            return Outcome.DONE;
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"rollup_upsert_failed\",\"eventId\":\"{}\",\"err\":\"{}\"}",
                    enriched.eventId(), dbErr.getMessage());
            return Outcome.RETRY;
        }
    }
}
