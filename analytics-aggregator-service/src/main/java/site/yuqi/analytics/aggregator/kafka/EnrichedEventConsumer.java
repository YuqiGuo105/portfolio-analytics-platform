package site.yuqi.analytics.aggregator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.kafka.Outcome;

/**
 * Consumes {@code analytics.enriched.events} and folds each record into
 * the Postgres rollup table at both 5m and 1d granularity.
 *
 * <p>Same DONE/DLQ/RETRY pattern as the enrichment service — but here
 * "DLQ" means we log + ack (we don't republish, because the bad record is
 * already past the upstream DLQ; republishing here would create a loop).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class EnrichedEventConsumer {

    private final ObjectMapper mapper;
    private final RollupUpsertService rollupUpsertService;

    @KafkaListener(
            topics = "${analytics.topics.enriched}",
            groupId = "${spring.kafka.consumer.group-id:portfolio-analytics-aggregator}",
            autoStartup = "${analytics.consumer-enabled:true}"
    )
    public void onMessage(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        Outcome outcome = process(rec.value(), rec.topic(), rec.partition(), rec.offset());
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

    Outcome process(String value, String topic, int partition, long offset) {
        if (value == null || value.isBlank()) {
            log.warn("{\"event\":\"empty_enriched_payload\",\"topic\":\"{}\",\"offset\":{}}", topic, offset);
            return Outcome.DLQ;
        }
        EnrichedEvent enriched;
        try {
            enriched = mapper.readValue(value, EnrichedEvent.class);
        } catch (Exception parseErr) {
            log.warn("{\"event\":\"enriched_parse_failed\",\"err\":\"{}\"}", parseErr.getMessage());
            return Outcome.DLQ;
        }
        if (enriched.siteId() == null || enriched.eventType() == null) {
            log.warn("{\"event\":\"enriched_missing_required\",\"eventId\":\"{}\"}", enriched.eventId());
            return Outcome.DLQ;
        }
        try {
            rollupUpsertService.upsert(enriched);
            return Outcome.DONE;
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"rollup_upsert_failed\",\"eventId\":\"{}\",\"err\":\"{}\"}",
                    enriched.eventId(), dbErr.getMessage());
            return Outcome.RETRY;
        }
    }
}
