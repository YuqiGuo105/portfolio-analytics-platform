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

import java.util.ArrayList;
import java.util.List;

/**
 * Batch Kafka listener for {@code analytics.raw.events}.
 *
 * <p>Each poll delivers up to {@code max.poll.records} (100) records.
 * The batch is processed in three phases:
 * <ol>
 *   <li>Parse + enrich every record. Parse failures → DLQ immediately.
 *       Dedup hits and empty payloads are silently dropped.</li>
 *   <li>Hand the successfully-enriched records to
 *       {@link RollupUpsertService#upsertBatch} which pre-aggregates
 *       in memory and fires a single {@code jdbc.batchUpdate} — one
 *       round-trip to Postgres per Kafka poll instead of N×2.</li>
 *   <li>Acknowledge the whole batch once the DB write succeeds. If the
 *       DB write throws, the batch is left un-acked so Kafka re-delivers
 *       it (at-least-once). The upstream UUIDv7 dedup key keeps the
 *       re-processed records idempotent.</li>
 * </ol>
 *
 * <p>The previous single-record listener is preserved as {@link #process}
 * (package-private) so existing unit tests continue to compile unchanged.
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
    public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records.isEmpty()) {
            ack.acknowledge();
            return;
        }

        List<EnrichedEvent> enriched = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> rec : records) {
            EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
            EnrichedEvent ev;
            try {
                ev = pipeline.parseAndEnrich(rec.value(), pr);
            } catch (RuntimeException unexpected) {
                log.error("{\"event\":\"pipeline_threw\",\"offset\":{},\"err\":\"{}\"}",
                        rec.offset(), unexpected.getMessage());
                // Leave entire batch un-acked so it is re-delivered and retried.
                return;
            }

            if (ev == null) {
                if (!pr.duplicate && pr.parseError != null) {
                    // Malformed record: send to DLQ so the batch can proceed.
                    dlq.publish(rec.key(),
                            rec.value() == null ? "" : rec.value(),
                            "%s at %s-%d@%d".formatted(
                                    pr.parseError, rec.topic(), rec.partition(), rec.offset()));
                }
                // Dedup hit or empty payload — just skip, already handled.
                continue;
            }
            enriched.add(ev);
        }

        if (enriched.isEmpty()) {
            ack.acknowledge();
            return;
        }

        try {
            rollup.upsertBatch(enriched);
            ack.acknowledge();
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"batch_upsert_failed\",\"batchSize\":{},\"err\":\"{}\"}",
                    enriched.size(), dbErr.getMessage());
            // Do NOT ack — let Kafka re-deliver the whole batch.
        }
    }

    /**
     * Single-record path kept for unit-test compatibility and for
     * {@link site.yuqi.analytics.aggregator.backfill.BackfillService} which
     * drives the pipeline one row at a time from the legacy tables.
     */
    Outcome process(String value, String key, String topic, int partition, long offset) {
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        EnrichedEvent ev;
        try {
            ev = pipeline.parseAndEnrich(value, pr);
        } catch (RuntimeException unexpected) {
            log.error("{\"event\":\"pipeline_threw\",\"offset\":{},\"err\":\"{}\"}",
                    offset, unexpected.getMessage());
            return Outcome.RETRY;
        }

        if (ev == null) {
            if (pr.duplicate) return Outcome.DONE;
            dlq.publish(key, value == null ? "" : value,
                    "%s at %s-%d@%d".formatted(pr.parseError, topic, partition, offset));
            return Outcome.DLQ;
        }

        try {
            rollup.upsert(ev);
            return Outcome.DONE;
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"rollup_upsert_failed\",\"eventId\":\"{}\",\"err\":\"{}\"}",
                    ev.eventId(), dbErr.getMessage());
            return Outcome.RETRY;
        }
    }
}
