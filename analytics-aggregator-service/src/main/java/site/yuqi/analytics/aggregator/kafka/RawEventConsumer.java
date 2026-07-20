package site.yuqi.analytics.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.service.KafkaEventBatchProcessor;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;
import site.yuqi.analytics.common.kafka.DlqProducer;
import site.yuqi.analytics.common.kafka.Outcome;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch Kafka listener for {@code analytics.raw.events}.
 *
 * <p>Each poll delivers up to {@code max.poll.records} (100) records.
 * The batch is processed in four phases:
 * <ol>
 *   <li>Parse + enrich every record. Parse failures → DLQ immediately.
 *       Dedup hits and empty payloads are silently dropped. Both the
 *       {@link EnrichedEvent} <b>and</b> parsed {@link RawEvent} are collected
 *       for separate private-raw and canonical-fact writes.</li>
 *   <li>Claim each event in the durable Postgres inbox and update all database
 *       projections in one transaction.</li>
 *   <li>Acknowledge only after that transaction commits. Transient failures are
 *       thrown to the container error handler, which seeks and retries.</li>
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
    private final KafkaEventBatchProcessor batchProcessor;
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

        List<RawEventEnvelope> candidates = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> rec : records) {
            EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
            EnrichedEvent ev;
            try {
                ev = pipeline.parseAndEnrichWithoutDedup(rec.value(), pr);
            } catch (RuntimeException unexpected) {
                log.error("{\"event\":\"pipeline_threw\",\"offset\":{},\"err\":\"{}\"}",
                        rec.offset(), unexpected.getMessage());
                throw unexpected;
            }

            if (ev == null) {
                if (!pr.duplicate && pr.parseError != null) {
                    // Malformed record: send to DLQ so the batch can proceed.
                    dlq.publishOrThrow(rec.key(),
                            rec.value() == null ? "" : rec.value(),
                            "%s at %s-%d@%d".formatted(
                                    pr.parseError, rec.topic(), rec.partition(), rec.offset()));
                }
                // Malformed records are durable in the DLQ before source ack.
                continue;
            }
            candidates.add(new RawEventEnvelope(
                    rec.topic(), rec.partition(), rec.offset(), pr.rawEvent, ev));
        }

        if (candidates.isEmpty()) {
            ack.acknowledge();
            return;
        }

        try {
            batchProcessor.process(candidates);
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"batch_persist_failed\",\"batchSize\":{},\"err\":\"{}\"}",
                    candidates.size(), dbErr.getMessage());
            throw dbErr;
        }
        // A replay after an ack/commit race is absorbed by analytics_kafka_inbox.
        ack.acknowledge();
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
