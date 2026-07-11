package site.yuqi.analytics.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import site.yuqi.analytics.aggregator.enrich.DedupService;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.aggregator.service.SessionAggregatorService;
import site.yuqi.analytics.aggregator.service.VisitorLogPersistService;
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
 *   <li>Persist exact raw, sanitized fact, and legacy compatibility rows in one
 *       idempotent transaction via {@link VisitorLogPersistService}.</li>
 *   <li>Rebuild affected session projections and configured funnel steps.</li>
 *   <li>Upsert additive geo rollups last, then acknowledge. On persistence
 *       failure Redis processing/throttle claims are released for Kafka replay.</li>
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
    private final SessionAggregatorService sessions;
    private final VisitorLogPersistService visitorLogs;
    private final DlqProducer dlq;
    private final DedupService dedupService;

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
        // Parallel list of raw events for the access-restricted raw tier.
        // Order does not need to match `enriched` — the two tables are
        // independent — but both lists are populated from the same
        // successful-parse iterations so they stay in lockstep in practice.
        List<RawEvent> rawEvents = new ArrayList<>(records.size());

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
            // Visit throttle: same session+page within 5 min counts as 1.
            // Checked here (before visitor_logs write) so the dashboard TODAY
            // count doesn't increment on rapid page refreshes.
            if ("page_view".equals(ev.eventType()) && !dedupService.throttleVisit(ev)) {
                log.debug("{\"event\":\"visit_throttled\",\"page\":\"{}\"}", ev.pageUrl());
                continue;
            }
            enriched.add(ev);
            if (pr.rawEvent != null) rawEvents.add(pr.rawEvent);
        }

        if (enriched.isEmpty()) {
            ack.acknowledge();
            return;
        }

        try {
            // Idempotent facts first, then idempotent session projections.
            // Additive rollups run last so no fallible side effect follows them.
            visitorLogs.persistBatch(rawEvents, enriched);
            sessions.processBatch(enriched);
            rollup.upsertBatch(enriched);
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"batch_persist_failed\",\"batchSize\":{},\"err\":\"{}\"}",
                    enriched.size(), dbErr.getMessage());
            enriched.forEach(event -> {
                dedupService.release(event.eventId());
                dedupService.releaseThrottle(event);
            });
            // Do NOT ack — let Kafka re-deliver the whole batch.
            return;
        }
        // Keep acknowledgement outside the persistence catch. If the broker
        // rejects the ack, the retained dedup claims make replay a no-op.
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
