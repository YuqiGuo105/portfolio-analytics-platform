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
 *       {@link EnrichedEvent} <b>and</b> the parsed {@link RawEvent}
 *       are collected — the raw form is required to write the raw
 *       source-of-truth row into {@code visitor_logs} before enrichment
 *       discards {@code ipRaw} and coarsens the geo hint.</li>
 *   <li>Persist raw events into {@code visitor_logs} via
 *       {@link VisitorLogPersistService#persistBatch} — one
 *       {@code jdbc.batchUpdate} per poll, idempotent via
 *       {@code ON CONFLICT (event_id) DO NOTHING}. This replaces the
 *       per-request write the Portfolio /api/track endpoint used to do
 *       against Supabase.</li>
 *   <li>Upsert the enriched batch into {@code geo_time_rollups} via
 *       {@link RollupUpsertService#upsertBatch}.</li>
 *   <li>Acknowledge only after BOTH writes succeed. If either throws,
 *       leave the batch un-acked so Kafka re-delivers it. Idempotency:
 *       visitor_logs is guarded by the ON CONFLICT DO NOTHING on
 *       event_id (partial unique index added in V6); rollups are
 *       guarded by the upstream UUIDv7 dedup key in
 *       {@link EnrichmentPipeline#parseAndEnrich} — a redelivered
 *       event fails the {@code DedupService.acquire} check and is
 *       dropped before it reaches the rollup batch, so counters never
 *       double-count.</li>
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
        // Parallel list of raw events for the visitor_logs batch insert.
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
            if (!dedupService.throttleVisit(ev)) {
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
            // Phase 2: raw source-of-truth first. If this throws we do NOT
            // ack, Kafka replays the batch, and the ON CONFLICT (event_id)
            // DO NOTHING clause absorbs the retry without duplicates.
            visitorLogs.persistBatch(rawEvents);
            // Phase 3: aggregated rollups. Same at-least-once contract —
            // upstream dedup blocks a replayed event from being counted twice.
            rollup.upsertBatch(enriched);
            // Phase 3.5: session aggregation (best-effort — failure here
            // must NOT block acknowledgement since rollups already committed).
            try {
                sessions.processBatch(enriched);
            } catch (RuntimeException sessionErr) {
                log.warn("{\"event\":\"session_batch_failed\",\"batchSize\":{},\"err\":\"{}\"}",
                        enriched.size(), sessionErr.getMessage());
            }
            // Phase 4: single ack for the entire poll.
            ack.acknowledge();
        } catch (RuntimeException dbErr) {
            log.warn("{\"event\":\"batch_persist_failed\",\"batchSize\":{},\"err\":\"{}\"}",
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

