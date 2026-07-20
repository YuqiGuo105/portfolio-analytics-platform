package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.aggregator.kafka.RawEventEnvelope;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;

import java.util.ArrayList;
import java.util.List;

/** Owns the atomic Postgres boundary for one Kafka poll. */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventBatchProcessor {

    private final KafkaInboxRepository inbox;
    private final DurableVisitThrottle visitThrottle;
    private final VisitorLogPersistService visitorLogs;
    private final SessionAggregatorService sessions;
    private final RollupUpsertService rollup;

    @Transactional
    public int process(List<RawEventEnvelope> records) {
        List<RawEvent> raw = new ArrayList<>(records.size());
        List<EnrichedEvent> enriched = new ArrayList<>(records.size());

        for (RawEventEnvelope record : records) {
            if (!inbox.claim(record)) {
                log.debug("{\"event\":\"kafka_inbox_duplicate\",\"eventId\":\"{}\"}",
                        record.enriched().eventId());
                continue;
            }
            if (!visitThrottle.shouldProcess(record.enriched())) {
                log.debug("{\"event\":\"visit_throttled\",\"eventId\":\"{}\"}",
                        record.enriched().eventId());
                continue;
            }
            raw.add(record.raw());
            enriched.add(record.enriched());
        }

        if (enriched.isEmpty()) return 0;
        visitorLogs.persistBatch(raw, enriched);
        sessions.processBatch(enriched);
        rollup.upsertBatch(enriched);
        return enriched.size();
    }
}
