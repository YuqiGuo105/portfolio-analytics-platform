package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import site.yuqi.analytics.aggregator.kafka.RawEventEnvelope;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventBatchProcessorTest {

    private KafkaInboxRepository inbox;
    private DurableVisitThrottle throttle;
    private VisitorLogPersistService visitorLogs;
    private SessionAggregatorService sessions;
    private RollupUpsertService rollup;
    private KafkaEventBatchProcessor processor;

    @BeforeEach
    void setUp() {
        inbox = mock(KafkaInboxRepository.class);
        throttle = mock(DurableVisitThrottle.class);
        visitorLogs = mock(VisitorLogPersistService.class);
        sessions = mock(SessionAggregatorService.class);
        rollup = mock(RollupUpsertService.class);
        processor = new KafkaEventBatchProcessor(inbox, throttle, visitorLogs, sessions, rollup);
    }

    @Test
    void committedInboxDuplicateDoesNotTouchAnyProjection() {
        RawEventEnvelope record = record("evt-1");
        when(inbox.claim(record)).thenReturn(false);

        assertThat(processor.process(List.of(record))).isZero();

        verify(throttle, never()).shouldProcess(record.enriched());
        verify(visitorLogs, never()).persistBatch(anyList(), anyList());
        verify(sessions, never()).processBatch(anyList());
        verify(rollup, never()).upsertBatch(anyList());
    }

    @Test
    void newlyClaimedEventUpdatesAllProjectionsBeforeReturning() {
        RawEventEnvelope record = record("evt-2");
        when(inbox.claim(record)).thenReturn(true);
        when(throttle.shouldProcess(record.enriched())).thenReturn(true);

        assertThat(processor.process(List.of(record))).isEqualTo(1);

        InOrder order = inOrder(inbox, throttle, visitorLogs, sessions, rollup);
        order.verify(inbox).claim(record);
        order.verify(throttle).shouldProcess(record.enriched());
        order.verify(visitorLogs).persistBatch(anyList(), anyList());
        order.verify(sessions).processBatch(anyList());
        order.verify(rollup).upsertBatch(anyList());
    }

    private static RawEventEnvelope record(String eventId) {
        Instant now = Instant.parse("2026-07-19T00:00:00Z");
        RawEvent raw = new RawEvent(
                eventId, "yuqi.site", "page_view", now, now,
                "session", "anon", "/", null, null,
                "ua", "127.0.0.1", null, 2, "granted", null, Map.of());
        EnrichedEvent enriched = new EnrichedEvent(
                eventId, "yuqi.site", "page_view", now, now,
                "session", "anon", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "hash", null, 2, "granted", Map.of());
        return new RawEventEnvelope("analytics.raw.events", 0, 1L, raw, enriched);
    }
}
