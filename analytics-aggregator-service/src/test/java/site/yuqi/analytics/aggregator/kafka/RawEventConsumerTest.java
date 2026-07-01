package site.yuqi.analytics.aggregator.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.aggregator.service.VisitorLogPersistService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.GeoLevel;
import site.yuqi.analytics.common.event.RawEvent;
import site.yuqi.analytics.common.kafka.DlqProducer;
import site.yuqi.analytics.common.kafka.Outcome;

import org.mockito.InOrder;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * 3-state DONE / DLQ / RETRY contract for the merged raw → enrich → upsert
 * consumer. The previous-generation raw and enriched consumers each had
 * their own test of the same shape — keeping it on this combined version
 * preserves the same coverage invariants.
 */
class RawEventConsumerTest {

    private EnrichmentPipeline pipeline;
    private RollupUpsertService rollup;
    private VisitorLogPersistService visitorLogs;
    private DlqProducer dlq;
    private RawEventConsumer consumer;

    @BeforeEach
    void setUp() {
        pipeline = mock(EnrichmentPipeline.class);
        rollup = mock(RollupUpsertService.class);
        visitorLogs = mock(VisitorLogPersistService.class);
        dlq = mock(DlqProducer.class);
        consumer = new RawEventConsumer(pipeline, rollup, visitorLogs, dlq);
    }

    @Test
    void happyPathReturnsDone() {
        EnrichedEvent enriched = sampleEnriched();
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.eventId = enriched.eventId();
            return enriched;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));
        doNothing().when(rollup).upsert(enriched);

        Outcome out = consumer.process("{\"foo\":1}", "k1", "topic", 0, 42L);

        assertThat(out).isEqualTo(Outcome.DONE);
        verify(rollup, times(1)).upsert(enriched);
        verify(dlq, never()).publish(any(), any(), any());
    }

    @Test
    void parseFailureGoesToDlq() {
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.parseError = "parse_error: boom";
            return null;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));

        Outcome out = consumer.process("not json", "k", "t", 1, 99L);

        assertThat(out).isEqualTo(Outcome.DLQ);
        verify(dlq, times(1)).publish(eq("k"), eq("not json"), anyString());
        verify(rollup, never()).upsert(any());
    }

    @Test
    void duplicateAcksWithoutDlq() {
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.duplicate = true;
            return null;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));

        Outcome out = consumer.process("{}", "k", "t", 0, 1L);

        assertThat(out).isEqualTo(Outcome.DONE);
        verify(rollup, never()).upsert(any());
        verify(dlq, never()).publish(any(), any(), any());
    }

    @Test
    void pipelineExceptionTriggersRetry() {
        doThrow(new RuntimeException("nepe")).when(pipeline)
                .parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));

        Outcome out = consumer.process("{}", "k", "t", 0, 7L);

        assertThat(out).isEqualTo(Outcome.RETRY);
        verify(rollup, never()).upsert(any());
        verify(dlq, never()).publish(any(), any(), any());
    }

    @Test
    void upsertFailureTriggersRetry() {
        EnrichedEvent enriched = sampleEnriched();
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.eventId = enriched.eventId();
            return enriched;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));
        doThrow(new RuntimeException("db gone")).when(rollup).upsert(enriched);

        Outcome out = consumer.process("{}", "k", "t", 0, 3L);

        assertThat(out).isEqualTo(Outcome.RETRY);
        verify(dlq, never()).publish(any(), any(), any());
    }

    private static EnrichedEvent sampleEnriched() {
        return new EnrichedEvent(
                "evt-1", "yuqi.site", "page_view",
                Instant.parse("2025-01-01T00:00:00Z"),
                Instant.parse("2025-01-01T00:00:01Z"),
                null, null, null, null, null,
                "desktop", "chrome", "macos",
                false, 0.1, "deadbeef",
                new EnrichedGeo(GeoLevel.COUNTRY, "COUNTRY:US", "US", null, null));
    }

    private static RawEvent sampleRaw(String eventId) {
        return new RawEvent(
                eventId, "yuqi.site", "page_view",
                Instant.parse("2025-01-01T00:00:00Z"),
                Instant.parse("2025-01-01T00:00:01Z"),
                null, null, "/", null, null,
                "Mozilla/5.0", "1.2.3.4",
                new GeoHint("US", "CA", "SF", 37.77, -122.41, "vercel"));
    }

    // ---------------------------------------------------------------- batch listener

    @Test
    void batchHappyPathCallsPersistThenUpsertBatchAndAcks() {
        EnrichedEvent enriched = sampleEnriched();
        RawEvent raw = sampleRaw(enriched.eventId());
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.eventId = enriched.eventId();
            pr.rawEvent = raw;
            return enriched;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));
        doNothing().when(visitorLogs).persistBatch(anyList());
        doNothing().when(rollup).upsertBatch(anyList());

        Acknowledgment ack = mock(Acknowledgment.class);
        consumer.onMessage(List.of(record("k1", "{\"x\":1}", 0L)), ack);

        // Persist must happen before rollup so that a rollup failure leaves
        // visitor_logs already written (idempotent via ON CONFLICT); a
        // persist failure leaves rollup untouched.
        InOrder inOrder = inOrder(visitorLogs, rollup, ack);
        inOrder.verify(visitorLogs).persistBatch(anyList());
        inOrder.verify(rollup).upsertBatch(anyList());
        inOrder.verify(ack).acknowledge();
        verify(dlq, never()).publish(any(), any(), any());
    }

    @Test
    void batchPersistFailureLeavesUnackedAndSkipsRollup() {
        EnrichedEvent enriched = sampleEnriched();
        RawEvent raw = sampleRaw(enriched.eventId());
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.eventId = enriched.eventId();
            pr.rawEvent = raw;
            return enriched;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));
        doThrow(new RuntimeException("visitor_logs write failed"))
                .when(visitorLogs).persistBatch(anyList());

        Acknowledgment ack = mock(Acknowledgment.class);
        consumer.onMessage(List.of(record("k", "{}", 1L)), ack);

        verify(visitorLogs, times(1)).persistBatch(anyList());
        verify(rollup,      never()).upsertBatch(anyList());
        verify(ack,         never()).acknowledge();
        verify(dlq,         never()).publish(any(), any(), any());
    }

    @Test
    void batchParseFailureGoesToDlqAndStillAcksGoodRecords() {
        EnrichedEvent good = sampleEnriched();
        RawEvent goodRaw = sampleRaw(good.eventId());
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            String value = inv.getArgument(0);
            if ("bad".equals(value)) {
                pr.parseError = "parse_error";
                return null;
            }
            pr.eventId = good.eventId();
            pr.rawEvent = goodRaw;
            return good;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));

        Acknowledgment ack = mock(Acknowledgment.class);
        consumer.onMessage(List.of(
                record("kgood", "{}",  10L),
                record("kbad",  "bad", 11L)
        ), ack);

        verify(dlq,         times(1)).publish(eq("kbad"), eq("bad"), anyString());
        verify(visitorLogs, times(1)).persistBatch(anyList());
        verify(rollup,      times(1)).upsertBatch(anyList());  // good record still processed
        verify(ack,         times(1)).acknowledge();
    }

    @Test
    void batchDbFailureLeavesUnacked() {
        EnrichedEvent enriched = sampleEnriched();
        RawEvent raw = sampleRaw(enriched.eventId());
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.eventId = enriched.eventId();
            pr.rawEvent = raw;
            return enriched;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));
        doNothing().when(visitorLogs).persistBatch(anyList());
        doThrow(new RuntimeException("db gone")).when(rollup).upsertBatch(anyList());

        Acknowledgment ack = mock(Acknowledgment.class);
        consumer.onMessage(List.of(record("k", "{}", 1L)), ack);

        verify(ack, never()).acknowledge();
        verify(dlq, never()).publish(any(), any(), any());
    }

    @Test
    void batchAllDuplicatesAcksAndSkipsUpsert() {
        doAnswer(inv -> {
            EnrichmentPipeline.ParseResult pr = inv.getArgument(1);
            pr.duplicate = true;
            return null;
        }).when(pipeline).parseAndEnrich(anyString(), any(EnrichmentPipeline.ParseResult.class));

        Acknowledgment ack = mock(Acknowledgment.class);
        consumer.onMessage(List.of(record("k", "{}", 1L)), ack);

        verify(rollup, never()).upsertBatch(anyList());
        verify(ack,    times(1)).acknowledge();
        verify(dlq,    never()).publish(any(), any(), any());
    }

    private static ConsumerRecord<String, String> record(String key, String value, long offset) {
        return new ConsumerRecord<>("analytics.raw.events", 0, offset, key, value);
    }
}
