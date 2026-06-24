package site.yuqi.analytics.aggregator.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoLevel;
import site.yuqi.analytics.common.kafka.DlqProducer;
import site.yuqi.analytics.common.kafka.Outcome;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
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
    private DlqProducer dlq;
    private RawEventConsumer consumer;

    @BeforeEach
    void setUp() {
        pipeline = mock(EnrichmentPipeline.class);
        rollup = mock(RollupUpsertService.class);
        dlq = mock(DlqProducer.class);
        consumer = new RawEventConsumer(pipeline, rollup, dlq);
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
}
