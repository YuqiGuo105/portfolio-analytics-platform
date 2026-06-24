package site.yuqi.analytics.aggregator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoLevel;
import site.yuqi.analytics.common.kafka.Outcome;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class EnrichedEventConsumerTest {

    private ObjectMapper mapper;
    private RollupUpsertService rollup;
    private EnrichedEventConsumer consumer;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule()).findAndRegisterModules();
        rollup = mock(RollupUpsertService.class);
        consumer = new EnrichedEventConsumer(mapper, rollup);
    }

    @Test
    void emptyPayloadIsDroppedAndNotUpserted() {
        Outcome out = consumer.process("", "t", 0, 0);
        assertThat(out).isEqualTo(Outcome.DLQ);
        verifyNoInteractions(rollup);
    }

    @Test
    void malformedJsonIsDropped() {
        Outcome out = consumer.process("{not json", "t", 0, 1);
        assertThat(out).isEqualTo(Outcome.DLQ);
        verifyNoInteractions(rollup);
    }

    @Test
    void missingSiteIdIsDropped() throws Exception {
        EnrichedEvent ev = new EnrichedEvent("ev", null, "page_view",
                Instant.now(), Instant.now(), "s", "a", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0, "h",
                new EnrichedGeo(GeoLevel.GLOBAL, "GLOBAL", null, null, null));
        Outcome out = consumer.process(mapper.writeValueAsString(ev), "t", 0, 2);
        assertThat(out).isEqualTo(Outcome.DLQ);
        verifyNoInteractions(rollup);
    }

    @Test
    void happyPathInvokesUpsertAndReturnsDone() throws Exception {
        doNothing().when(rollup).upsert(any());
        EnrichedEvent ev = new EnrichedEvent("ev", "yuqi.site", "page_view",
                Instant.now(), Instant.now(), "s", "a", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0, "h",
                new EnrichedGeo(GeoLevel.GLOBAL, "GLOBAL", null, null, null));

        Outcome out = consumer.process(mapper.writeValueAsString(ev), "t", 0, 3);

        assertThat(out).isEqualTo(Outcome.DONE);
        verify(rollup).upsert(any(EnrichedEvent.class));
    }

    @Test
    void dbFailureReturnsRetry() throws Exception {
        doThrow(new RuntimeException("conn refused")).when(rollup).upsert(any());
        EnrichedEvent ev = new EnrichedEvent("ev", "yuqi.site", "page_view",
                Instant.now(), Instant.now(), "s", "a", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0, "h",
                new EnrichedGeo(GeoLevel.GLOBAL, "GLOBAL", null, null, null));

        Outcome out = consumer.process(mapper.writeValueAsString(ev), "t", 0, 4);
        assertThat(out).isEqualTo(Outcome.RETRY);
    }
}
