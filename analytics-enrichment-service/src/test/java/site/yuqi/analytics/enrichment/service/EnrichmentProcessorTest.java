package site.yuqi.analytics.enrichment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.GeoLevel;
import site.yuqi.analytics.common.event.RawEvent;
import site.yuqi.analytics.common.kafka.DlqProducer;
import site.yuqi.analytics.common.kafka.Outcome;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EnrichmentProcessorTest {

    private ObjectMapper mapper;
    private IpHashService ipHash;
    private UaParserService ua;
    private BotScoreService bot;
    private GeoSnapService geo;
    private DedupService dedup;
    @SuppressWarnings("rawtypes") private KafkaTemplate kafka;
    private DlqProducer dlq;

    private EnrichmentProcessor processor;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule()).findAndRegisterModules();
        ipHash = new IpHashService("test-salt");
        ua = new UaParserService();
        bot = new BotScoreService();
        geo = new GeoSnapService();
        dedup = mock(DedupService.class);
        kafka = mock(KafkaTemplate.class);
        dlq = mock(DlqProducer.class);

        processor = new EnrichmentProcessor(mapper, ipHash, ua, bot, geo, dedup, kafka, dlq);
        ReflectionTestUtils.setField(processor, "enrichedTopic", "test.enriched");

        // Default: dedup succeeds and kafka send succeeds.
        when(dedup.acquire(anyString())).thenReturn(true);
        when(kafka.send(anyString(), any(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        doNothing().when(dlq).publish(any(), any(), anyString());
    }

    @Test
    void emptyPayloadGoesToDlq() {
        Outcome out = processor.process("", "raw", 0, "0");
        assertThat(out).isEqualTo(Outcome.DLQ);
        verify(dlq).publish(eq(null), eq(""), anyString());
    }

    @Test
    void malformedJsonGoesToDlq() {
        Outcome out = processor.process("{not json", "raw", 0, "1");
        assertThat(out).isEqualTo(Outcome.DLQ);
        verify(dlq).publish(eq(null), eq("{not json"), anyString());
    }

    @Test
    void missingRequiredFieldsGoToDlq() throws Exception {
        String json = mapper.writeValueAsString(new RawEvent(
                null, "s", "page_view", Instant.now(), Instant.now(),
                "sess", "anon", "/", null, null, "ua", "1.2.3.4", null));
        Outcome out = processor.process(json, "raw", 0, "2");
        assertThat(out).isEqualTo(Outcome.DLQ);
    }

    @Test
    void duplicateEventIdIsSkipped() throws Exception {
        when(dedup.acquire("dup-id")).thenReturn(false);
        String json = mapper.writeValueAsString(new RawEvent(
                "dup-id", "s", "page_view", Instant.now(), Instant.now(),
                "sess", "anon", "/", null, null, "ua", "1.2.3.4", null));
        Outcome out = processor.process(json, "raw", 0, "3");
        assertThat(out).isEqualTo(Outcome.DONE);
    }

    @Test
    void happyPathProducesEnrichedEventWithoutRawIp() throws Exception {
        RawEvent raw = new RawEvent(
                "ev-1", "yuqi.site", "page_view",
                Instant.parse("2026-06-23T12:00:00Z"),
                Instant.parse("2026-06-23T12:00:01Z"),
                "sess-1", "anon-1", "/blog/x", null, "https://google.com",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X) Chrome/126 Safari/537",
                "1.2.3.4", null);

        Outcome out = processor.process(mapper.writeValueAsString(raw), "raw", 0, "4");

        assertThat(out).isEqualTo(Outcome.DONE);

        EnrichedEvent enriched = processor.enrich(raw);
        assertThat(enriched.ipHash()).isNotNull().hasSize(64);
        assertThat(enriched.deviceType()).isEqualTo("desktop");
        assertThat(enriched.browser()).isEqualTo("Chrome");
        assertThat(enriched.bot()).isFalse();
        assertThat(enriched.geo().geoLevel()).isEqualTo(GeoLevel.GLOBAL);

        // Most important: serialised wire payload must NOT contain "ipRaw".
        String wire = mapper.writeValueAsString(enriched);
        assertThat(wire).doesNotContain("ipRaw").doesNotContain("1.2.3.4");
    }

    @Test
    @SuppressWarnings("unchecked")
    void kafkaSendFailureReturnsRetry() throws Exception {
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new ExecutionException("broker", new RuntimeException()));
        when(kafka.send(anyString(), any(), anyString())).thenReturn(failed);

        String json = mapper.writeValueAsString(new RawEvent(
                "ev-2", "s", "page_view", Instant.now(), Instant.now(),
                "sess", "anon", "/", null, null, "ua", "1.2.3.4", null));

        Outcome out = processor.process(json, "raw", 0, "5");
        assertThat(out).isEqualTo(Outcome.RETRY);
    }
}
