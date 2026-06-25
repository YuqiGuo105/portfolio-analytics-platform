package site.yuqi.analytics.aggregator.enrich;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.RawEvent;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EnrichmentPipelineTest {

    private EnrichmentPipeline pipeline;
    private DedupService dedup;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        IpHashService ip = new IpHashService("test-salt");
        UaParserService ua = new UaParserService();
        BotScoreService bot = new BotScoreService();
        GeoSnapService geo = new GeoSnapService(mock(GeoAreaCentroidService.class));
        dedup = mock(DedupService.class);
        pipeline = new EnrichmentPipeline(mapper, ip, ua, bot, geo, dedup);
    }

    @Test
    void enrichesAValidRawEventEndToEnd() {
        when(dedup.acquire(anyString())).thenReturn(true);
        String json = """
                {
                  "eventId":"evt-1",
                  "siteId":"yuqi.site",
                  "eventType":"page_view",
                  "eventTime":"2025-01-01T00:00:00Z",
                  "serverTime":"2025-01-01T00:00:01Z",
                  "uaRaw":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/537",
                  "ipRaw":"1.2.3.4",
                  "geoHint":{"country":"US","region":"UT","city":"Salt Lake City"}
                }
                """;
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        EnrichedEvent out = pipeline.parseAndEnrich(json, pr);

        assertThat(out).isNotNull();
        assertThat(out.eventId()).isEqualTo("evt-1");
        assertThat(out.ipHash()).isNotBlank().doesNotContain("1.2.3.4");
        assertThat(out.geo().country()).isEqualTo("US");
        assertThat(pr.duplicate).isFalse();
        assertThat(pr.parseError).isNull();
    }

    @Test
    void blankPayloadIsParseError() {
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        assertThat(pipeline.parseAndEnrich("", pr)).isNull();
        assertThat(pr.parseError).contains("empty");
    }

    @Test
    void malformedJsonIsParseError() {
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        assertThat(pipeline.parseAndEnrich("not json", pr)).isNull();
        assertThat(pr.parseError).contains("parse_error");
    }

    @Test
    void missingRequiredFieldsIsParseError() {
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        assertThat(pipeline.parseAndEnrich("{\"eventId\":\"x\"}", pr)).isNull();
        assertThat(pr.parseError).contains("missing");
    }

    @Test
    void duplicateEventIdShortCircuits() {
        when(dedup.acquire(anyString())).thenReturn(false);
        String json = """
                {"eventId":"evt-2","siteId":"yuqi.site","eventType":"click","eventTime":"2025-01-01T00:00:00Z"}
                """;
        EnrichmentPipeline.ParseResult pr = new EnrichmentPipeline.ParseResult();
        EnrichedEvent out = pipeline.parseAndEnrich(json, pr);

        assertThat(out).isNull();
        assertThat(pr.duplicate).isTrue();
    }

    @Test
    void backfillEnrichSkipsDedup() {
        RawEvent raw = new RawEvent(
                "vl:123", "yuqi.site", "page_view",
                Instant.parse("2024-12-31T00:00:00Z"),
                Instant.parse("2024-12-31T00:00:01Z"),
                null, null, null, null, null, null, null,
                new GeoHint("US", null, null, null, null, "supabase-backfill"));
        EnrichedEvent out = pipeline.enrich(raw);
        assertThat(out.eventId()).isEqualTo("vl:123");
        assertThat(out.geo().country()).isEqualTo("US");
    }
}
