package site.yuqi.analytics.aggregator.enrich;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.RawEvent;

import java.time.Instant;

/**
 * Stage 1 of the aggregator: parse a raw JSON payload + run the
 * deterministic enrichment pipeline (UA classify → bot score → HMAC IP →
 * geo snap), returning a typed {@link EnrichedEvent}. No Kafka I/O lives
 * here — the consumer hands the result straight to {@code RollupUpsertService}.
 *
 * <p>This used to be a separate Spring service ({@code analytics-enrichment-service})
 * that wrote to its own Kafka topic, but Aiven's free tier only allows 2
 * topics total — so we collapsed enrichment into the aggregator process.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EnrichmentPipeline {

    private final ObjectMapper mapper;
    private final IpHashService ipHashService;
    private final UaParserService uaParser;
    private final BotScoreService botScorer;
    private final GeoSnapService geoSnap;
    private final DedupService dedup;

    /**
     * @return {@code null} when the payload is empty, malformed, missing
     *         required fields, or already processed (dedup hit). Callers
     *         that get {@code null} for an empty / parse error should DLQ
     *         the original record; a dedup-hit {@code null} should just be
     *         acked.
     */
    public EnrichedEvent parseAndEnrich(String value, ParseResult outResult) {
        outResult.reset();
        if (value == null || value.isBlank()) {
            outResult.parseError = "empty payload";
            return null;
        }
        RawEvent raw;
        try {
            raw = mapper.readValue(value, RawEvent.class);
        } catch (Exception parseErr) {
            outResult.parseError = "parse_error: " + parseErr.getMessage();
            return null;
        }
        if (raw.eventId() == null || raw.eventId().isBlank()
                || raw.eventType() == null || raw.siteId() == null) {
            outResult.parseError = "missing required field (eventId/eventType/siteId)";
            outResult.eventId = raw.eventId();
            return null;
        }
        outResult.eventId = raw.eventId();
        if (!dedup.acquire(raw.eventId())) {
            outResult.duplicate = true;
            return null;
        }
        // Expose the parsed RawEvent so downstream callers (e.g. the batch
        // consumer that now writes visitor_logs) can persist the raw
        // source-of-truth row without re-parsing the JSON.
        outResult.rawEvent = raw;
        return enrich(raw);
    }

    /** Pure enrichment — exposed for backfill which doesn't need parse/dedup. */
    public EnrichedEvent enrich(RawEvent raw) {
        UaParserService.Parsed ua = uaParser.parse(raw.uaRaw());
        double botScore = botScorer.score(ua.deviceType(), raw.referrer());
        boolean isBot = botScorer.isBot(botScore);
        String ipHash = ipHashService.hash(raw.ipRaw());
        EnrichedGeo geo = geoSnap.snap(raw.geoHint());

        return new EnrichedEvent(
                raw.eventId(),
                raw.siteId(),
                raw.eventType(),
                raw.eventTime() != null ? raw.eventTime() : Instant.now(),
                raw.serverTime() != null ? raw.serverTime() : Instant.now(),
                raw.sessionId(),
                raw.anonId(),
                raw.pageUrl(),
                raw.targetUrl(),
                raw.referrer(),
                ua.deviceType(),
                ua.browser(),
                ua.os(),
                isBot,
                botScore,
                ipHash,
                geo);
    }

    /** Out-parameter holder so callers can distinguish dedup hits from parse failures. */
    public static final class ParseResult {
        public String eventId;
        public String parseError;
        public boolean duplicate;
        /**
         * The parsed {@link RawEvent} — populated on successful parse+dedup
         * (i.e. the same code path that returns a non-null EnrichedEvent).
         * Left null on parse errors, missing-field failures, and dedup hits.
         * Consumers use this to persist visitor_logs before enrichment
         * discards the raw ip / geo lat-lng.
         */
        public RawEvent rawEvent;

        void reset() {
            this.eventId = null;
            this.parseError = null;
            this.duplicate = false;
            this.rawEvent = null;
        }
    }
}

