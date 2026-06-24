package site.yuqi.analytics.enrichment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.RawEvent;
import site.yuqi.analytics.common.kafka.DlqProducer;
import site.yuqi.analytics.common.kafka.Outcome;

import java.time.Instant;

/**
 * Heart of the enrichment service. Pure, synchronous, side-effect-free
 * except for the explicit {@code redis.acquire}, {@code kafka.send} and
 * {@code dlq.publish} calls — which is what makes it easy to unit test.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EnrichmentProcessor {

    private final ObjectMapper mapper;
    private final IpHashService ipHashService;
    private final UaParserService uaParser;
    private final BotScoreService botScorer;
    private final GeoSnapService geoSnap;
    private final DedupService dedup;
    private final KafkaTemplate<String, String> kafka;
    private final DlqProducer dlq;

    @Value("${analytics.topics.enriched}")
    private String enrichedTopic;

    public Outcome process(String value, String topic, int partition, String offset) {
        if (value == null || value.isBlank()) {
            dlq.publish(null, "", "empty raw event payload at " + topic + "-" + partition + "@" + offset);
            return Outcome.DLQ;
        }

        RawEvent raw;
        try {
            raw = mapper.readValue(value, RawEvent.class);
        } catch (Exception parseErr) {
            dlq.publish(null, value, "parse_error: " + parseErr.getMessage());
            return Outcome.DLQ;
        }

        if (raw.eventId() == null || raw.eventId().isBlank()
                || raw.eventType() == null || raw.siteId() == null) {
            dlq.publish(raw.eventId(), value, "missing required field (eventId/eventType/siteId)");
            return Outcome.DLQ;
        }

        // Effectively-once dedup. If we've already processed this id, ack and skip.
        if (!dedup.acquire(raw.eventId())) {
            log.debug("{\"event\":\"dedup_skip\",\"eventId\":\"{}\"}", raw.eventId());
            return Outcome.DONE;
        }

        EnrichedEvent enriched = enrich(raw);

        String payload;
        try {
            payload = mapper.writeValueAsString(enriched);
        } catch (JsonProcessingException serialiseErr) {
            // Bug in our own code; do not DLQ the raw record for our own
            // serialization bug. Retry so the operator notices.
            log.error("{\"event\":\"serialise_failed\",\"eventId\":\"{}\",\"err\":\"{}\"}",
                    raw.eventId(), serialiseErr.getMessage());
            return Outcome.RETRY;
        }

        try {
            kafka.send(enrichedTopic, raw.sessionId(), payload).get();
            return Outcome.DONE;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return Outcome.RETRY;
        } catch (Exception sendErr) {
            log.warn("{\"event\":\"enriched_send_failed\",\"eventId\":\"{}\",\"err\":\"{}\"}",
                    raw.eventId(), sendErr.getMessage());
            return Outcome.RETRY;
        }
    }

    EnrichedEvent enrich(RawEvent raw) {
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
                geo
        );
    }
}
