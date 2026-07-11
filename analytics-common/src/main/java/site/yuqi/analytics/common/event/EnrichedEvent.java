package site.yuqi.analytics.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * In-process wire format produced by the aggregator's enrichment pipeline
 * and consumed by {@code RollupUpsertService}. Used to be on its own
 * Kafka topic before Aiven's free-tier 2-topic cap forced the merge.
 *
 * <p><b>Privacy invariants enforced by the type itself</b>:
 * <ul>
 *   <li>No {@code ipRaw} field — only {@code ipHash}.</li>
 *   <li>No raw {@code lat} / {@code lng} fields — only {@link EnrichedGeo}
 *       with {@code geoLevel} ∈ {GLOBAL, COUNTRY, REGION, METRO}.</li>
 *   <li>No fields below METRO (city, postcode, IP block, …).</li>
 * </ul>
 * If you find yourself wanting to add one of those fields, the answer is
 * always "no" — emit it to the DLQ for the operator to look at instead.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EnrichedEvent(
        String eventId,
        String siteId,
        String eventType,
        Instant eventTime,
        Instant serverTime,
        String sessionId,
        String anonId,
        String pageUrl,
        String targetUrl,
        String referrer,
        /** {@code "desktop" | "mobile" | "tablet" | "bot" | "unknown"}. */
        String deviceType,
        String browser,
        String os,
        /** {@code true} when {@code botScore >= 0.8} or UA is on the blacklist. */
        boolean bot,
        /** {@code [0.0, 1.0]}; higher = more bot-like. */
        double botScore,
        /** {@code HMAC-SHA256(salt, ipRaw)} hex. Salt rotation is the only way to break linkage. */
        String ipHash,
        EnrichedGeo geo,
        int schemaVersion,
        String consentState,
        Map<String, Object> properties) {

    /** Backwards-compatible constructor for callers producing schema-v1 events. */
    public EnrichedEvent(
            String eventId, String siteId, String eventType, Instant eventTime,
            Instant serverTime, String sessionId, String anonId, String pageUrl,
            String targetUrl, String referrer, String deviceType, String browser,
            String os, boolean bot, double botScore, String ipHash, EnrichedGeo geo) {
        this(eventId, siteId, eventType, eventTime, serverTime, sessionId, anonId,
                pageUrl, targetUrl, referrer, deviceType, browser, os, bot, botScore,
                ipHash, geo, 1, "unknown", Map.of());
    }
}
