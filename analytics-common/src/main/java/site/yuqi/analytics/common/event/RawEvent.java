package site.yuqi.analytics.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Wire format produced by {@code /api/analytics/ingest} (in the Portfolio
 * Next.js repo) onto the {@code analytics.raw.events} Kafka topic. Field
 * names are stable and consumed by the enrichment service.
 *
 * <p>Schema v2 carries both an ingestion-boundary HMAC and the exact request
 * context. Exact fields may only be written to the access-restricted raw tier;
 * all serving and recommendation paths consume {@link EnrichedEvent} instead.
 *
 * @param eventId    Client-generated UUIDv7. Dedup key everywhere downstream.
 * @param siteId     Logical tenant id (currently always {@code "yuqi.site"}).
 * @param eventType  {@code "page_view"} or {@code "click"}.
 * @param eventTime  Client wall-clock at the moment of the event.
 * @param serverTime Stamped by the Ingestion API on receipt.
 * @param sessionId  Tab-lived id (sessionStorage).
 * @param anonId     First-party cookie id; survives tabs but not full logout.
 * @param pageUrl    URL the event happened on.
 * @param targetUrl  Click destination (clicks only; null for page_view).
 * @param referrer   {@code document.referrer} at event time.
 * @param uaRaw      Raw {@code User-Agent} header (parsed downstream).
 * @param ipRaw      Raw client IP — restricted to the short-lived private raw tier.
 * @param geoHint    Edge-header-derived hint, advisory only.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RawEvent(
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
        String uaRaw,
        String ipRaw,
        GeoHint geoHint,
        Integer schemaVersion,
        String consentState,
        String ipHash,
        Map<String, Object> properties) {

    /** Backwards-compatible constructor used by legacy backfill and tests. */
    public RawEvent(
            String eventId, String siteId, String eventType, Instant eventTime,
            Instant serverTime, String sessionId, String anonId, String pageUrl,
            String targetUrl, String referrer, String uaRaw, String ipRaw, GeoHint geoHint) {
        this(eventId, siteId, eventType, eventTime, serverTime, sessionId, anonId,
                pageUrl, targetUrl, referrer, uaRaw, ipRaw, geoHint,
                1, "unknown", null, Map.of());
    }
}
