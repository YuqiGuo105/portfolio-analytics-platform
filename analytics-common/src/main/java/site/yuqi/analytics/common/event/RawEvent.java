package site.yuqi.analytics.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Wire format produced by {@code /api/analytics/ingest} (in the Portfolio
 * Next.js repo) onto the {@code analytics.raw.events} Kafka topic. Field
 * names are stable and consumed by the enrichment service.
 *
 * <p>This is the <b>only</b> place {@code ipRaw} appears. By the time
 * enrichment writes an {@link EnrichedEvent} the IP has been HMAC'd into
 * {@code ipHash} and the plaintext discarded.
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
 * @param ipRaw      Raw client IP — discarded by enrichment, never persisted.
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
        GeoHint geoHint) {
}
