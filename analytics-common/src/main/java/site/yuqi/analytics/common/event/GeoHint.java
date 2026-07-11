package site.yuqi.analytics.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Optional best-effort geo hint attached to a {@link RawEvent} by the
 * Ingestion API based on Vercel / Cloudflare edge headers.
 *
 * <p>The enrichment service treats this as an advisory input — if the
 * MaxMind GeoIP lookup on {@code ipRaw} disagrees with the hint, the
 * MaxMind result wins (the edge header is easier to spoof). Exact values may
 * only be retained in the restricted raw tier; serving facts use snapped geo.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GeoHint(
        String country,
        String region,
        String city,
        Double lat,
        Double lng,
        /** Source of the hint, for debugging only: "vercel" | "cloudflare" | "none". */
        String src) {
}
