package site.yuqi.analytics.aggregator.enrich;

import org.springframework.stereotype.Service;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.GeoLevel;

/**
 * Snaps an incoming {@link GeoHint} (or a future MaxMind lookup result)
 * onto a {@code geo_areas.geo_area_id} at METRO floor.
 *
 * <p>This is intentionally simple: when we don't have a populated
 * {@code geo_areas} table loaded into memory yet, we keep ascending the
 * hierarchy until we find a level the dimension knows about. Worst case
 * we return a GLOBAL bucket — which is correct, just less useful.
 *
 * <p>The full implementation (TODO) loads {@code geo_areas} into a JTS
 * R-Tree on startup and performs an actual point-in-polygon snap. The
 * privacy invariant we hold today: <b>raw lat/lng never escape this
 * service</b>.
 */
@Service
public class GeoSnapService {

    private static final String GLOBAL_AREA_ID = "GLOBAL";

    public EnrichedGeo snap(GeoHint hint) {
        if (hint == null) {
            return new EnrichedGeo(GeoLevel.GLOBAL, GLOBAL_AREA_ID, null, null, null);
        }
        // Strict ascending fallback: METRO → REGION → COUNTRY → GLOBAL.
        if (hint.city() != null && !hint.city().isBlank()
                && hint.country() != null && !hint.country().isBlank()) {
            String metroId = "METRO:" + hint.country() + ":" + safe(hint.region()) + ":" + hint.city();
            return new EnrichedGeo(GeoLevel.METRO, metroId, hint.country(), hint.region(), hint.city());
        }
        if (hint.region() != null && hint.country() != null) {
            String regionId = "REGION:" + hint.country() + ":" + hint.region();
            return new EnrichedGeo(GeoLevel.REGION, regionId, hint.country(), hint.region(), null);
        }
        if (hint.country() != null) {
            String countryId = "COUNTRY:" + hint.country();
            return new EnrichedGeo(GeoLevel.COUNTRY, countryId, hint.country(), null, null);
        }
        return new EnrichedGeo(GeoLevel.GLOBAL, GLOBAL_AREA_ID, null, null, null);
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }
}
