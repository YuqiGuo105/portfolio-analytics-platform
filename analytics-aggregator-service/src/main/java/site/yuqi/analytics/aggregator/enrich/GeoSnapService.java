package site.yuqi.analytics.aggregator.enrich;

import lombok.RequiredArgsConstructor;
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
 * <p>As a side effect this service hands the visitor's raw lat/lng off
 * to {@link GeoAreaCentroidService#ensureChain} so the matching
 * {@code geo_areas} rows get a coarsened centroid the first time we see
 * them. The raw coordinates do <b>not</b> escape this method —
 * {@code ensureChain} quantises to 0.1° internally and the returned
 * {@link EnrichedGeo} carries no lat/lng. That keeps the privacy
 * invariant intact.
 */
@Service
@RequiredArgsConstructor
public class GeoSnapService {

    private static final String GLOBAL_AREA_ID = "GLOBAL";

    private final GeoAreaCentroidService centroids;

    public EnrichedGeo snap(GeoHint hint) {
        if (hint == null) {
            return new EnrichedGeo(GeoLevel.GLOBAL, GLOBAL_AREA_ID, null, null, null);
        }
        // Privacy-bound side effect: lazy-populate the dimension table so
        // the dashboard's pin lands close to where the visitor actually
        // was, not on the (often offshore) country centroid. The centroid
        // service coarsens to 0.1°; raw lat/lng never leave this call.
        centroids.ensureChain(hint.country(), hint.region(), hint.city(),
                hint.lat(), hint.lng());

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
