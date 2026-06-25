package site.yuqi.analytics.aggregator.enrich;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lazily populates {@code geo_areas} centroid coordinates from the real
 * (coarsened) lat/lng of incoming visitor events.
 *
 * <p>Privacy boundary: raw lat/lng never leave this package. We snap them
 * to a 0.1° (~11 km) grid before persisting and never store the original
 * point. This is the same boundary {@link GeoSnapService} enforces by not
 * propagating lat/lng on {@link site.yuqi.analytics.common.event.EnrichedGeo}.
 *
 * <p>{@link #ensureChain(String, String, String, Double, Double)} walks
 * COUNTRY → REGION → METRO, upserting each so that:
 * <ul>
 *   <li>the parent FK is satisfied (parents are inserted before children);</li>
 *   <li>an already-seeded centroid is never overwritten ({@code coalesce}
 *       keeps the existing value);</li>
 *   <li>a missing centroid on an existing row gets backfilled.</li>
 * </ul>
 *
 * <p>An in-process {@link ConcurrentHashMap}-backed seen-set keeps the
 * write amplification to one DB roundtrip per (area_id) per process
 * lifetime — important because this runs on the hot enrichment path.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class GeoAreaCentroidService {

    private static final String INSERT_SQL =
            "insert into public.geo_areas " +
            "  (geo_area_id, geo_level, parent_id, name, country, region, center_lat, center_lng) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?) " +
            "on conflict (geo_area_id) do update set " +
            "  center_lat = coalesce(public.geo_areas.center_lat, excluded.center_lat), " +
            "  center_lng = coalesce(public.geo_areas.center_lng, excluded.center_lng)";

    private final JdbcTemplate jdbc;

    /** Per-process cache of geo_area_ids we've already upserted. */
    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    /**
     * Ensure COUNTRY → REGION → METRO rows for this visitor's geography
     * exist in {@code geo_areas} with a centroid. The lat/lng input MUST
     * be the visitor's real coordinates — they are coarsened to 0.1°
     * before write and never persisted at full precision.
     *
     * <p>Any input combination is accepted; missing intermediate fields
     * just truncate the chain (e.g. a METRO insert is skipped when no
     * city is known). A missing country aborts the whole call: without
     * it we can't construct any geo_area_id.
     */
    public void ensureChain(String country, String region, String city,
                            Double rawLat, Double rawLng) {
        if (country == null || country.isBlank()) return;
        Double lat = coarsen(rawLat);
        Double lng = coarsen(rawLng);

        String countryId = "COUNTRY:" + country;
        upsert(countryId, "COUNTRY", "GLOBAL", country, country, null, lat, lng);

        if (region == null || region.isBlank()) return;
        String regionId = "REGION:" + country + ":" + region;
        upsert(regionId, "REGION", countryId, region, country, region, lat, lng);

        if (city == null || city.isBlank()) return;
        String metroId = "METRO:" + country + ":" + region + ":" + city;
        upsert(metroId, "METRO", regionId, city, country, region, lat, lng);
    }

    private void upsert(String areaId, String level, String parentId,
                        String name, String country, String region,
                        Double lat, Double lng) {
        if (seen.contains(areaId)) return;
        try {
            jdbc.update(INSERT_SQL,
                    areaId, level, parentId, name, country, region, lat, lng);
            seen.add(areaId);
        } catch (RuntimeException e) {
            // Never fail enrichment because the dimension lazy-write blipped;
            // we'll just retry on the next event for this area. Don't add to
            // `seen` so the next call re-attempts.
            log.warn("geo_areas upsert failed for {}: {}", areaId, e.getMessage());
        }
    }

    /** 0.1° quantization (~11 km). Privacy floor + small intra-metro spread. */
    private static Double coarsen(Double v) {
        return v == null ? null : Math.round(v * 10.0) / 10.0;
    }
}
