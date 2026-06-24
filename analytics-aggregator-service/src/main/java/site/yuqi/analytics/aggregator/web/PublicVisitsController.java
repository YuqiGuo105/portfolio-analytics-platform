package site.yuqi.analytics.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Public read endpoints powering the visitor globe and the
 * {@code /analytics} page on the Portfolio dashboard. No auth — this is
 * the same trust posture as the existing {@code visitor_logs} table the
 * Portfolio queries directly from Supabase.
 *
 * <p>CORS is permissive on these specific endpoints because Render and
 * Vercel live on different origins. Everything else on the service is
 * still locked down by the absence of any other public controller +
 * actuator's built-in security.
 */
@RestController
@RequestMapping("/api/public")
@CrossOrigin(origins = "*", allowedHeaders = "*", methods = {
        org.springframework.web.bind.annotation.RequestMethod.GET,
        org.springframework.web.bind.annotation.RequestMethod.HEAD,
        org.springframework.web.bind.annotation.RequestMethod.OPTIONS
})
@RequiredArgsConstructor
public class PublicVisitsController {

    private final JdbcTemplate jdbc;

    @Value("${analytics.backfill.site-id:yuqi.site}")
    private String siteId;

    /**
     * Per-country (or per-METRO when seeded) counts since {@code days} ago,
     * joined with {@code geo_areas.center_lat/lng} so the globe can place
     * a marker. METRO buckets win over their COUNTRY parents when present.
     *
     * <p>Pass {@code days=0} (or any non-positive number) to drop the time
     * filter entirely and return the all-time aggregate. This is the
     * "All visitors" view powering the home-page globe.
     *
     * @param days how far back to roll up (default 30, {@code <=0} for all time)
     * @return list of {country, region, geoAreaId, lat, lng, count}
     */
    @GetMapping("/visits/markers")
    public List<Map<String, Object>> markers(
            @RequestParam(value = "days", defaultValue = "30") int days) {
        boolean allTime = days <= 0;
        // Aggregate over time at the 1d granularity (fewer rows than 5m).
        String sql =
                "select r.geo_area_id      as \"geoAreaId\", " +
                "       r.geo_level        as \"geoLevel\", " +
                "       r.country          as \"country\", " +
                "       coalesce(a.name, r.country) as \"name\", " +
                "       a.region           as \"region\", " +
                "       a.center_lat       as \"lat\", " +
                "       a.center_lng       as \"lng\", " +
                "       sum(r.event_count) as \"count\" " +
                "from public.geo_time_rollups r " +
                "left join public.geo_areas a on a.geo_area_id = r.geo_area_id " +
                "where r.site_id = ? and r.granularity = '1d' " +
                (allTime ? "" : "  and r.bucket_time >= ? ") +
                "group by r.geo_area_id, r.geo_level, r.country, a.name, a.region, a.center_lat, a.center_lng " +
                "order by sum(r.event_count) desc " +
                "limit 5000";
        if (allTime) {
            return jdbc.queryForList(sql, siteId);
        }
        Instant since = Instant.now().minus(days, ChronoUnit.DAYS);
        return jdbc.queryForList(sql, siteId, java.sql.Timestamp.from(since));
    }

    /**
     * Same data as {@link #markers(int)} but scoped to a lat/lng bounding box
     * and (optionally) a single {@code geo_level} bucket. This powers the
     * "zoom-in re-fetch" behavior on the globe: when the user pans or zooms
     * the camera, the client sends the visible window + a desired level
     * (world / continent / local) and receives only the markers that fall
     * inside it, ranked by event count.
     *
     * <p>The {@code lngMin}/{@code lngMax} pair may wrap the antimeridian
     * (i.e. {@code lngMin > lngMax}); in that case the predicate becomes
     * {@code center_lng >= lngMin OR center_lng <= lngMax}, matching the
     * two halves of the wrapped window.
     *
     * @param days     how far back to roll up (default 30)
     * @param latMin   southern edge of the visible window, degrees [-90, 90]
     * @param latMax   northern edge of the visible window, degrees [-90, 90]
     * @param lngMin   western edge, degrees [-180, 180] (may exceed lngMax to
     *                 indicate an antimeridian-wrapped window)
     * @param lngMax   eastern edge, degrees [-180, 180]
     * @param level    optional zoom bucket: {@code world}/{@code continent}
     *                 map to {@code COUNTRY} rollups, {@code local} maps to
     *                 {@code METRO}. {@code null} or unknown returns all
     *                 levels.
     * @param limit    server-side row cap (default 1600, hard ceiling 5000)
     */
    @GetMapping("/visits/markers/area")
    public List<Map<String, Object>> markersInArea(
            @RequestParam(value = "days", defaultValue = "30") int days,
            @RequestParam("latMin") double latMin,
            @RequestParam("latMax") double latMax,
            @RequestParam("lngMin") double lngMin,
            @RequestParam("lngMax") double lngMax,
            @RequestParam(value = "level", required = false) String level,
            @RequestParam(value = "limit", defaultValue = "1600") int limit) {
        boolean allTime = days <= 0;
        int boundedLimit = Math.min(Math.max(limit, 1), 5000);

        StringBuilder sql = new StringBuilder(
                "select r.geo_area_id      as \"geoAreaId\", " +
                "       r.geo_level        as \"geoLevel\", " +
                "       r.country          as \"country\", " +
                "       coalesce(a.name, r.country) as \"name\", " +
                "       a.region           as \"region\", " +
                "       a.center_lat       as \"lat\", " +
                "       a.center_lng       as \"lng\", " +
                "       sum(r.event_count) as \"count\" " +
                "from public.geo_time_rollups r " +
                "left join public.geo_areas a on a.geo_area_id = r.geo_area_id " +
                "where r.site_id = ? and r.granularity = '1d' "
                + (allTime ? "" : "  and r.bucket_time >= ? ") +
                "  and a.center_lat is not null and a.center_lng is not null " +
                "  and a.center_lat between ? and ? ");
        List<Object> params = new ArrayList<>();
        params.add(siteId);
        if (!allTime) {
            params.add(java.sql.Timestamp.from(Instant.now().minus(days, ChronoUnit.DAYS)));
        }
        params.add(latMin);
        params.add(latMax);

        if (lngMin <= lngMax) {
            sql.append("  and a.center_lng between ? and ? ");
            params.add(lngMin);
            params.add(lngMax);
        } else {
            // Antimeridian wrap: the visible window straddles ±180.
            sql.append("  and (a.center_lng >= ? or a.center_lng <= ?) ");
            params.add(lngMin);
            params.add(lngMax);
        }

        String geoLevel = mapZoomBucketToGeoLevel(level);
        if (geoLevel != null) {
            sql.append("  and r.geo_level = ? ");
            params.add(geoLevel);
        }

        sql.append("group by r.geo_area_id, r.geo_level, r.country, a.name, a.region, a.center_lat, a.center_lng ");
        sql.append("order by sum(r.event_count) desc ");
        sql.append("limit ?");
        params.add(boundedLimit);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    /**
     * Translate the globe's zoom bucket into the matching rollup level.
     * The two coarser buckets share COUNTRY because that is the only level
     * with comprehensive seed data; METRO is reserved for the most zoomed-in
     * view where city-scale pins are useful and privacy-safe.
     */
    private static String mapZoomBucketToGeoLevel(String level) {
        if (level == null) return null;
        switch (level.toLowerCase()) {
            case "world":
            case "continent":
                return "COUNTRY";
            case "local":
                return "METRO";
            default:
                return null;
        }
    }

    /**
     * Topline stats for the {@code /analytics} page.
     * <ul>
     *   <li>{@code totals} — total events, total clicks, total page_views</li>
     *   <li>{@code topCountries} — top 20 countries by event count</li>
     *   <li>{@code topDevices} — split by deviceType</li>
     *   <li>{@code timeSeries} — daily buckets (events/day) over {@code days}</li>
     * </ul>
     */
    @GetMapping("/visits/summary")
    public Map<String, Object> summary(
            @RequestParam(value = "days", defaultValue = "30") int days) {
        boolean allTime = days <= 0;
        String timeFilter = allTime ? "" : "  and bucket_time >= ? ";
        java.sql.Timestamp sinceTs = allTime
                ? null
                : java.sql.Timestamp.from(Instant.now().minus(days, ChronoUnit.DAYS));

        Object[] base = allTime ? new Object[]{siteId} : new Object[]{siteId, sinceTs};

        Map<String, Object> totalsRow = jdbc.queryForMap(
                "select coalesce(sum(event_count), 0) as \"events\", " +
                "       coalesce(sum(event_count) filter (where event_type = 'click'), 0) as \"clicks\", " +
                "       coalesce(sum(event_count) filter (where event_type = 'page_view'), 0) as \"pageViews\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d'" + timeFilter,
                base);

        List<Map<String, Object>> topCountries = jdbc.queryForList(
                "select country as \"country\", sum(event_count) as \"count\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d'" + timeFilter + " and country <> '' " +
                "group by country order by sum(event_count) desc limit 20",
                base);

        List<Map<String, Object>> topDevices = jdbc.queryForList(
                "select device_type as \"deviceType\", sum(event_count) as \"count\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d'" + timeFilter +
                "group by device_type order by sum(event_count) desc",
                base);

        List<Map<String, Object>> timeSeries = jdbc.queryForList(
                "select bucket_time as \"bucketTime\", sum(event_count) as \"count\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d'" + timeFilter +
                "group by bucket_time order by bucket_time",
                base);

        return Map.of(
                "siteId", siteId,
                "days", days,
                "totals", totalsRow,
                "topCountries", topCountries,
                "topDevices", topDevices,
                "timeSeries", timeSeries);
    }
}
