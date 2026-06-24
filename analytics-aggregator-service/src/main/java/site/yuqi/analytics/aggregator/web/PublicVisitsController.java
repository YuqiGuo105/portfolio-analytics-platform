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
     * @param days how far back to roll up (default 30)
     * @return list of {country, region, geoAreaId, lat, lng, count}
     */
    @GetMapping("/visits/markers")
    public List<Map<String, Object>> markers(
            @RequestParam(value = "days", defaultValue = "30") int days) {
        Instant since = Instant.now().minus(days, ChronoUnit.DAYS);
        // Aggregate over time at the 1d granularity (fewer rows than 5m).
        return jdbc.queryForList(
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
                "where r.site_id = ? and r.granularity = '1d' and r.bucket_time >= ? " +
                "group by r.geo_area_id, r.geo_level, r.country, a.name, a.region, a.center_lat, a.center_lng " +
                "order by sum(r.event_count) desc " +
                "limit 5000",
                siteId, java.sql.Timestamp.from(since));
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
        Instant since = Instant.now().minus(days, ChronoUnit.DAYS);
        java.sql.Timestamp sinceTs = java.sql.Timestamp.from(since);

        Map<String, Object> totalsRow = jdbc.queryForMap(
                "select coalesce(sum(event_count), 0) as \"events\", " +
                "       coalesce(sum(event_count) filter (where event_type = 'click'), 0) as \"clicks\", " +
                "       coalesce(sum(event_count) filter (where event_type = 'page_view'), 0) as \"pageViews\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d' and bucket_time >= ?",
                siteId, sinceTs);

        List<Map<String, Object>> topCountries = jdbc.queryForList(
                "select country as \"country\", sum(event_count) as \"count\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d' and bucket_time >= ? and country <> '' " +
                "group by country order by sum(event_count) desc limit 20",
                siteId, sinceTs);

        List<Map<String, Object>> topDevices = jdbc.queryForList(
                "select device_type as \"deviceType\", sum(event_count) as \"count\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d' and bucket_time >= ? " +
                "group by device_type order by sum(event_count) desc",
                siteId, sinceTs);

        List<Map<String, Object>> timeSeries = jdbc.queryForList(
                "select bucket_time as \"bucketTime\", sum(event_count) as \"count\" " +
                "from public.geo_time_rollups " +
                "where site_id = ? and granularity = '1d' and bucket_time >= ? " +
                "group by bucket_time order by bucket_time",
                siteId, sinceTs);

        return Map.of(
                "siteId", siteId,
                "days", days,
                "totals", totalsRow,
                "topCountries", topCountries,
                "topDevices", topDevices,
                "timeSeries", timeSeries);
    }
}
