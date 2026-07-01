package site.yuqi.analytics.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Public read endpoints powering the visitor globe and the
 * {@code /analytics} page on the Portfolio dashboard. No auth — this is
 * the same trust posture as the legacy {@code visitor_logs} table the
 * Portfolio used to query directly from Supabase.
 *
 * <p>CORS is permissive on these specific endpoints because Render /
 * Cloud Run and Vercel live on different origins. Everything else on the
 * service is still locked down by the absence of any other public
 * controller + actuator's built-in security.
 *
 * <p>All query endpoints accept a {@code window} parameter
 * ({@code 7d|30d|90d|all}). For backwards compatibility, the older
 * {@code days} parameter is honoured when {@code window} is not given —
 * {@code days=0} or any non-positive value maps to {@code all}.
 */
@RestController
@RequestMapping("/api/public")
@CrossOrigin(origins = "*", allowedHeaders = "*", exposedHeaders = {"ETag"}, methods = {
        org.springframework.web.bind.annotation.RequestMethod.GET,
        org.springframework.web.bind.annotation.RequestMethod.HEAD,
        org.springframework.web.bind.annotation.RequestMethod.OPTIONS
})
@RequiredArgsConstructor
public class PublicVisitsController {

    /** Hard ceilings per level so a misbehaving client can't ask for everything. */
    private static final int LIMIT_COUNTRY = 300;
    private static final int LIMIT_REGION = 1500;
    private static final int LIMIT_METRO = 6000;
    private static final int LIMIT_DEFAULT = 6000;
    private static final int LIMIT_HARD_CEILING = 10000;

    /**
     * Largest viewport (in square degrees) allowed when {@code window=all}.
     * 180° × 360° = 64800 covers the whole globe. We allow up to one
     * hemisphere (~32400) for the page-level "show everything everywhere
     * across all time" view; anything bigger we'd rather force the client
     * to use a narrower window so the join stays small.
     */
    private static final double MAX_BBOX_AREA_ALL = 32_400.0;

    private final JdbcTemplate jdbc;
    private final ResponseCache cache;

    @Value("${analytics.backfill.site-id:yuqi.site}")
    private String siteId;

    /**
     * Per-area counts joined with {@code geo_areas.center_lat/lng} so the
     * globe can place a marker. METRO buckets win over their COUNTRY
     * parents when present.
     *
     * <p>This single endpoint covers three use cases the dashboard needs:
     * <ul>
     *   <li>Page-level "All time" snapshot ({@code window=all}, no bbox).</li>
     *   <li>Sliding-window stats ({@code window=7d|30d|90d}, no bbox).</li>
     *   <li>Viewport-aware re-fetch when the globe pans/zooms — pass
     *       bbox + {@code level} and we return only what's visible.</li>
     * </ul>
     *
     * <p>Rows whose centroid is unknown ({@code center_lat IS NULL}) are
     * filtered out at the SQL layer because the client can't render them
     * anyway, and serving NULLs in the JSON only inflates the payload.
     *
     * @param window  time window: {@code 7d}, {@code 30d}, {@code 90d}, or
     *                {@code all}. Default {@code 30d}.
     * @param days    legacy alias for {@code window}; pass {@code 0} for
     *                all-time. Ignored when {@code window} is provided.
     * @param level   optional LOD filter: {@code COUNTRY|REGION|METRO} (or
     *                the older zoom-bucket aliases {@code world|continent|local}).
     * @param latMin  southern edge of the viewport, degrees [-89.9, 89.9].
     * @param latMax  northern edge of the viewport, degrees [-89.9, 89.9].
     * @param lngMin  western edge, degrees [-180, 180] (may exceed {@code lngMax}
     *                to indicate an antimeridian-wrapped window).
     * @param lngMax  eastern edge, degrees [-180, 180].
     * @param limit   server-side row cap; defaults to a per-level cap if omitted.
     */
    @GetMapping("/visits/markers")
    public ResponseEntity<?> markers(
            @RequestParam(value = "window", required = false) String window,
            @RequestParam(value = "days", required = false) Integer days,
            @RequestParam(value = "level", required = false) String level,
            @RequestParam(value = "latMin", required = false) Double latMin,
            @RequestParam(value = "latMax", required = false) Double latMax,
            @RequestParam(value = "lngMin", required = false) Double lngMin,
            @RequestParam(value = "lngMax", required = false) Double lngMax,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {

        WindowSpec ws = resolveWindow(window, days);
        String geoLevel = normalizeLevel(level);
        BBox bbox = BBox.maybe(latMin, latMax, lngMin, lngMax);

        if (ws.allTime && bbox != null && bbox.area() > MAX_BBOX_AREA_ALL) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "bbox too large for window=all; narrow the window or shrink the viewport");
        }

        int boundedLimit = clampLimit(limit, defaultLimitFor(geoLevel));

        String cacheKey = "pub:markers:" + ws.label + ":" + geoLevel + ":" + boundedLimit
                + (bbox != null ? ":" + bbox.minLat + ":" + bbox.maxLat + ":" + bbox.minLng + ":" + bbox.maxLng : "");

        ResponseCache.CacheEntry hit = cache.get(cacheKey);
        if (hit != null) {
            if (hit.etag().equals(ifNoneMatch)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED)
                        .header("ETag", hit.etag()).build();
            }
            return ResponseEntity.ok()
                    .header("ETag", hit.etag())
                    .header("Content-Type", "application/json").body(hit.json());
        }

        List<Map<String, Object>> result = runMarkersQuery(ws, geoLevel, bbox, boundedLimit);
        String etag = cache.put(cacheKey, result);
        return ResponseEntity.ok().header("ETag", etag).body(result);
    }

    /**
     * Backwards-compatible alias for the older viewport endpoint the
     * deployed frontend still calls. New clients should use
     * {@link #markers(String, Integer, String, Double, Double, Double, Double, Integer)}
     * — this thin wrapper delegates to the same query path.
     */
    @GetMapping("/visits/markers/area")
    public ResponseEntity<?> markersInArea(
            @RequestParam(value = "window", required = false) String window,
            @RequestParam(value = "days", required = false) Integer days,
            @RequestParam("latMin") double latMin,
            @RequestParam("latMax") double latMax,
            @RequestParam("lngMin") double lngMin,
            @RequestParam("lngMax") double lngMax,
            @RequestParam(value = "level", required = false) String level,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {
        return markers(window, days, level, latMin, latMax, lngMin, lngMax, limit, ifNoneMatch);
    }

    /**
     * Topline stats for the {@code /analytics} page.
     * <ul>
     *   <li>{@code totals} — total events, total clicks, total page_views</li>
     *   <li>{@code topCountries} — top 20 countries by event count</li>
     *   <li>{@code topDevices} — split by deviceType</li>
     *   <li>{@code timeSeries} — daily buckets (events/day)</li>
     * </ul>
     */
    @GetMapping("/visits/summary")
    public ResponseEntity<?> summary(
            @RequestParam(value = "window", required = false) String window,
            @RequestParam(value = "days", required = false) Integer days,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {

        WindowSpec ws = resolveWindow(window, days);
        String cacheKey = "pub:summary:" + ws.label;

        ResponseCache.CacheEntry hit = cache.get(cacheKey);
        if (hit != null) {
            if (hit.etag().equals(ifNoneMatch)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED)
                        .header("ETag", hit.etag()).build();
            }
            return ResponseEntity.ok()
                    .header("ETag", hit.etag())
                    .header("Content-Type", "application/json").body(hit.json());
        }

        String timeFilter = ws.allTime ? "" : "  and bucket_time >= ? ";
        Object[] base = ws.allTime ? new Object[]{siteId} : new Object[]{siteId, ws.timestampSince()};

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

        Map<String, Object> result = Map.of(
                "siteId", siteId,
                "window", ws.label,
                "days", ws.legacyDays,
                "totals", totalsRow,
                "topCountries", topCountries,
                "topDevices", topDevices,
                "timeSeries", timeSeries);

        String etag = cache.put(cacheKey, result);
        return ResponseEntity.ok().header("ETag", etag).body(result);
    }

    // ────────────────────────────────────────────────────────────────────
    // Session + funnel endpoints
    // ────────────────────────────────────────────────────────────────────

    /**
     * Session stats: total sessions, avg duration, bounce rate.
     */
    @GetMapping("/visits/sessions")
    public ResponseEntity<?> sessionStats(
            @RequestParam(value = "window", required = false) String window,
            @RequestParam(value = "days", required = false) Integer days,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {

        WindowSpec ws = resolveWindow(window, days);
        String cacheKey = "pub:sessions:" + ws.label;

        ResponseCache.CacheEntry hit = cache.get(cacheKey);
        if (hit != null) {
            if (hit.etag().equals(ifNoneMatch)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED)
                        .header("ETag", hit.etag()).build();
            }
            return ResponseEntity.ok()
                    .header("ETag", hit.etag())
                    .header("Content-Type", "application/json").body(hit.json());
        }

        String timeFilter = ws.allTime ? "" : " AND first_event >= ? ";
        Object[] params = ws.allTime ? new Object[]{siteId} : new Object[]{siteId, ws.timestampSince()};

        Map<String, Object> stats = jdbc.queryForMap(
                "SELECT count(*) as \"totalSessions\", " +
                "       coalesce(avg(duration_ms), 0) as \"avgDurationMs\", " +
                "       coalesce(sum(CASE WHEN page_views <= 1 THEN 1 ELSE 0 END)::float / " +
                "         NULLIF(count(*), 0), 0) as \"bounceRate\" " +
                "FROM public.sessions WHERE site_id = ?" + timeFilter, params);

        List<Map<String, Object>> topEntry = jdbc.queryForList(
                "SELECT entry_page as \"page\", count(*) as \"count\" " +
                "FROM public.sessions WHERE site_id = ?" + timeFilter +
                " AND entry_page IS NOT NULL " +
                "GROUP BY entry_page ORDER BY count(*) DESC LIMIT 10", params);

        Map<String, Object> result = Map.of(
                "stats", stats,
                "topEntryPages", topEntry,
                "window", ws.label);

        String etag = cache.put(cacheKey, result);
        return ResponseEntity.ok().header("ETag", etag).body(result);
    }

    /**
     * Funnel analysis: step-by-step conversion counts.
     */
    @GetMapping("/visits/funnel")
    public ResponseEntity<?> funnel(
            @RequestParam(value = "window", required = false) String window,
            @RequestParam(value = "days", required = false) Integer days,
            @RequestHeader(value = "If-None-Match", required = false) String ifNoneMatch) {

        WindowSpec ws = resolveWindow(window, days);
        String cacheKey = "pub:funnel:" + ws.label;

        ResponseCache.CacheEntry hit = cache.get(cacheKey);
        if (hit != null) {
            if (hit.etag().equals(ifNoneMatch)) {
                return ResponseEntity.status(HttpStatus.NOT_MODIFIED)
                        .header("ETag", hit.etag()).build();
            }
            return ResponseEntity.ok()
                    .header("ETag", hit.etag())
                    .header("Content-Type", "application/json").body(hit.json());
        }

        String timeFilter = ws.allTime ? "" : " AND event_time >= ? ";
        Object[] params = ws.allTime ? new Object[]{siteId} : new Object[]{siteId, ws.timestampSince()};

        List<Map<String, Object>> steps = jdbc.queryForList(
                "SELECT step_name as \"step\", count(DISTINCT session_id) as \"sessions\" " +
                "FROM public.funnel_steps WHERE site_id = ?" + timeFilter +
                " GROUP BY step_name ORDER BY count(DISTINCT session_id) DESC LIMIT 20", params);

        Map<String, Object> result = Map.of("steps", steps, "window", ws.label);

        String etag = cache.put(cacheKey, result);
        return ResponseEntity.ok().header("ETag", etag).body(result);
    }

    // ────────────────────────────────────────────────────────────────────
    // Query implementation
    // ────────────────────────────────────────────────────────────────────

    /**
     * One markers query shared by both the page-level and viewport-scoped
     * endpoints. We INNER JOIN (not LEFT JOIN) because a missing centroid
     * is unusable for the globe; the legacy LEFT JOIN was returning
     * null-lat rows that the client just had to throw away.
     */
    private List<Map<String, Object>> runMarkersQuery(
            WindowSpec ws, String geoLevel, BBox bbox, int limit) {

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
                "join public.geo_areas a on a.geo_area_id = r.geo_area_id " +
                "where r.site_id = ? and r.granularity = '1d' " +
                "  and a.center_lat is not null and a.center_lng is not null ");

        List<Object> params = new ArrayList<>();
        params.add(siteId);
        if (!ws.allTime) {
            sql.append("  and r.bucket_time >= ? ");
            params.add(ws.timestampSince());
        }
        if (geoLevel != null) {
            sql.append("  and r.geo_level = ? ");
            params.add(geoLevel);
        }
        if (bbox != null) {
            sql.append("  and a.center_lat between ? and ? ");
            params.add(bbox.minLat);
            params.add(bbox.maxLat);
            if (bbox.minLng <= bbox.maxLng) {
                sql.append("  and a.center_lng between ? and ? ");
                params.add(bbox.minLng);
                params.add(bbox.maxLng);
            } else {
                // Antimeridian wrap: viewport straddles ±180°.
                sql.append("  and (a.center_lng >= ? or a.center_lng <= ?) ");
                params.add(bbox.minLng);
                params.add(bbox.maxLng);
            }
        }

        sql.append("group by r.geo_area_id, r.geo_level, r.country, a.name, a.region, a.center_lat, a.center_lng ");
        sql.append("order by sum(r.event_count) desc ");
        sql.append("limit ?");
        params.add(limit);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    // ────────────────────────────────────────────────────────────────────
    // Parameter normalisation
    // ────────────────────────────────────────────────────────────────────

    /** Resolves {@code window} (preferred) / {@code days} (legacy) into a window spec. */
    private static WindowSpec resolveWindow(String window, Integer days) {
        if (window != null && !window.isBlank()) {
            String w = window.trim().toLowerCase();
            switch (w) {
                case "7d":  return new WindowSpec(false, 7, "7d");
                case "30d": return new WindowSpec(false, 30, "30d");
                case "90d": return new WindowSpec(false, 90, "90d");
                case "all": return new WindowSpec(true, 0, "all");
                default:
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                            "unknown window: " + window + " (allowed: 7d, 30d, 90d, all)");
            }
        }
        if (days != null) {
            if (days <= 0) return new WindowSpec(true, 0, "all");
            return new WindowSpec(false, days, days + "d");
        }
        // Neither given — default to 30d, matching the legacy endpoint behaviour.
        return new WindowSpec(false, 30, "30d");
    }

    /**
     * Normalise an externally-provided level to one of COUNTRY / REGION /
     * METRO. Accepts both literal level names and the older zoom-bucket
     * aliases ({@code world}, {@code continent}, {@code local}). Rejects
     * GLOBAL — that level has no per-area pin to render and asking for it
     * would just return the bucket-of-everything row.
     */
    private static String normalizeLevel(String level) {
        if (level == null || level.isBlank()) return null;
        String l = level.trim().toUpperCase();
        switch (l) {
            case "COUNTRY":
            case "WORLD":
            case "CONTINENT":
                return "COUNTRY";
            case "REGION":
                return "REGION";
            case "METRO":
            case "LOCAL":
                return "METRO";
            case "GLOBAL":
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "level=GLOBAL is not supported; use COUNTRY, REGION, or METRO");
            default:
                // Unknown bucket → null lets the query return all levels.
                return null;
        }
    }

    private static int defaultLimitFor(String geoLevel) {
        if (geoLevel == null) return LIMIT_DEFAULT;
        switch (geoLevel) {
            case "COUNTRY": return LIMIT_COUNTRY;
            case "REGION":  return LIMIT_REGION;
            case "METRO":   return LIMIT_METRO;
            default:        return LIMIT_DEFAULT;
        }
    }

    private static int clampLimit(Integer requested, int fallback) {
        int n = requested == null ? fallback : requested;
        if (n < 1) n = 1;
        if (n > LIMIT_HARD_CEILING) n = LIMIT_HARD_CEILING;
        return n;
    }

    // ────────────────────────────────────────────────────────────────────
    // Value types
    // ────────────────────────────────────────────────────────────────────

    /** Resolved time window. {@code allTime=true} means no lower bound. */
    private record WindowSpec(boolean allTime, int legacyDays, String label) {
        java.sql.Timestamp timestampSince() {
            return java.sql.Timestamp.from(Instant.now().minus(legacyDays, ChronoUnit.DAYS));
        }
    }

    /**
     * Validated lat/lng bounding box. lat is clamped to ±89.9° to dodge
     * pole-induced edge cases. lng is normalised to [-180, 180]; if
     * {@code minLng > maxLng} after normalisation we treat it as an
     * antimeridian wrap (the SQL handles both shapes).
     */
    private record BBox(double minLat, double maxLat, double minLng, double maxLng) {
        static BBox maybe(Double minLat, Double maxLat, Double minLng, Double maxLng) {
            int given = (minLat != null ? 1 : 0) + (maxLat != null ? 1 : 0)
                      + (minLng != null ? 1 : 0) + (maxLng != null ? 1 : 0);
            if (given == 0) return null;
            if (given != 4
                    || minLat == null || maxLat == null
                    || minLng == null || maxLng == null) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "bbox requires all four of minLat/maxLat/minLng/maxLng");
            }
            double laMin = clampLat(Math.min(minLat.doubleValue(), maxLat.doubleValue()));
            double laMax = clampLat(Math.max(minLat.doubleValue(), maxLat.doubleValue()));
            double lnMin = normalizeLng(minLng.doubleValue());
            double lnMax = normalizeLng(maxLng.doubleValue());
            return new BBox(laMin, laMax, lnMin, lnMax);
        }

        /** Approximate area in square degrees; only used for the size guard. */
        double area() {
            double lat = Math.max(0.0, maxLat - minLat);
            double lng = minLng <= maxLng
                    ? Math.max(0.0, maxLng - minLng)
                    // wrap: take the two halves
                    : (180.0 - minLng) + (maxLng + 180.0);
            return lat * lng;
        }

        private static double clampLat(double v) {
            if (v < -89.9) return -89.9;
            if (v > 89.9) return 89.9;
            return v;
        }

        private static double normalizeLng(double v) {
            double x = ((v + 180.0) % 360.0 + 360.0) % 360.0 - 180.0;
            return x;
        }
    }
}
