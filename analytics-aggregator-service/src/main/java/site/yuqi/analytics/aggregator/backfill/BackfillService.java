package site.yuqi.analytics.aggregator.backfill;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.RawEvent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Replays the historical {@code visitor_logs} + {@code visitor_clicks}
 * tables (the previous, hand-rolled tracking storage on Supabase) through
 * the same enrichment + rollup pipeline a live Kafka event would take.
 *
 * <p>This is the user-requested workflow: <em>"not just import; aggregator
 * and then import (write code). The data is also on the Supabase database
 * as the truth of result."</em> So we don't COPY rows; we re-synthesise
 * each row as a {@link RawEvent}, enrich it deterministically, then UPSERT
 * to {@code geo_time_rollups}. Re-runs are safe because:
 *
 * <ul>
 *   <li>The synthetic eventId is stable: {@code "vl:" + id} / {@code "vc:" + id}.</li>
 *   <li>{@code RollupUpsertService} uses Postgres {@code ON CONFLICT} semantics
 *       — but we deliberately skip the dedup SETNX here because the per-event
 *       rollup add is small and the rollup itself is overcounted if you replay
 *       the same row twice. Idempotency is achieved by truncating the rollup
 *       table before a fresh backfill (see {@code BackfillRunner#reset}).</li>
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BackfillService {

    private final JdbcTemplate jdbc;
    private final EnrichmentPipeline pipeline;
    private final RollupUpsertService rollup;

    @Value("${analytics.backfill.site-id:yuqi.site}")
    private String siteId;

    @Value("${analytics.backfill.batch-size:500}")
    private int batchSize;

    /**
     * Replay every row in {@code visitor_logs}. Returns the number of
     * rows successfully fed through the pipeline.
     */
    public int backfillVisitorLogs() {
        log.info("{\"event\":\"backfill_start\",\"source\":\"visitor_logs\"}");
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        jdbc.setFetchSize(batchSize);
        ResultSetExtractor<Void> rse = (ResultSet rs) -> {
            while (rs.next()) {
                try {
                    RawEvent raw = rowToVisitorLogEvent(rs);
                    if (raw == null) { bad.incrementAndGet(); continue; }
                    EnrichedEvent enriched = pipeline.enrich(raw);
                    rollup.upsert(enriched);
                    int n = ok.incrementAndGet();
                    if (n % 1000 == 0) {
                        log.info("{\"event\":\"backfill_progress\",\"source\":\"visitor_logs\",\"rows\":{}}", n);
                    }
                } catch (RuntimeException rowErr) {
                    bad.incrementAndGet();
                    log.warn("{\"event\":\"backfill_row_failed\",\"source\":\"visitor_logs\",\"err\":\"{}\"}",
                            rowErr.getMessage());
                }
            }
            return null;
        };
        jdbc.query(
                "select id, ip, local_time, event, ua, country, region, city, " +
                "       latitude, longitude, created_at " +
                "from visitor_logs order by id",
                rse);
        log.info("{\"event\":\"backfill_done\",\"source\":\"visitor_logs\",\"ok\":{},\"bad\":{}}", ok.get(), bad.get());
        return ok.get();
    }

    /** Replay every row in {@code visitor_clicks}. */
    public int backfillVisitorClicks() {
        log.info("{\"event\":\"backfill_start\",\"source\":\"visitor_clicks\"}");
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        jdbc.setFetchSize(batchSize);
        ResultSetExtractor<Void> rse = (ResultSet rs) -> {
            while (rs.next()) {
                try {
                    RawEvent raw = rowToVisitorClickEvent(rs);
                    if (raw == null) { bad.incrementAndGet(); continue; }
                    EnrichedEvent enriched = pipeline.enrich(raw);
                    rollup.upsert(enriched);
                    int n = ok.incrementAndGet();
                    if (n % 1000 == 0) {
                        log.info("{\"event\":\"backfill_progress\",\"source\":\"visitor_clicks\",\"rows\":{}}", n);
                    }
                } catch (RuntimeException rowErr) {
                    bad.incrementAndGet();
                    log.warn("{\"event\":\"backfill_row_failed\",\"source\":\"visitor_clicks\",\"err\":\"{}\"}",
                            rowErr.getMessage());
                }
            }
            return null;
        };
        jdbc.query(
                "select id, click_event, target_url, local_time, ip, event, ua, " +
                "       country, region, city, latitude, longitude, created_at " +
                "from visitor_clicks order by id",
                rse);
        log.info("{\"event\":\"backfill_done\",\"source\":\"visitor_clicks\",\"ok\":{},\"bad\":{}}", ok.get(), bad.get());
        return ok.get();
    }

    private RawEvent rowToVisitorLogEvent(ResultSet rs) throws SQLException {
        long id = rs.getLong("id");
        Instant eventTime = toInstant(rs.getTimestamp("local_time"));
        Instant serverTime = toInstant(rs.getTimestamp("created_at"));
        if (eventTime == null) eventTime = serverTime;
        if (eventTime == null) return null;

        return new RawEvent(
                "vl:" + id,
                siteId,
                "page_view",
                eventTime,
                serverTime != null ? serverTime : eventTime,
                /* sessionId */ null,
                /* anonId */ null,
                /* pageUrl */ null,
                /* targetUrl */ null,
                /* referrer */ null,
                /* uaRaw */ nullable(rs.getString("ua")),
                /* ipRaw */ nullable(rs.getString("ip")),
                buildHint(rs));
    }

    private RawEvent rowToVisitorClickEvent(ResultSet rs) throws SQLException {
        long id = rs.getLong("id");
        Instant eventTime = toInstant(rs.getTimestamp("local_time"));
        Instant serverTime = toInstant(rs.getTimestamp("created_at"));
        if (eventTime == null) eventTime = serverTime;
        if (eventTime == null) return null;

        return new RawEvent(
                "vc:" + id,
                siteId,
                "click",
                eventTime,
                serverTime != null ? serverTime : eventTime,
                /* sessionId */ null,
                /* anonId */ null,
                /* pageUrl */ null,
                nullable(rs.getString("target_url")),
                /* referrer */ null,
                nullable(rs.getString("ua")),
                nullable(rs.getString("ip")),
                buildHint(rs));
    }

    private static GeoHint buildHint(ResultSet rs) throws SQLException {
        String country = nullable(rs.getString("country"));
        String region = nullable(rs.getString("region"));
        String city = nullable(rs.getString("city"));
        Double lat = (Double) rs.getObject("latitude");
        Double lng = (Double) rs.getObject("longitude");
        if (country == null && region == null && city == null && lat == null && lng == null) {
            return null;
        }
        return new GeoHint(country, region, city, lat, lng, "supabase-backfill");
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    private static String nullable(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
