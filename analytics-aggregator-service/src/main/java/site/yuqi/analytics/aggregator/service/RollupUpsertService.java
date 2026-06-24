package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.Granularity;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;

/**
 * UPSERTs one enriched event into the {@code geo_time_rollups} table at
 * both 5m and 1d granularity. ON CONFLICT updates {@code event_count} and
 * {@code unique_sessions} in-place — Postgres takes care of the math so we
 * stay correct under at-least-once redelivery (the {@code event_id} dedup
 * upstream is what guarantees we don't double-count the same event).
 */
@Service
@RequiredArgsConstructor
public class RollupUpsertService {

    private static final String UPSERT_SQL = """
            insert into geo_time_rollups
                (site_id, bucket_time, granularity, geo_level, geo_area_id,
                 event_type, device_type, browser, os, is_bot, country,
                 event_count, unique_sessions, updated_at)
            values
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, now())
            on conflict (site_id, bucket_time, granularity, geo_level, geo_area_id,
                         event_type, device_type, browser, os, is_bot, country)
            do update set
                event_count     = geo_time_rollups.event_count + excluded.event_count,
                unique_sessions = geo_time_rollups.unique_sessions + excluded.unique_sessions,
                updated_at      = now()
            """;

    private final JdbcTemplate jdbc;

    @Transactional
    public void upsert(EnrichedEvent e) {
        Objects.requireNonNull(e, "enriched event must not be null");
        Instant ts = e.eventTime() == null ? Instant.now() : e.eventTime();

        upsertAtGranularity(e, ts, Granularity.FIVE_MIN);
        upsertAtGranularity(e, ts, Granularity.ONE_DAY);
    }

    private void upsertAtGranularity(EnrichedEvent e, Instant ts, Granularity g) {
        Timestamp bucket = Timestamp.from(g.floor(ts));
        jdbc.update(UPSERT_SQL,
                e.siteId(),
                bucket,
                g.code(),
                e.geo() == null ? "GLOBAL" : e.geo().geoLevel().name(),
                e.geo() == null ? "GLOBAL" : e.geo().geoAreaId(),
                e.eventType(),
                nz(e.deviceType()),
                nz(e.browser()),
                nz(e.os()),
                e.bot(),
                e.geo() == null || e.geo().country() == null ? "" : e.geo().country());
    }

    private static String nz(String s) {
        return (s == null || s.isBlank()) ? "unknown" : s;
    }
}
