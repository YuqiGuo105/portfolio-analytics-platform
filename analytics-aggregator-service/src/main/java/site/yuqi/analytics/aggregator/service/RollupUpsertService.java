package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.Granularity;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashMap;

/**
 * UPSERTs enriched events into {@code geo_time_rollups} at 5m and 1d granularity.
 *
 * <p><b>Single-event path</b> ({@link #upsert}): two SQL round-trips per event.
 * Used by {@code BackfillService} which drives events one at a time.
 *
 * <p><b>Batch path</b> ({@link #upsertBatch}): pre-aggregates the list in memory
 * with a {@code Map<RollupKey, counters>} so that events sharing the same natural
 * key within a single Kafka poll are collapsed before they reach the database.
 * This reduces DB write QPS from (N × 2) per event to at most (unique_keys × 2)
 * per batch — typically much lower since many events share the same
 * geo/device/bucket. A single {@code jdbc.batchUpdate} per granularity then
 * fires the consolidated UPSERT rows.
 *
 * <p>The durable Kafka inbox prevents the same event from reaching this
 * additive projection twice. Every event contributes to all available geo
 * ancestors so REGION and COUNTRY alert rules can match METRO events.
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
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            on conflict (site_id, bucket_time, granularity, geo_level, geo_area_id,
                         event_type, device_type, browser, os, is_bot, country)
            do update set
                event_count     = geo_time_rollups.event_count + excluded.event_count,
                unique_sessions = geo_time_rollups.unique_sessions + excluded.unique_sessions,
                updated_at      = now()
            """;

    private final JdbcTemplate jdbc;
    private final DurableUniqueSessionCounter sessionCounter;

    // ------------------------------------------------------------------ single

    /** Single-event upsert kept for BackfillService. Two DB round-trips. */
    @Transactional
    public void upsert(EnrichedEvent e) {
        Objects.requireNonNull(e, "enriched event must not be null");
        Instant ts = e.eventTime() == null ? Instant.now() : e.eventTime();
        String sessionKey = resolveSessionKey(e);
        upsertAtGranularity(e, ts, Granularity.FIVE_MIN, sessionKey);
        upsertAtGranularity(e, ts, Granularity.ONE_DAY, sessionKey);
    }

    // ------------------------------------------------------------------ batch

    /**
     * Accepts a batch of enriched events, pre-aggregates identical natural keys
     * in memory, then issues one {@code batchUpdate} per granularity tier.
     * The whole operation runs inside a single transaction.
     *
     * @param events non-null, non-empty; items may not be null.
     */
    @Transactional
    public void upsertBatch(List<EnrichedEvent> events) {
        Objects.requireNonNull(events, "events must not be null");
        if (events.isEmpty()) return;

        Map<RollupKey, long[]> fiveMin = new HashMap<>();
        Map<RollupKey, long[]> oneDay  = new HashMap<>();

        for (EnrichedEvent e : events) {
            if (e == null) continue;
            Instant ts = e.eventTime() == null ? Instant.now() : e.eventTime();
            String sessionKey = resolveSessionKey(e);
            accumulate(fiveMin, e, Granularity.FIVE_MIN, ts, sessionKey);
            accumulate(oneDay,  e, Granularity.ONE_DAY,  ts, sessionKey);
        }

        flushGranularity(fiveMin, Granularity.FIVE_MIN);
        flushGranularity(oneDay,  Granularity.ONE_DAY);
    }

    // ---------------------------------------------------------------- helpers

    private void accumulate(Map<RollupKey, long[]> acc,
                            EnrichedEvent e, Granularity g, Instant ts, String sessionKey) {
        for (RollupKey key : keysFor(e, g, ts)) {
            long[] counts = acc.computeIfAbsent(key, k -> new long[]{0L, 0L});
            counts[0] += 1;
            counts[1] += sessionCounter.addAndDelta(
                    key.siteId, key.bucketTime, key.granularity,
                    key.geoLevel, key.geoAreaId, key.eventType, sessionKey);
        }
    }

    private void flushGranularity(Map<RollupKey, long[]> acc, Granularity g) {
        if (acc.isEmpty()) return;
        List<Object[]> rows = new ArrayList<>(acc.size());
        for (Map.Entry<RollupKey, long[]> entry : acc.entrySet()) {
            RollupKey k = entry.getKey();
            long[] c    = entry.getValue();
            rows.add(new Object[]{
                    k.siteId, k.bucketTime, k.granularity,
                    k.geoLevel, k.geoAreaId,
                    k.eventType, k.deviceType, k.browser, k.os,
                    k.isBot, k.country,
                    c[0], c[1]   // event_count, unique_sessions
            });
        }
        jdbc.batchUpdate(UPSERT_SQL, rows);
    }

    private void upsertAtGranularity(EnrichedEvent e, Instant ts,
                                     Granularity g, String sessionKey) {
        for (RollupKey k : keysFor(e, g, ts)) {
            long sessions = sessionCounter.addAndDelta(
                    k.siteId, k.bucketTime, k.granularity,
                    k.geoLevel, k.geoAreaId, k.eventType, sessionKey);
            jdbc.update(UPSERT_SQL,
                    k.siteId, k.bucketTime, k.granularity,
                    k.geoLevel, k.geoAreaId,
                    k.eventType, k.deviceType, k.browser, k.os,
                    k.isBot, k.country,
                    1L, sessions);
        }
    }

    private static List<RollupKey> keysFor(EnrichedEvent e, Granularity g, Instant ts) {
        Map<String, RollupKey> keys = new LinkedHashMap<>();
        addKey(keys, e, g, ts, "GLOBAL", "GLOBAL");

        EnrichedGeo geo = e.geo();
        if (geo != null && notBlank(geo.country())) {
            addKey(keys, e, g, ts, "COUNTRY", "COUNTRY:" + geo.country());
            if (notBlank(geo.region())) {
                addKey(keys, e, g, ts, "REGION", "REGION:" + geo.country() + ":" + geo.region());
            }
            if (notBlank(geo.metro())) {
                addKey(keys, e, g, ts, "METRO",
                        "METRO:" + geo.country() + ":" + (geo.region() == null ? "" : geo.region())
                                + ":" + geo.metro());
            }
        }
        if (geo != null && notBlank(geo.geoAreaId())) {
            addKey(keys, e, g, ts, geo.geoLevel().name(), geo.geoAreaId());
        }
        return List.copyOf(keys.values());
    }

    private static void addKey(Map<String, RollupKey> keys, EnrichedEvent e,
                               Granularity g, Instant ts, String geoLevel, String geoAreaId) {
        keys.putIfAbsent(geoLevel + "|" + geoAreaId, keyFor(e, g, ts, geoLevel, geoAreaId));
    }

    private static RollupKey keyFor(EnrichedEvent e, Granularity g, Instant ts,
                                    String geoLevel, String geoAreaId) {
        return new RollupKey(
                e.siteId(),
                Timestamp.from(g.floor(ts)),
                g.code(),
                geoLevel,
                geoAreaId,
                e.eventType(),
                nz(e.deviceType()),
                nz(e.browser()),
                nz(e.os()),
                e.bot(),
                e.geo() == null || e.geo().country() == null ? "" : e.geo().country()
        );
    }

    /**
     * 解析最佳会话身份标识：sessionId > anonId > ipHash + 设备类型。
     *
     * <p>Schema-v2 前端发送稳定的 anonId 与 30 分钟 sessionId。旧事件、
     * 拒绝持久标识的请求以及 backfill 仍会 fallback 到 ipHash + device。
     *
     * <p>粒度选择：按 deviceType 区分，不按 browser/os 区分。
     * 即：电脑访问 + 手机访问 = 2 个 unique session；
     * 手机上换浏览器仍算 1 个 unique session（同一设备）。
     *
     * <p>Fallback 只是历史兼容，新的行为分析不依赖 IP 作为主身份。
     */
    private static String resolveSessionKey(EnrichedEvent e) {
        if (e.sessionId() != null && !e.sessionId().isBlank()) return e.sessionId();
        if (e.anonId() != null && !e.anonId().isBlank()) return e.anonId();
        // fallback: ipHash + 设备类型，让同 IP 不同设备（电脑/手机/平板）算作不同 unique visitor
        // 同一设备换浏览器不额外计数
        return e.ipHash() + ":" + nz(e.deviceType());
    }

    private static String nz(String s) {
        return (s == null || s.isBlank()) ? "unknown" : s;
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    // ------------------------------------------------------------------ key

    /**
     * Value object representing the 11-column natural key of {@code geo_time_rollups}.
     * Used as a {@code HashMap} key during in-memory pre-aggregation.
     */
    private record RollupKey(
            String siteId,
            Timestamp bucketTime,
            String granularity,
            String geoLevel,
            String geoAreaId,
            String eventType,
            String deviceType,
            String browser,
            String os,
            boolean isBot,
            String country) {}
}
