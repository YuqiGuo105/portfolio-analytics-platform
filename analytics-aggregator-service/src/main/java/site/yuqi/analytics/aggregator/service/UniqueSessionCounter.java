package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.HyperLogLogOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Valkey-backed HyperLogLog to produce real unique-session deltas for
 * {@link RollupUpsertService}.
 *
 * <p>One HLL key per (siteId, granularity, bucketEpoch, geoLevel, geoAreaId,
 * eventType). Device/browser/os are intentionally excluded from the HLL
 * <b>key</b> to keep the total key count within Valkey free-tier memory
 * (~12 KB per HLL). However, device type IS reflected in the
 * <b>session key value</b> that is PFADD'd into the HLL: when the caller
 * (RollupUpsertService) falls back to ipHash, it appends the device type
 * (desktop/mobile/tablet), so the same IP on different device types
 * counts as distinct unique sessions. Same device type with different
 * browsers still counts as one session (e.g. phone Chrome + phone Safari = 1).
 *
 * <p>TTL policy: 5m-granularity keys expire after 2 hours; 1d keys after
 * 48 hours. The HLL only needs to survive the bucket's active-write window.
 * After expiry, the additive count stored in Postgres is authoritative.
 *
 * <p><b>Fail-open</b>: on any Redis/Valkey error, logs a warning and
 * returns 0 (never inflates unique_sessions, never blocks ingestion).
 * Consistent with {@link site.yuqi.analytics.aggregator.enrich.DedupService}.
 *
 * <p><b>Redelivery safety</b>: a redelivered event's eventId is caught by
 * {@code DedupService.acquire} upstream before reaching the rollup batch.
 * Even if it slips through, PFADD of an already-counted session yields
 * delta 0 — so unique_sessions is never double-counted.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class UniqueSessionCounter {

    private static final String KEY_PREFIX = "hll:uniq:";
    private static final Duration TTL_5M = Duration.ofHours(2);
    private static final Duration TTL_1D = Duration.ofHours(48);

    private final StringRedisTemplate redis;

    /**
     * Adds the given session key to the HLL for the specified bucket and
     * returns the marginal new-unique contribution (0 or 1 in most cases,
     * but can be higher if the HLL was empty and we're the first caller).
     *
     * @param siteId     site identifier (e.g. "yuqi.site")
     * @param granularity "5m" or "1d"
     * @param bucketEpoch epoch seconds of the bucket start
     * @param geoLevel   "GLOBAL", "COUNTRY", "REGION", or "METRO"
     * @param geoAreaId  geo area identifier
     * @param eventType  "page_view", "click", etc.
     * @param sessionKey the session identifier to count (sessionId, anonId, or ipHash)
     * @return the HLL delta (0 = already seen, ≥1 = newly unique)
     */
    public long addAndDelta(String siteId, String granularity, long bucketEpoch,
                            String geoLevel, String geoAreaId, String eventType,
                            String sessionKey) {
        if (sessionKey == null || sessionKey.isBlank()) return 0;
        String key = KEY_PREFIX + siteId + ":" + granularity + ":" + bucketEpoch
                + ":" + geoLevel + ":" + geoAreaId + ":" + eventType;
        try {
            HyperLogLogOperations<String, String> hll = redis.opsForHyperLogLog();
            Long before = hll.size(key);
            hll.add(key, sessionKey);
            Long after = hll.size(key);

            // Set TTL on first touch (before == 0 means key was just created).
            if (before != null && before == 0L) {
                Duration ttl = "1d".equals(granularity) ? TTL_1D : TTL_5M;
                redis.expire(key, ttl);
            }

            long delta = (after != null ? after : 0L) - (before != null ? before : 0L);
            return Math.max(0L, delta);
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"hll_error\",\"key\":\"{}\",\"err\":\"{}\"}", key, e.getMessage());
            return 0; // fail-open: never inflate, never block
        }
    }
}
