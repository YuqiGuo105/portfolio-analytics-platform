package site.yuqi.analytics.aggregator.enrich;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * At-least-once Kafka semantics + this {@code SETNX} guard = effectively-once
 * enrichment. If two consumers race on the same {@code event_id}, only the
 * one that wins the SETNX runs the side effects; the other skips.
 *
 * <p>Falls open on Redis errors — we'd rather risk one duplicate enriched
 * event than block ingestion when Valkey is briefly down.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DedupService {

    private static final String KEY_PREFIX = "analytics:eid:";

    private final StringRedisTemplate redis;

    @Value("${analytics.dedup.ttl-seconds:86400}")
    private long ttlSeconds;

    @Value("${analytics.dedup.throttle-seconds:300}")
    private long throttleSeconds;

    /** @return {@code true} when this is the first time we've seen {@code eventId}. */
    public boolean acquire(String eventId) {
        if (eventId == null || eventId.isBlank()) return true;
        try {
            Boolean ok = redis.opsForValue().setIfAbsent(
                    KEY_PREFIX + eventId, "1", Duration.ofSeconds(ttlSeconds));
            if (ok == null) {
                // Lettuce can return null when connection pool is in a degraded
                // state (pipeline/transaction context). Treat as fail-open.
                log.warn("{\"event\":\"dedup_setnx_null\",\"eventId\":\"{}\"}", eventId);
                return true;
            }
            if (!ok) {
                log.debug("{\"event\":\"dedup_hit\",\"eventId\":\"{}\"}", eventId);
            }
            return ok;
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"dedup_redis_error\",\"err\":\"{}\"}", e.getMessage());
            return true; // fail-open
        }
    }

    /**
     * Visit throttle: same session + same page within {@code throttleSeconds}
     * (default 5 min). Called once per event in {@code RawEventConsumer} so
     * throttled refreshes are dropped before reaching both {@code visitor_logs}
     * AND {@code geo_time_rollups} — ensuring the dashboard TODAY count does
     * not increment on rapid page refreshes.
     *
     * @return {@code true} if this is a <b>new</b> visit (count it);
     *         {@code false} if it is a refresh within the throttle window (skip it).
     */
    public boolean throttleVisit(String sessionKey, String pageUrl) {
        if (sessionKey == null || sessionKey.isBlank()) return true;
        if (pageUrl == null || pageUrl.isBlank()) return true;
        String key = "analytics:throttle:" + sessionKey + ":" + pageUrl;
        try {
            Boolean ok = redis.opsForValue().setIfAbsent(
                    key, "1", Duration.ofSeconds(throttleSeconds));
            if (ok == null) {
                log.warn("{\"event\":\"throttle_setnx_null\",\"sessionKey\":\"{}\",\"page\":\"{}\"}", sessionKey, pageUrl);
                return true; // fail-open
            }
            if (!ok) {
                log.debug("{\"event\":\"throttle_hit\",\"sessionKey\":\"{}\",\"page\":\"{}\"}", sessionKey, pageUrl);
            }
            return ok;
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"throttle_redis_error\",\"err\":\"{}\"}", e.getMessage());
            return true; // fail-open
        }
    }

    /**
     * Convenience overload: resolves the session key from an {@link site.yuqi.analytics.common.event.EnrichedEvent}
     * and delegates to {@link #throttleVisit(String, String)}.
     * Priority: sessionId > anonId > ipHash:deviceType.
     */
    public boolean throttleVisit(site.yuqi.analytics.common.event.EnrichedEvent e) {
        String sessionKey;
        if (e.sessionId() != null && !e.sessionId().isBlank()) {
            sessionKey = e.sessionId();
        } else if (e.anonId() != null && !e.anonId().isBlank()) {
            sessionKey = e.anonId();
        } else {
            String dt = (e.deviceType() == null || e.deviceType().isBlank()) ? "unknown" : e.deviceType();
            sessionKey = e.ipHash() + ":" + dt;
        }
        return throttleVisit(sessionKey, e.pageUrl());
    }
}
