package site.yuqi.analytics.enrichment.service;

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

    /** @return {@code true} when this is the first time we've seen {@code eventId}. */
    public boolean acquire(String eventId) {
        if (eventId == null || eventId.isBlank()) return true;
        try {
            Boolean ok = redis.opsForValue().setIfAbsent(
                    KEY_PREFIX + eventId, "1", Duration.ofSeconds(ttlSeconds));
            return Boolean.TRUE.equals(ok);
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"dedup_redis_error\",\"err\":\"{}\"}", e.getMessage());
            return true; // fail-open
        }
    }
}
