package site.yuqi.analytics.aggregator.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Valkey read-through cache for the public markers and summary endpoints.
 *
 * <p>Stores serialized JSON + a weak ETag derived from SHA-256 of the
 * content. TTL is short (default 30s) so the dashboard never sees stale
 * data beyond one refresh cycle. No explicit invalidation on write —
 * relying purely on TTL expiry. Jitter ±20% is added to avoid
 * synchronized expiry stampede across concurrent requests.
 *
 * <p><b>Fail-open</b>: on any Valkey error, logs a warning and returns
 * null (miss), letting the controller fall through to the live DB query.
 * Consistent with {@link site.yuqi.analytics.aggregator.enrich.DedupService}.
 */
@Component
@Slf4j
public class ResponseCache {

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final boolean enabled;
    private final int ttlSeconds;

    public ResponseCache(
            StringRedisTemplate redis,
            ObjectMapper mapper,
            @Value("${analytics.public.cache.enabled:true}") boolean enabled,
            @Value("${analytics.public.cache.ttl-seconds:30}") int ttlSeconds) {
        this.redis = redis;
        this.mapper = mapper;
        this.enabled = enabled;
        this.ttlSeconds = ttlSeconds;
    }

    /** Cached entry: serialized JSON body + weak ETag. */
    public record CacheEntry(String json, String etag) {}

    /**
     * Try to get a cached response for the given key.
     * @return the cached entry, or null on miss / disabled / error.
     */
    public CacheEntry get(String key) {
        if (!enabled) return null;
        try {
            String raw = redis.opsForValue().get(key);
            if (raw == null) return null;
            // Stored format: etag\n{json}
            int nl = raw.indexOf('\n');
            if (nl < 0) return null;
            String etag = raw.substring(0, nl);
            String json = raw.substring(nl + 1);
            return new CacheEntry(json, etag);
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"cache_get_error\",\"key\":\"{}\",\"err\":\"{}\"}", key, e.getMessage());
            return null;
        }
    }

    /**
     * Store a response in the cache.
     * @param key   cache key
     * @param value the response object to serialize and cache
     * @return the computed ETag for the response
     */
    public String put(String key, Object value) {
        if (!enabled) return computeEtag(serializeSafe(value));
        String json = serializeSafe(value);
        String etag = computeEtag(json);
        try {
            String stored = etag + "\n" + json;
            Duration ttl = jitteredTtl();
            redis.opsForValue().set(key, stored, ttl);
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"cache_put_error\",\"key\":\"{}\",\"err\":\"{}\"}", key, e.getMessage());
        }
        return etag;
    }

    private String serializeSafe(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return "[]";
        }
    }

    private Duration jitteredTtl() {
        // ±20% jitter to avoid stampede
        double jitter = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4;
        long ms = (long) (ttlSeconds * 1000.0 * jitter);
        return Duration.ofMillis(Math.max(ms, 1000));
    }

    static String computeEtag(String json) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(json.getBytes(StandardCharsets.UTF_8));
            return "W/\"" + HexFormat.of().formatHex(hash, 0, 8) + "\"";
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is always available in standard JVM
            return "W/\"unknown\"";
        }
    }
}
