package site.yuqi.analytics.aggregator.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Valkey read-through cache for the public markers and summary endpoints.
 *
 * <p>Stores serialized JSON + a weak ETag derived from SHA-256 of the
 * content. TTL is short (default 30s) so the dashboard never sees stale
 * data beyond one refresh cycle. No explicit invalidation on write —
 * relying purely on TTL expiry. Jitter ±20% is added to avoid
 * synchronized expiry stampede across concurrent requests.
 *
 * <p>Cache misses use a short Valkey lease as a distributed single-flight
 * guard. One request fills the cache while contenders wait for a bounded
 * interval. Lease ownership is represented by a random token and released
 * through compare-and-delete Lua, so a slow request cannot delete a newer
 * owner's lock after its own lease expires.
 *
 * <p><b>Fail-open</b>: on any Valkey error, logs a warning and returns
 * null (miss), letting the controller fall through to the live DB query.
 * Consistent with {@link site.yuqi.analytics.aggregator.enrich.DedupService}.
 */
@Component
@Slf4j
public class ResponseCache {

    private static final DefaultRedisScript<Long> RELEASE_LOCK_SCRIPT =
            new DefaultRedisScript<>(
                    "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                            "return redis.call('del', KEYS[1]) else return 0 end",
                    Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final boolean enabled;
    private final int ttlSeconds;
    private final boolean lockEnabled;
    private final Duration lockLease;
    private final Duration lockWait;
    private final long lockPollMillis;

    public ResponseCache(
            StringRedisTemplate redis,
            ObjectMapper mapper,
            @Value("${analytics.public.cache.enabled:true}") boolean enabled,
            @Value("${analytics.public.cache.ttl-seconds:30}") int ttlSeconds,
            @Value("${analytics.public.cache.lock.enabled:true}") boolean lockEnabled,
            @Value("${analytics.public.cache.lock.lease-seconds:5}") long lockLeaseSeconds,
            @Value("${analytics.public.cache.lock.wait-millis:500}") long lockWaitMillis,
            @Value("${analytics.public.cache.lock.poll-millis:40}") long lockPollMillis) {
        this.redis = redis;
        this.mapper = mapper;
        this.enabled = enabled;
        this.ttlSeconds = ttlSeconds;
        this.lockEnabled = lockEnabled;
        this.lockLease = Duration.ofSeconds(Math.max(1, lockLeaseSeconds));
        this.lockWait = Duration.ofMillis(Math.max(0, lockWaitMillis));
        this.lockPollMillis = Math.max(10, lockPollMillis);
    }

    /** Cached entry: serialized JSON body + weak ETag. */
    public record CacheEntry(String json, String etag) {}

    /**
     * HTTP-ready response from either Valkey or the supplied authoritative loader.
     * A cache hit carries serialized JSON; a miss carries the original object so
     * controller tests and Spring's normal response serialization remain unchanged.
     */
    public record CacheResult(Object body, String etag, boolean cacheHit) {}

    /**
     * Read through the cache with a distributed single-flight guard.
     *
     * <p>Contenders wait only for {@code lock.wait-millis}; after that they execute
     * the loader independently. This bounds request latency and keeps PostgreSQL
     * available as the source-of-truth fallback if the lock owner stalls.
     */
    public CacheResult getOrLoad(String key, Supplier<?> loader) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");

        CacheEntry initial = get(key);
        if (initial != null) return hit(initial);
        if (!enabled) return loadAndPut(key, loader);

        try (LoadLease lease = tryAcquireLease(key)) {
            if (lease.owner()) {
                // Close the race where another owner filled the key between
                // our initial GET and successful lease acquisition.
                CacheEntry raced = get(key);
                if (raced != null) return hit(raced);
            } else if (lease.contended()) {
                CacheEntry filled = awaitFill(key);
                if (filled != null) return hit(filled);
                log.warn("{\"event\":\"cache_fill_wait_timeout\",\"key\":\"{}\",\"waitMs\":{}}",
                        key, lockWait.toMillis());
            }
            return loadAndPut(key, loader);
        }
    }

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
        String json = serializeSafe(value);
        String etag = computeEtag(json);
        if (!enabled) return etag;
        try {
            String stored = etag + "\n" + json;
            Duration ttl = jitteredTtl();
            redis.opsForValue().set(key, stored, ttl);
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"cache_put_error\",\"key\":\"{}\",\"err\":\"{}\"}", key, e.getMessage());
        }
        return etag;
    }

    private CacheResult loadAndPut(String key, Supplier<?> loader) {
        Object value = loader.get();
        return new CacheResult(value, put(key, value), false);
    }

    private static CacheResult hit(CacheEntry entry) {
        return new CacheResult(entry.json(), entry.etag(), true);
    }

    private LoadLease tryAcquireLease(String key) {
        if (!lockEnabled) return LoadLease.bypassLease();

        String lockKey = key + ":fill-lock";
        String token = UUID.randomUUID().toString();
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(lockKey, token, lockLease);
            if (Boolean.TRUE.equals(acquired)) {
                return LoadLease.owner(lockKey, token, this::releaseLease);
            }
            if (Boolean.FALSE.equals(acquired)) return LoadLease.forContender();

            log.warn("{\"event\":\"cache_lock_setnx_null\",\"key\":\"{}\"}", key);
            return LoadLease.bypassLease();
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"cache_lock_acquire_error\",\"key\":\"{}\",\"err\":\"{}\"}",
                    key, e.getMessage());
            return LoadLease.bypassLease();
        }
    }

    private CacheEntry awaitFill(String key) {
        long deadline = System.nanoTime() + lockWait.toNanos();
        while (System.nanoTime() < deadline) {
            CacheEntry filled = get(key);
            if (filled != null) return filled;

            long remainingMillis = Math.max(1,
                    Duration.ofNanos(Math.max(0, deadline - System.nanoTime())).toMillis());
            long boundedPoll = Math.min(lockPollMillis, remainingMillis);
            long jitter = ThreadLocalRandom.current().nextLong(
                    Math.max(1, boundedPoll / 4 + 1));
            try {
                Thread.sleep(Math.min(remainingMillis, boundedPoll + jitter));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return get(key);
    }

    private void releaseLease(String lockKey, String token) {
        try {
            redis.execute(RELEASE_LOCK_SCRIPT, List.of(lockKey), token);
        } catch (RuntimeException e) {
            // The TTL still guarantees eventual release. Do not fail a
            // successful response because the best-effort cleanup failed.
            log.warn("{\"event\":\"cache_lock_release_error\",\"key\":\"{}\",\"err\":\"{}\"}",
                    lockKey, e.getMessage());
        }
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

    private enum LeaseState {
        OWNER,
        CONTENDED,
        BYPASS
    }

    private record LoadLease(
            LeaseState state,
            String lockKey,
            String token,
            LeaseReleaser releaser) implements AutoCloseable {

        static LoadLease owner(String lockKey, String token, LeaseReleaser releaser) {
            return new LoadLease(LeaseState.OWNER, lockKey, token, releaser);
        }

        static LoadLease forContender() {
            return new LoadLease(LeaseState.CONTENDED, null, null, null);
        }

        static LoadLease bypassLease() {
            return new LoadLease(LeaseState.BYPASS, null, null, null);
        }

        boolean owner() {
            return state == LeaseState.OWNER;
        }

        boolean contended() {
            return state == LeaseState.CONTENDED;
        }

        @Override
        public void close() {
            if (owner()) releaser.release(lockKey, token);
        }
    }

    @FunctionalInterface
    private interface LeaseReleaser {
        void release(String lockKey, String token);
    }
}
