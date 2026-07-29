package site.yuqi.analytics.aggregator.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ResponseCacheTest {

    private StringRedisTemplate redis;
    private ValueOperations<String, String> ops;
    private ResponseCache cache;

    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        cache = new ResponseCache(
                redis, new ObjectMapper(), true, 30,
                true, 5, 100, 10);
    }

    @Test
    void putAndGet() {
        // Capture what's stored
        doAnswer(inv -> {
            String key = inv.getArgument(0);
            String val = inv.getArgument(1);
            when(ops.get(eq(key))).thenReturn(val);
            return null;
        }).when(ops).set(anyString(), anyString(), any(Duration.class));

        List<Map<String, Object>> data = List.of(Map.of("count", 42));
        String etag = cache.put("k1", data);

        assertThat(etag).startsWith("W/\"");
        ResponseCache.CacheEntry entry = cache.get("k1");
        assertThat(entry).isNotNull();
        assertThat(entry.etag()).isEqualTo(etag);
        assertThat(entry.json()).contains("42");
    }

    @Test
    void getMissReturnsNull() {
        when(ops.get("miss")).thenReturn(null);
        assertThat(cache.get("miss")).isNull();
    }

    @Test
    void disabledCacheReturnsNull() {
        ResponseCache disabled = new ResponseCache(
                redis, new ObjectMapper(), false, 30,
                true, 5, 100, 10);
        assertThat(disabled.get("any")).isNull();
    }

    @Test
    void redisErrorFailsOpen() {
        when(ops.get("boom")).thenThrow(new RuntimeException("connection refused"));
        assertThat(cache.get("boom")).isNull();
    }

    @Test
    void computeEtagDeterministic() {
        String e1 = ResponseCache.computeEtag("hello");
        String e2 = ResponseCache.computeEtag("hello");
        assertThat(e1).isEqualTo(e2);
        assertThat(e1).startsWith("W/\"");
    }

    @Test
    void cacheHitSkipsLockAndLoader() {
        when(ops.get("hit")).thenReturn("W/\"cached\"\n{\"count\":7}");
        AtomicInteger loads = new AtomicInteger();

        ResponseCache.CacheResult result = cache.getOrLoad("hit", () -> {
            loads.incrementAndGet();
            return Map.of("count", 99);
        });

        assertThat(result.cacheHit()).isTrue();
        assertThat(result.body()).isEqualTo("{\"count\":7}");
        assertThat(loads).hasValue(0);
        verify(ops, never()).setIfAbsent(anyString(), anyString(), any(Duration.class));
    }

    @Test
    void leaseOwnerLoadsCachesAndReleasesWithCompareAndDelete() {
        when(ops.get("load")).thenReturn(null);
        when(ops.setIfAbsent(eq("load:fill-lock"), anyString(), any(Duration.class)))
                .thenReturn(true);
        AtomicInteger loads = new AtomicInteger();

        ResponseCache.CacheResult result = cache.getOrLoad("load", () -> {
            loads.incrementAndGet();
            return Map.of("count", 42);
        });

        assertThat(result.cacheHit()).isFalse();
        assertThat(result.body()).isEqualTo(Map.of("count", 42));
        assertThat(loads).hasValue(1);
        verify(ops).set(eq("load"), contains("\"count\":42"), any(Duration.class));
        verify(redis).execute(any(), eq(List.of("load:fill-lock")), anyString());
        verify(redis, never()).delete("load:fill-lock");
    }

    @Test
    void contenderUsesValueFilledByLeaseOwner() {
        when(ops.get("shared"))
                .thenReturn(null, "W/\"filled\"\n{\"count\":11}");
        when(ops.setIfAbsent(eq("shared:fill-lock"), anyString(), any(Duration.class)))
                .thenReturn(false);
        AtomicInteger loads = new AtomicInteger();

        ResponseCache.CacheResult result = cache.getOrLoad("shared", () -> {
            loads.incrementAndGet();
            return Map.of("count", 99);
        });

        assertThat(result.cacheHit()).isTrue();
        assertThat(result.body()).isEqualTo("{\"count\":11}");
        assertThat(loads).hasValue(0);
    }

    @Test
    void contenderFallsBackToLoaderAfterBoundedWait() {
        ResponseCache noWait = new ResponseCache(
                redis, new ObjectMapper(), true, 30,
                true, 5, 0, 10);
        when(ops.get("timeout")).thenReturn(null);
        when(ops.setIfAbsent(eq("timeout:fill-lock"), anyString(), any(Duration.class)))
                .thenReturn(false);

        ResponseCache.CacheResult result =
                noWait.getOrLoad("timeout", () -> Map.of("count", 3));

        assertThat(result.cacheHit()).isFalse();
        assertThat(result.body()).isEqualTo(Map.of("count", 3));
        verify(ops).set(eq("timeout"), contains("\"count\":3"), any(Duration.class));
    }

    @Test
    void lockFailureFailsOpenToAuthoritativeLoader() {
        when(ops.get("degraded")).thenReturn(null);
        when(ops.setIfAbsent(eq("degraded:fill-lock"), anyString(), any(Duration.class)))
                .thenThrow(new RuntimeException("Valkey unavailable"));

        ResponseCache.CacheResult result =
                cache.getOrLoad("degraded", () -> Map.of("count", 5));

        assertThat(result.cacheHit()).isFalse();
        assertThat(result.body()).isEqualTo(Map.of("count", 5));
    }

    @Test
    void concurrentMissBurstInvokesAuthoritativeLoaderOnce() throws Exception {
        AtomicReference<String> stored = new AtomicReference<>();
        AtomicBoolean leaseHeld = new AtomicBoolean();
        AtomicInteger loads = new AtomicInteger();

        when(ops.get("burst")).thenAnswer(inv -> stored.get());
        when(ops.setIfAbsent(eq("burst:fill-lock"), anyString(), any(Duration.class)))
                .thenAnswer(inv -> leaseHeld.compareAndSet(false, true));
        doAnswer(inv -> {
            stored.set(inv.getArgument(1));
            return null;
        }).when(ops).set(eq("burst"), anyString(), any(Duration.class));

        int callers = 8;
        ExecutorService executor = Executors.newFixedThreadPool(callers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseCache.CacheResult>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < callers; i++) {
                futures.add(executor.submit(() -> {
                    start.await();
                    return cache.getOrLoad("burst", () -> {
                        loads.incrementAndGet();
                        try {
                            Thread.sleep(60);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return Map.of("count", 77);
                    });
                }));
            }
            start.countDown();

            for (Future<ResponseCache.CacheResult> future : futures) {
                assertThat(future.get(2, TimeUnit.SECONDS).etag()).startsWith("W/\"");
            }
            assertThat(loads).hasValue(1);
        } finally {
            executor.shutdownNow();
        }
    }
}
