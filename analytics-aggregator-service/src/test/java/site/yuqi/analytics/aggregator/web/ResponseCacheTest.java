package site.yuqi.analytics.aggregator.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.List;
import java.util.Map;

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
        cache = new ResponseCache(redis, new ObjectMapper(), true, 30);
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
        ResponseCache disabled = new ResponseCache(redis, new ObjectMapper(), false, 30);
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
}
