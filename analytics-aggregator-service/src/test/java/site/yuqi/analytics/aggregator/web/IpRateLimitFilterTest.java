package site.yuqi.analytics.aggregator.web;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IpRateLimitFilterTest {

    private StringRedisTemplate redis;
    private IpRateLimitFilter filter;

    @BeforeEach
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        filter = new IpRateLimitFilter(redis, true, 2, 60);
    }

    @Test
    void underLimitUsesAtomicCounterAndContinues() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString())).thenReturn(1L);
        FilterChain chain = mock(FilterChain.class);
        MockHttpServletRequest request = publicRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(redis).execute(
                any(RedisScript.class),
                anyList(),
                anyString());
        verify(redis, never()).expire(anyString(), any());
    }

    @Test
    void overLimitReturns429WithoutCallingApplication() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString())).thenReturn(3L);
        FilterChain chain = mock(FilterChain.class);
        MockHttpServletRequest request = publicRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(429);
        assertThat(response.getHeader("Retry-After")).isEqualTo("60");
        assertThat(response.getContentAsString()).contains("\"error\":\"rate_limited\"");
        verify(chain, never()).doFilter(request, response);
    }

    @Test
    void valkeyFailureFailsOpen() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), anyString()))
                .thenThrow(new RuntimeException("timeout"));
        FilterChain chain = mock(FilterChain.class);
        MockHttpServletRequest request = publicRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void nonPublicPathDoesNotTouchValkey() throws Exception {
        FilterChain chain = mock(FilterChain.class);
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        verify(redis, never()).execute(
                any(RedisScript.class),
                any(List.class),
                anyString());
    }

    private static MockHttpServletRequest publicRequest() {
        MockHttpServletRequest request =
                new MockHttpServletRequest("GET", "/api/public/visits/summary");
        request.setRemoteAddr("203.0.113.10");
        return request;
    }
}
