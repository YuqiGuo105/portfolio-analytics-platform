package site.yuqi.analytics.aggregator.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;

/**
 * Per-IP fixed-window rate limit for the {@code /api/public/*} read
 * endpoints. Backed by Valkey so limits are shared across every replica
 * behind the Cloud Run load balancer — a single scraper can't just wait
 * for round-robin to hit a fresh instance.
 *
 * <p>Fail-open on Valkey errors: better to serve a few extra reads than
 * to page-crash the visitor globe when Valkey is momentarily unreachable.
 * We do log every degradation so the SRE dashboard can catch a sustained
 * outage that would otherwise silently disable rate-limiting.
 *
 * <p>The X-Forwarded-For header is trusted when present because both
 * Render (dev) and Cloud Run (prod) sit behind Google's HTTPS LB, which
 * scrubs client-supplied XFF and prepends the real client IP as the
 * left-most value. If we ever ship without a reverse proxy, this needs
 * to be tightened to reject XFF outright.
 */
@Slf4j
@Component
public class IpRateLimitFilter extends OncePerRequestFilter {

    /** Only guard the public read surface — actuator and swagger have their own posture. */
    private static final String PROTECTED_PREFIX = "/api/public/";

    private final StringRedisTemplate redis;
    private final int limit;
    private final Duration window;
    private final boolean enabled;

    public IpRateLimitFilter(
            StringRedisTemplate redis,
            @Value("${analytics.public.rate-limit.enabled:true}") boolean enabled,
            @Value("${analytics.public.rate-limit.max-per-window:120}") int limit,
            @Value("${analytics.public.rate-limit.window-seconds:60}") long windowSeconds) {
        this.redis = redis;
        this.enabled = enabled;
        this.limit = limit;
        this.window = Duration.ofSeconds(Math.max(1, windowSeconds));
    }

    @Override
    protected boolean shouldNotFilter(@NonNull HttpServletRequest req) {
        if (!enabled) return true;
        String uri = req.getRequestURI();
        return uri == null || !uri.startsWith(PROTECTED_PREFIX);
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest req,
                                    @NonNull HttpServletResponse resp,
                                    @NonNull FilterChain chain)
            throws ServletException, IOException {
        String ip = clientIp(req);
        // Fixed-window key: bucket the current wall-clock into window-sized
        // slices so INCR + EXPIRE gives us a naturally-resetting counter
        // without needing per-request Lua.
        long bucket = System.currentTimeMillis() / (window.toMillis());
        String key = "rl:pub:" + ip + ":" + bucket;
        long count;
        try {
            Long incr = redis.opsForValue().increment(key);
            count = incr == null ? 0L : incr;
            if (count == 1L) {
                // First hit in this window — set TTL so the key can't leak.
                // Add one window of grace so a request landing on the
                // boundary doesn't get its counter evicted mid-bucket.
                redis.expire(key, window.plusSeconds(5));
            }
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"ratelimit_redis_error\",\"ip\":\"{}\",\"err\":\"{}\"}",
                    ip, e.getMessage());
            chain.doFilter(req, resp); // fail-open
            return;
        }

        if (count > limit) {
            log.info("{\"event\":\"ratelimit_block\",\"ip\":\"{}\",\"count\":{},\"limit\":{}}}",
                    ip, count, limit);
            resp.setStatus(429);
            resp.setHeader("Retry-After", String.valueOf(window.toSeconds()));
            resp.setContentType("application/json");
            resp.getWriter().write("{\"error\":\"rate_limited\",\"limit\":" + limit
                    + ",\"window_seconds\":" + window.toSeconds() + "}");
            return;
        }
        chain.doFilter(req, resp);
    }

    /**
     * Prefer the left-most XFF hop when the request came through a proxy.
     * Falls back to the TCP peer. See class javadoc for the trust model.
     */
    private static String clientIp(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            int comma = xff.indexOf(',');
            String first = (comma > 0 ? xff.substring(0, comma) : xff).trim();
            if (!first.isEmpty()) return first;
        }
        String real = req.getHeader("X-Real-IP");
        if (real != null && !real.isBlank()) return real.trim();
        return req.getRemoteAddr() == null ? "unknown" : req.getRemoteAddr();
    }
}
