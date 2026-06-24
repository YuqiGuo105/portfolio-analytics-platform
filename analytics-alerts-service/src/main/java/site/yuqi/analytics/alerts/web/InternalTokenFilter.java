package site.yuqi.analytics.alerts.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Rejects any non-actuator, non-OpenAPI request that doesn't carry a valid
 * {@code X-Internal-Token} header. The dashboard / Next.js side will hit
 * these endpoints through its own server-side proxy that injects the
 * header — they're never reachable from a browser directly.
 */
@Slf4j
@Component
public class InternalTokenFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-Internal-Token";

    private final String expected;

    public InternalTokenFilter(@Value("${analytics.internal-token:}") String expected) {
        this.expected = expected == null ? "" : expected.trim();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest req) {
        String p = req.getRequestURI();
        return p.startsWith("/actuator/")
                || p.startsWith("/v3/api-docs")
                || p.startsWith("/swagger-ui");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        if (expected.isEmpty()) {
            // No token configured → fail closed.
            resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            resp.getWriter().write("{\"error\":\"internal token not configured on server\"}");
            return;
        }
        String got = req.getHeader(HEADER);
        if (got == null || !expected.equals(got.trim())) {
            resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            resp.getWriter().write("{\"error\":\"invalid or missing X-Internal-Token\"}");
            return;
        }
        chain.doFilter(req, resp);
    }
}
