package site.yuqi.analytics.aggregator.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Protects the aggregator's admin-only query surface with a server-to-server token. */
@Component
public class AdminTokenFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-Internal-Token";

    private final String expected;

    public AdminTokenFilter(@Value("${analytics.admin.internal-token:}") String expected) {
        this.expected = expected == null ? "" : expected.trim();
    }

    @Override
    protected boolean shouldNotFilter(@NonNull HttpServletRequest request) {
        String path = canonicalPath(request);
        boolean read = "GET".equals(request.getMethod()) || "HEAD".equals(request.getMethod());
        return path != null && read && (under(path, "/api/public")
                || under(path, "/actuator/health") || path.equals("/actuator/prometheus"));
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain filterChain)
            throws ServletException, IOException {
        if (canonicalPath(request) == null) {
            writeError(response, HttpServletResponse.SC_BAD_REQUEST, "non-canonical request path");
            return;
        }
        if (expected.isEmpty()) {
            writeError(response, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "admin token is not configured");
            return;
        }

        String actual = request.getHeader(HEADER);
        if (actual == null || !constantTimeEquals(expected, actual.trim())) {
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "invalid or missing X-Internal-Token");
            return;
        }
        filterChain.doFilter(request, response);
    }

    private static boolean constantTimeEquals(String expected, String actual) {
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                actual.getBytes(StandardCharsets.UTF_8));
    }

    private static String canonicalPath(HttpServletRequest request) {
        String path = request.getRequestURI();
        if (path == null || !path.startsWith("/") || path.contains("%") || path.contains(";")
                || path.contains("\\") || path.contains("//")
                || path.chars().anyMatch(c -> c <= 32 || c == 127)) return null;
        for (String segment : path.split("/")) {
            if (segment.equals(".") || segment.equals("..")) return null;
        }
        String context = request.getContextPath();
        return context != null && !context.isEmpty() && under(path, context)
                ? path.substring(context.length()) : path;
    }

    private static boolean under(String path, String prefix) {
        return path.equals(prefix) || path.startsWith(prefix + "/");
    }

    private static void writeError(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + message + "\"}");
    }
}
