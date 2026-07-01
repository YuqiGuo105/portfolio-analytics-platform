package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregates enriched events into per-session rows in the
 * {@code sessions} table and records funnel steps in
 * {@code funnel_steps}.
 *
 * <p>Uses an UPSERT (INSERT ... ON CONFLICT UPDATE) on
 * {@code (session_id, site_id)} so multiple batches can incrementally
 * update the same session as more events arrive.
 *
 * <p>Funnel steps are only recorded for specific event types that
 * matter for conversion tracking: {@code page_view} and {@code click}.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SessionAggregatorService {

    private final JdbcTemplate jdbc;

    private static final String UPSERT_SESSION = """
            INSERT INTO public.sessions
              (session_id, site_id, anon_id, first_event, last_event,
               page_views, clicks, duration_ms, entry_page, exit_page,
               device_type, browser, os, country, geo_area_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (session_id, site_id) DO UPDATE SET
              last_event   = GREATEST(sessions.last_event, EXCLUDED.last_event),
              page_views   = sessions.page_views + EXCLUDED.page_views,
              clicks       = sessions.clicks + EXCLUDED.clicks,
              duration_ms  = EXTRACT(EPOCH FROM (GREATEST(sessions.last_event, EXCLUDED.last_event)
                             - sessions.first_event)) * 1000,
              exit_page    = EXCLUDED.exit_page
            """;

    private static final String INSERT_FUNNEL_STEP = """
            INSERT INTO public.funnel_steps
              (session_id, site_id, step_name, page_url, event_type, event_time, step_order)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    /**
     * Process a batch of enriched events: group by session, upsert
     * session rows, and insert funnel steps.
     */
    public void processBatch(List<EnrichedEvent> events) {
        if (events == null || events.isEmpty()) return;

        log.info("{\"event\":\"session_batch_start\",\"size\":{}}", events.size());

        // Group events by session
        Map<String, List<EnrichedEvent>> bySession = new HashMap<>();
        for (EnrichedEvent e : events) {
            String key = resolveSessionKey(e);
            if (key == null) {
                log.warn("{\"event\":\"session_key_null\",\"eventId\":\"{}\"}", e.eventId());
                continue;
            }
            bySession.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(e);
        }

        log.info("{\"event\":\"session_batch_grouped\",\"sessions\":{}}", bySession.size());

        // Upsert each session and insert funnel steps
        for (var entry : bySession.entrySet()) {
            String sessionKey = entry.getKey();
            List<EnrichedEvent> sessionEvents = entry.getValue();
            upsertSession(sessionKey, sessionEvents);
            insertFunnelSteps(sessionKey, sessionEvents);
        }
    }

    private void upsertSession(String sessionKey, List<EnrichedEvent> events) {
        EnrichedEvent first = events.get(0);
        String siteId = first.siteId();

        Instant firstTime = events.stream().map(EnrichedEvent::eventTime).min(Instant::compareTo).orElse(first.eventTime());
        Instant lastTime = events.stream().map(EnrichedEvent::eventTime).max(Instant::compareTo).orElse(first.eventTime());

        long pageViews = events.stream().filter(e -> "page_view".equals(e.eventType())).count();
        long clicks = events.stream().filter(e -> "click".equals(e.eventType())).count();
        long durationMs = Duration.between(firstTime, lastTime).toMillis();

        // Entry page = first event's pageUrl, exit page = last event's pageUrl
        String entryPage = events.stream()
                .min((a, b) -> a.eventTime().compareTo(b.eventTime()))
                .map(EnrichedEvent::pageUrl).orElse(null);
        String exitPage = events.stream()
                .max((a, b) -> a.eventTime().compareTo(b.eventTime()))
                .map(EnrichedEvent::pageUrl).orElse(null);

        String country = first.geo() != null ? first.geo().country() : null;
        String geoAreaId = first.geo() != null ? first.geo().geoAreaId() : null;

        try {
            jdbc.update(UPSERT_SESSION,
                    sessionKey, siteId, first.anonId(),
                    Timestamp.from(firstTime), Timestamp.from(lastTime),
                    (int) pageViews, (int) clicks, durationMs,
                    entryPage, exitPage,
                    first.deviceType(), first.browser(), first.os(),
                    country, geoAreaId);
            log.info("{\"event\":\"session_upserted\",\"sessionId\":\"{}\",\"siteId\":\"{}\"}",
                    sessionKey, siteId);
        } catch (RuntimeException e) {
            log.warn("{\"event\":\"session_upsert_error\",\"sessionId\":\"{}\",\"err\":\"{}\"}",
                    sessionKey, e.getMessage());
        }
    }

    private void insertFunnelSteps(String sessionKey, List<EnrichedEvent> events) {
        int stepOrder = 0;
        for (EnrichedEvent e : events) {
            if (!"page_view".equals(e.eventType()) && !"click".equals(e.eventType())) {
                continue;
            }
            String stepName = deriveFunnelStep(e);
            try {
                jdbc.update(INSERT_FUNNEL_STEP,
                        sessionKey, e.siteId(), stepName,
                        e.pageUrl(), e.eventType(),
                        Timestamp.from(e.eventTime()), stepOrder++);
            } catch (RuntimeException ex) {
                log.warn("{\"event\":\"funnel_step_error\",\"sessionId\":\"{}\",\"err\":\"{}\"}",
                        sessionKey, ex.getMessage());
            }
        }
    }

    /**
     * Derive a funnel step name from the event. Uses the page path as a
     * meaningful step name (e.g. "/", "/works", "/cv" → "home", "works", "cv").
     */
    private String deriveFunnelStep(EnrichedEvent e) {
        String url = e.pageUrl();
        if (url == null || url.isBlank()) return "unknown";

        // Strip domain, keep path
        String path = url;
        int schemeEnd = url.indexOf("://");
        if (schemeEnd > 0) {
            int pathStart = url.indexOf('/', schemeEnd + 3);
            path = pathStart > 0 ? url.substring(pathStart) : "/";
        }

        // Normalize path → step name
        if ("/".equals(path) || path.isBlank()) return "home";
        // Remove leading slash and trailing slash
        String step = path.replaceAll("^/|/$", "");
        // Take first path segment only
        int slash = step.indexOf('/');
        if (slash > 0) step = step.substring(0, slash);
        return step.isEmpty() ? "home" : step;
    }

    private String resolveSessionKey(EnrichedEvent e) {
        if (e.sessionId() != null && !e.sessionId().isBlank()) return e.sessionId();
        if (e.anonId() != null && !e.anonId().isBlank()) return e.anonId();
        if (e.ipHash() != null && !e.ipHash().isBlank()) return e.ipHash();
        return null;
    }
}
