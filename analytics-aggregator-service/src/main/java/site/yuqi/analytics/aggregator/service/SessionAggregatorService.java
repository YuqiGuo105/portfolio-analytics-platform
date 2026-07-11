package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Builds replay-safe session and configured funnel projections from behavior facts. */
@Service
@RequiredArgsConstructor
public class SessionAggregatorService {

    private static final String SELECT_SESSION = """
            select min(event_time) as first_event,
                   max(event_time) as last_event,
                   count(*) filter (where event_name = 'page_view') as page_views,
                   count(*) filter (where event_name in
                     ('outbound_link_clicked','search_result_clicked','recommendation_click')) as clicks,
                   (array_agg(page_path order by event_time asc)
                     filter (where page_path is not null))[1] as entry_page,
                   (array_agg(page_path order by event_time desc)
                     filter (where page_path is not null))[1] as exit_page,
                   max(anon_id_hash) as anon_id,
                   max(device_type) as device_type,
                   max(browser) as browser,
                   max(os) as os,
                   max(country) as country,
                   max(geo_area_id) as geo_area_id
              from public.behavior_events
             where site_id = ? and session_id = ?
            """;

    private static final String UPSERT_SESSION = """
            insert into public.sessions
              (session_id, site_id, anon_id, first_event, last_event,
               page_views, clicks, duration_ms, entry_page, exit_page,
               device_type, browser, os, country, geo_area_id)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (session_id, site_id) do update set
              anon_id = excluded.anon_id,
              first_event = excluded.first_event,
              last_event = excluded.last_event,
              page_views = excluded.page_views,
              clicks = excluded.clicks,
              duration_ms = excluded.duration_ms,
              entry_page = excluded.entry_page,
              exit_page = excluded.exit_page,
              device_type = excluded.device_type,
              browser = excluded.browser,
              os = excluded.os,
              country = excluded.country,
              geo_area_id = excluded.geo_area_id
            """;

    private static final String INSERT_FUNNEL_STEP = """
            insert into public.funnel_steps
              (event_id, session_id, site_id, step_name, page_url, event_type, event_time, step_order)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (event_id) where event_id is not null do nothing
            """;

    private final JdbcTemplate jdbc;

    @Value("${analytics.funnel.steps:content_open,read_progress,subscribe_started,subscribe_verified}")
    private List<String> configuredSteps;

    @Transactional
    public void processBatch(List<EnrichedEvent> events) {
        if (events == null || events.isEmpty()) return;

        Map<String, String> sessions = new LinkedHashMap<>();
        for (EnrichedEvent event : events) {
            String key = resolveSessionKey(event);
            if (key != null) sessions.put(key, event.siteId());
        }
        sessions.forEach(this::rebuildSession);

        for (EnrichedEvent event : events) {
            String sessionKey = resolveSessionKey(event);
            int stepOrder = configuredSteps.indexOf(event.eventType());
            if (sessionKey == null || stepOrder < 0) continue;
            jdbc.update(INSERT_FUNNEL_STEP,
                    event.eventId(), sessionKey, event.siteId(), event.eventType(),
                    event.pageUrl(), event.eventType(), Timestamp.from(event.eventTime()), stepOrder + 1);
        }
    }

    private void rebuildSession(String sessionKey, String siteId) {
        Map<String, Object> row = jdbc.queryForMap(SELECT_SESSION, siteId, sessionKey);
        Timestamp first = (Timestamp) row.get("first_event");
        Timestamp last = (Timestamp) row.get("last_event");
        if (first == null || last == null) return;
        long durationMs = Math.max(0, last.getTime() - first.getTime());

        jdbc.update(UPSERT_SESSION,
                sessionKey, siteId, row.get("anon_id"), first, last,
                number(row.get("page_views")), number(row.get("clicks")), durationMs,
                row.get("entry_page"), row.get("exit_page"), row.get("device_type"),
                row.get("browser"), row.get("os"), row.get("country"), row.get("geo_area_id"));
    }

    private static int number(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
    }

    private static String resolveSessionKey(EnrichedEvent event) {
        if (event.sessionId() != null && !event.sessionId().isBlank()) return event.sessionId();
        if (event.anonId() != null && !event.anonId().isBlank()) return event.anonId();
        if (event.ipHash() == null || event.ipHash().isBlank()) return null;
        String device = event.deviceType() == null || event.deviceType().isBlank()
                ? "unknown" : event.deviceType();
        return event.ipHash() + ":" + device;
    }
}
