package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SessionAggregatorServiceTest {

    private JdbcTemplate jdbc;
    private SessionAggregatorService service;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        service = new SessionAggregatorService(jdbc);
        ReflectionTestUtils.setField(service, "configuredSteps",
                List.of("content_open", "read_progress", "subscribe_started", "subscribe_verified"));
        when(jdbc.queryForMap(anyString(), eq("yuqi.site"), eq("session-1"))).thenReturn(Map.ofEntries(
                Map.entry("first_event", Timestamp.from(Instant.parse("2026-01-01T00:00:00Z"))),
                Map.entry("last_event", Timestamp.from(Instant.parse("2026-01-01T00:00:10Z"))),
                Map.entry("page_views", 1L), Map.entry("clicks", 0L),
                Map.entry("entry_page", "/blog/a"), Map.entry("exit_page", "/blog/a"),
                Map.entry("anon_id", "anon-1"), Map.entry("device_type", "desktop"),
                Map.entry("browser", "chrome"), Map.entry("os", "macos"),
                Map.entry("country", "US"), Map.entry("geo_area_id", "COUNTRY:US")));
    }

    @Test
    void rebuildsSessionAndUsesConfiguredFunnelOrder() {
        EnrichedEvent event = event("content_open");

        service.processBatch(List.of(event));

        verify(jdbc).queryForMap(anyString(), eq("yuqi.site"), eq("session-1"));
        verify(jdbc).update(anyString(), eq("event-1"), eq("session-1"), eq("yuqi.site"),
                eq("content_open"), eq("/blog/a"), eq("content_open"),
                eq(Timestamp.from(event.eventTime())), eq(1));
    }

    @Test
    void ignoresEventsOutsideConfiguredFunnel() {
        service.processBatch(List.of(event("page_view")));

        verify(jdbc, never()).update(anyString(), eq("event-1"), eq("session-1"), eq("yuqi.site"),
                eq("page_view"), eq("/blog/a"), eq("page_view"),
                eq(Timestamp.from(Instant.parse("2026-01-01T00:00:05Z"))), eq(1));
    }

    private static EnrichedEvent event(String name) {
        return new EnrichedEvent(
                "event-1", "yuqi.site", name,
                Instant.parse("2026-01-01T00:00:05Z"), Instant.parse("2026-01-01T00:00:06Z"),
                "session-1", "anon-1", "/blog/a", null, "https://google.com/search",
                "desktop", "chrome", "macos", false, 0.1, "ip-hash", null,
                2, "granted", Map.of("contentId", "a"));
    }
}
