package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoLevel;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test of the UPSERT binding logic. We can't unit-test the actual
 * Postgres {@code on conflict do update} round-trip here because H2 (the
 * only embedded DB available without Docker) doesn't accept that syntax —
 * a Testcontainers-backed integration test is tracked for follow-up.
 */
class RollupUpsertServiceTest {

    private JdbcTemplate jdbc;
    private RollupUpsertService svc;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), (Object[]) any())).thenReturn(1);
        svc = new RollupUpsertService(jdbc);
    }

    @Test
    void writesOneUpsertPerGranularity() {
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");
        svc.upsert(sampleEvent("ev-1", ts));

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbc, times(2)).update(sqlCaptor.capture(), (Object[]) argsCaptor.capture());

        // Both calls use the same UPSERT statement.
        assertThat(sqlCaptor.getAllValues()).hasSize(2);
        assertThat(sqlCaptor.getAllValues().get(0)).contains("on conflict");

        List<Object[]> calls = argsCaptor.getAllValues();
        assertThat(calls).hasSize(2);

        // 5m bucket floor of 12:03:00 = 12:00:00 UTC.
        assertThat(((Timestamp) calls.get(0)[1]).toInstant())
                .isEqualTo(Instant.parse("2026-06-23T12:00:00Z"));
        assertThat(calls.get(0)[2]).isEqualTo("5m");

        // 1d bucket floor of 12:03 = 00:00:00 UTC of that day.
        assertThat(((Timestamp) calls.get(1)[1]).toInstant())
                .isEqualTo(Instant.parse("2026-06-23T00:00:00Z"));
        assertThat(calls.get(1)[2]).isEqualTo("1d");
    }

    @Test
    void nullGeoBindsGlobalLevelAndAreaId() {
        EnrichedEvent ev = new EnrichedEvent(
                "ev-3", "yuqi.site", "page_view",
                Instant.parse("2026-06-23T12:03:00Z"),
                Instant.parse("2026-06-23T12:03:01Z"),
                "sess", "anon", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "hash", null);

        svc.upsert(ev);

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbc, times(2)).update(anyString(), (Object[]) argsCaptor.capture());

        Object[] firstCall = argsCaptor.getAllValues().get(0);
        // Positions in UPSERT_SQL: site_id, bucket_time, granularity, geo_level, geo_area_id, ...
        assertThat(firstCall[3]).isEqualTo("GLOBAL");
        assertThat(firstCall[4]).isEqualTo("GLOBAL");
    }

    @Test
    void blankDeviceTypeBecomesUnknownInBinding() {
        EnrichedEvent ev = new EnrichedEvent(
                "ev-4", "yuqi.site", "page_view",
                Instant.parse("2026-06-23T12:03:00Z"),
                Instant.parse("2026-06-23T12:03:01Z"),
                "sess", "anon", "/", null, null,
                "", "", "", false, 0.0,
                "hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:UT:Salt Lake City", "US", "UT", "Salt Lake City"));

        svc.upsert(ev);

        ArgumentCaptor<Object[]> args = ArgumentCaptor.forClass(Object[].class);
        verify(jdbc, times(2)).update(anyString(), (Object[]) args.capture());
        Object[] first = args.getAllValues().get(0);
        // device_type, browser, os are positions 6, 7, 8.
        assertThat(first[6]).isEqualTo("unknown");
        assertThat(first[7]).isEqualTo("unknown");
        assertThat(first[8]).isEqualTo("unknown");
        assertThat(first[10]).isEqualTo("US"); // country
    }

    private static EnrichedEvent sampleEvent(String eventId, Instant ts) {
        return new EnrichedEvent(
                eventId, "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                "sess-1", "anon-1", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:UT:Salt Lake City", "US", "UT", "Salt Lake City"));
    }
}
