package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import site.yuqi.analytics.aggregator.enrich.DedupService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoLevel;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
    private UniqueSessionCounter sessionCounter;
    private DedupService dedupService;
    private RollupUpsertService svc;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        sessionCounter = mock(UniqueSessionCounter.class);
        dedupService = mock(DedupService.class);
        when(jdbc.update(anyString(), (Object[]) any())).thenReturn(1);
        // Default: every session is unique (delta=1), matching old behaviour.
        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), anyString())).thenReturn(1L);
        // Default: throttle allows all visits through.
        when(dedupService.throttleVisit(anyString(), anyString())).thenReturn(true);
        svc = new RollupUpsertService(jdbc, sessionCounter, dedupService);
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

    // ---------------------------------------------------------------- batch

    @Test
    void upsertBatchCollapsesSameKeyEvents() {
        // Three events at the same 5m bucket, same geo, same UA — should
        // collapse to ONE row per granularity (2 rows total) with count=3.
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");
        List<EnrichedEvent> batch = List.of(
                sampleEvent("e1", ts),
                sampleEvent("e2", ts.plusSeconds(30)),  // 12:03:30 — same 5m bucket
                sampleEvent("e3", ts.plusSeconds(90))   // 12:04:30 — still same 5m bucket
        );

        svc.upsertBatch(batch);

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        List<List<Object[]>> calls = rowsCaptor.getAllValues();
        // One batch per granularity tier.
        assertThat(calls).hasSize(2);

        // Each granularity tier should collapse to exactly one row.
        assertThat(calls.get(0)).hasSize(1);
        assertThat(calls.get(1)).hasSize(1);

        // Counts are aggregated (event_count, unique_sessions are the last two params).
        Object[] fiveMinRow = calls.get(0).get(0);
        assertThat(fiveMinRow[fiveMinRow.length - 2]).isEqualTo(3L); // event_count
        assertThat(fiveMinRow[fiveMinRow.length - 1]).isEqualTo(3L); // unique_sessions
    }

    @Test
    void upsertBatchKeepsDistinctKeysSeparate() {
        // Two events with different countries → 2 distinct rows per granularity.
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");
        EnrichedEvent us = sampleEvent("e1", ts);
        EnrichedEvent ca = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                "sess-2", "anon-2", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "hash-2",
                new EnrichedGeo(GeoLevel.COUNTRY, "COUNTRY:CA", "CA", null, null));

        svc.upsertBatch(List.of(us, ca));

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());
        // 2 distinct keys → 2 rows per granularity.
        assertThat(rowsCaptor.getAllValues().get(0)).hasSize(2);
        assertThat(rowsCaptor.getAllValues().get(1)).hasSize(2);
    }

    @Test
    void upsertBatchEmptyListIsNoop() {
        svc.upsertBatch(List.of());
        verify(jdbc, times(0)).batchUpdate(anyString(), any(List.class));
        verify(jdbc, times(0)).update(anyString(), (Object[]) any());
    }

    // ---------------------------------------------------------------- HLL session tests

    @Test
    void sameSessionInBatchProducesUniqueSessionsOne() {
        // Two events with the SAME sessionId in one batch → unique_sessions should be 1.
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        // First call returns 1 (new session), second returns 0 (already seen).
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);
        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), eq("sess-1")))
                .thenAnswer(inv -> callCount.incrementAndGet() == 1 ? 1L : 0L);

        List<EnrichedEvent> batch = List.of(
                sampleEvent("e1", ts),
                sampleEvent("e2", ts.plusSeconds(30))
        );

        svc.upsertBatch(batch);

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        Object[] fiveMinRow = rowsCaptor.getAllValues().get(0).get(0);
        assertThat(fiveMinRow[fiveMinRow.length - 2]).isEqualTo(2L); // event_count=2
        assertThat(fiveMinRow[fiveMinRow.length - 1]).isEqualTo(1L); // unique_sessions=1
    }

    @Test
    void differentSessionsInBatchProducesCorrectUniqueSessions() {
        // Two events with DIFFERENT sessionIds → unique_sessions should be 2.
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        // Both calls return 1 (each is a new session).
        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), anyString())).thenReturn(1L);

        EnrichedEvent e1 = sampleEvent("e1", ts);
        EnrichedEvent e2 = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                "sess-2", "anon-1", "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:UT:Salt Lake City", "US", "UT", "Salt Lake City"));

        svc.upsertBatch(List.of(e1, e2));

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        Object[] fiveMinRow = rowsCaptor.getAllValues().get(0).get(0);
        assertThat(fiveMinRow[fiveMinRow.length - 2]).isEqualTo(2L); // event_count=2
        assertThat(fiveMinRow[fiveMinRow.length - 1]).isEqualTo(2L); // unique_sessions=2
    }

    @Test
    void hllFailureYieldsZeroUniqueSessions() {
        // When HLL fails (returns 0), unique_sessions should be 0 (fail-open).
        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), anyString())).thenReturn(0L);

        Instant ts = Instant.parse("2026-06-23T12:03:00Z");
        svc.upsertBatch(List.of(sampleEvent("e1", ts)));

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        Object[] fiveMinRow = rowsCaptor.getAllValues().get(0).get(0);
        assertThat(fiveMinRow[fiveMinRow.length - 2]).isEqualTo(1L); // event_count=1
        assertThat(fiveMinRow[fiveMinRow.length - 1]).isEqualTo(0L); // unique_sessions=0 (fail-open)
    }

    // ----------------------------------------------------------- IP+device 去重测试

    @Test
    void sameIpDifferentDeviceTypeCountsAsDifferentSessions() {
        // 同一 ipHash，desktop vs mobile → 不同设备类型 → 计为 2 个 unique sessions
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        // sessionId/anonId 为空 → fallback 到 ipHash+deviceType
        EnrichedEvent desktop = new EnrichedEvent(
                "e1", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                null, null, "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "same-ip-hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        EnrichedEvent mobile = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts.plusSeconds(10), ts.plusSeconds(10),
                null, null, "/", null, null,
                "mobile", "Safari", "iOS", false, 0.0,
                "same-ip-hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), anyString())).thenReturn(1L);

        svc.upsertBatch(List.of(desktop, mobile));

        // 验证 sessionCounter 收到不同的 session key（ipHash:deviceType）
        ArgumentCaptor<String> sessionKeyCaptor = ArgumentCaptor.forClass(String.class);
        verify(sessionCounter, times(4)).addAndDelta(
                anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), sessionKeyCaptor.capture());

        List<String> keys = sessionKeyCaptor.getAllValues();
        assertThat(keys).contains("same-ip-hash:desktop", "same-ip-hash:mobile");
    }

    @Test
    void sameIpSameDeviceDifferentBrowserCountsAsOne() {
        // 同一 ipHash + 同一 deviceType（mobile），但不同浏览器 → 仍算 1 个 unique session
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        EnrichedEvent chrome = new EnrichedEvent(
                "e1", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                null, null, "/", null, null,
                "mobile", "Chrome", "iOS", false, 0.0,
                "same-ip-hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        EnrichedEvent safari = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts.plusSeconds(30), ts.plusSeconds(30),
                null, null, "/", null, null,
                "mobile", "Safari", "iOS", false, 0.0,
                "same-ip-hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        // 两者 session key 相同: "same-ip-hash:mobile"
        String expectedKey = "same-ip-hash:mobile";
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger(0);
        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), eq(expectedKey)))
                .thenAnswer(inv -> calls.incrementAndGet() == 1 ? 1L : 0L);

        svc.upsertBatch(List.of(chrome, safari));

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        // 验证 session key 都是相同的（不区分浏览器）
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(sessionCounter, times(4)).addAndDelta(
                anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), keyCaptor.capture());
        // 所有 session key 都应该是 "same-ip-hash:mobile"
        assertThat(keyCaptor.getAllValues()).containsOnly(expectedKey);
    }

    @Test
    void sameIpSameDeviceRefreshCountsEventButNotSession() {
        // 同一 IP + 同一设备类型 + 同一浏览器刷新多次 → event_count 累加，unique_sessions 只计 1
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        EnrichedEvent e1 = new EnrichedEvent(
                "e1", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                null, null, "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "same-ip-hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        EnrichedEvent e2 = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts.plusSeconds(30), ts.plusSeconds(30),
                null, null, "/", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "same-ip-hash",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        // 相同 session key "same-ip-hash:desktop"，第一次 1，第二次 0
        String expectedKey = "same-ip-hash:desktop";
        java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger(0);
        when(sessionCounter.addAndDelta(anyString(), anyString(), anyLong(),
                anyString(), anyString(), anyString(), eq(expectedKey)))
                .thenAnswer(inv -> callCount.incrementAndGet() == 1 ? 1L : 0L);

        svc.upsertBatch(List.of(e1, e2));

        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        Object[] fiveMinRow = rowsCaptor.getAllValues().get(0).get(0);
        assertThat(fiveMinRow[fiveMinRow.length - 2]).isEqualTo(2L); // event_count=2（刷新两次）
        assertThat(fiveMinRow[fiveMinRow.length - 1]).isEqualTo(1L); // unique_sessions=1（同设备）
    }

    // ----------------------------------------------------------- visit throttle tests

    @Test
    void throttledRefreshWithin5MinSkipsRollupCounting() {
        // 同一 session+page 5 分钟内刷新 → throttle 返回 false → 不计入 rollup
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        EnrichedEvent e1 = new EnrichedEvent(
                "e1", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                null, null, "/about", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "ip-hash-1",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        EnrichedEvent e2 = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts.plusSeconds(60), ts.plusSeconds(60),
                null, null, "/about", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "ip-hash-1",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        // First call: allow (new visit). Second call onwards: throttled (refresh).
        java.util.concurrent.atomic.AtomicInteger throttleCalls = new java.util.concurrent.atomic.AtomicInteger(0);
        when(dedupService.throttleVisit(eq("ip-hash-1:desktop"), eq("/about")))
                .thenAnswer(inv -> throttleCalls.incrementAndGet() == 1);

        svc.upsertBatch(List.of(e1, e2));

        // Only 1 event should be counted (the second was throttled)
        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        Object[] fiveMinRow = rowsCaptor.getAllValues().get(0).get(0);
        assertThat(fiveMinRow[fiveMinRow.length - 2]).isEqualTo(1L); // event_count=1 (throttled refresh skipped)
    }

    @Test
    void differentPagesNotThrottled() {
        // 同一 session 访问不同页面 → 都通过 throttle → 都计数
        Instant ts = Instant.parse("2026-06-23T12:03:00Z");

        EnrichedEvent e1 = new EnrichedEvent(
                "e1", "yuqi.site", "page_view",
                ts, ts.plusMillis(50),
                null, null, "/about", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "ip-hash-1",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        EnrichedEvent e2 = new EnrichedEvent(
                "e2", "yuqi.site", "page_view",
                ts.plusSeconds(10), ts.plusSeconds(10),
                null, null, "/works", null, null,
                "desktop", "Chrome", "macOS", false, 0.0,
                "ip-hash-1",
                new EnrichedGeo(GeoLevel.METRO, "METRO:US:CA:Stockton", "US", "CA", "Stockton"));

        // Different pages → both pass throttle
        when(dedupService.throttleVisit(eq("ip-hash-1:desktop"), eq("/about"))).thenReturn(true);
        when(dedupService.throttleVisit(eq("ip-hash-1:desktop"), eq("/works"))).thenReturn(true);

        svc.upsertBatch(List.of(e1, e2));

        // Both events counted — same rollup key (same geo/device/bucket) → collapsed to 1 row with count=2
        ArgumentCaptor<List<Object[]>> rowsCaptor = ArgumentCaptor.forClass(List.class);
        verify(jdbc, times(2)).batchUpdate(anyString(), rowsCaptor.capture());

        // event_count should be 2 (both visits counted)
        long totalEvents = rowsCaptor.getAllValues().get(0).stream()
                .mapToLong(row -> (Long) row[row.length - 2]).sum();
        assertThat(totalEvents).isEqualTo(2L);
    }
}
