package site.yuqi.analytics.aggregator.backfill;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import site.yuqi.analytics.aggregator.enrich.BotScoreService;
import site.yuqi.analytics.aggregator.enrich.DedupService;
import site.yuqi.analytics.aggregator.enrich.EnrichmentPipeline;
import site.yuqi.analytics.aggregator.enrich.GeoAreaCentroidService;
import site.yuqi.analytics.aggregator.enrich.GeoSnapService;
import site.yuqi.analytics.aggregator.enrich.IpHashService;
import site.yuqi.analytics.aggregator.enrich.UaParserService;
import site.yuqi.analytics.aggregator.service.RollupUpsertService;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Backfill collects every row into a List via {@link RowMapper}, then
 * iterates outside the SELECT transaction. We stub
 * {@link JdbcTemplate#query(String, RowMapper)} with a hand-rolled
 * {@link ResultSet} so the mapper runs exactly as Postgres would drive it.
 */
class BackfillServiceTest {

    private JdbcTemplate jdbc;
    private RollupUpsertService rollup;
    private BackfillService svc;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        rollup = mock(RollupUpsertService.class);
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        EnrichmentPipeline pipeline = new EnrichmentPipeline(mapper,
                new IpHashService("salt"),
                new UaParserService(),
                new BotScoreService(),
                new GeoSnapService(mock(GeoAreaCentroidService.class)),
                mock(DedupService.class));
        svc = new BackfillService(jdbc, pipeline, rollup);
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "siteId", "yuqi.site");
        org.springframework.test.util.ReflectionTestUtils.setField(svc, "batchSize", 100);
    }

    @Test
    void backfillVisitorLogsRunsEveryRowThroughThePipeline() {
        ResultSet rs = mockVisitorLogResultSet(3);
        stubJdbcQuery(rs);

        int ok = svc.backfillVisitorLogs();

        assertThat(ok).isEqualTo(3);
        ArgumentCaptor<EnrichedEvent> cap = ArgumentCaptor.forClass(EnrichedEvent.class);
        verify(rollup, times(3)).upsert(cap.capture());
        assertThat(cap.getAllValues())
                .extracting(EnrichedEvent::eventId)
                .containsExactly("vl:1", "vl:2", "vl:3");
        assertThat(cap.getAllValues())
                .allMatch(e -> "page_view".equals(e.eventType()));
    }

    @Test
    void backfillVisitorClicksRunsEveryRowThroughThePipeline() throws SQLException {
        ResultSet rs = mockVisitorClickResultSet();
        stubJdbcQuery(rs);

        int ok = svc.backfillVisitorClicks();

        assertThat(ok).isEqualTo(2);
        ArgumentCaptor<EnrichedEvent> cap = ArgumentCaptor.forClass(EnrichedEvent.class);
        verify(rollup, times(2)).upsert(cap.capture());
        assertThat(cap.getAllValues())
                .extracting(EnrichedEvent::eventId)
                .containsExactly("vc:1", "vc:2");
        assertThat(cap.getAllValues()).allMatch(e -> "click".equals(e.eventType()));
    }

    @Test
    void rowsWithBothTimestampsNullAreSkipped() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getLong("id")).thenReturn(99L);
        when(rs.getTimestamp(anyString())).thenReturn(null);
        when(rs.getString(anyString())).thenReturn(null);
        when(rs.getObject(anyString())).thenReturn(null);
        stubJdbcQuery(rs);

        int ok = svc.backfillVisitorLogs();

        assertThat(ok).isZero();
        verify(rollup, times(0)).upsert(any());
    }

    @Test
    void rowFailureIsLoggedAndCountedAsBadButDoesNotAbort() throws SQLException {
        ResultSet rs = mockVisitorLogResultSet(2);
        stubJdbcQuery(rs);
        // First call throws; second succeeds.
        doThrow(new RuntimeException("boom")).doNothing().when(rollup).upsert(any());

        int ok = svc.backfillVisitorLogs();

        assertThat(ok).isEqualTo(1);
        verify(rollup, atLeastOnce()).upsert(any());
    }

    /** Drive a real RowMapper against a mocked ResultSet that walks {@code rs.next()}. */
    private void stubJdbcQuery(ResultSet rs) {
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            RowMapper<RawEvent> rm = (RowMapper<RawEvent>) inv.getArgument(1);
            List<RawEvent> out = new ArrayList<>();
            int row = 0;
            while (rs.next()) out.add(rm.mapRow(rs, row++));
            return out;
        }).when(jdbc).query(anyString(), any(RowMapper.class));
    }

    private static ResultSet mockVisitorLogResultSet(int rows) {
        try {
            ResultSet rs = mock(ResultSet.class);
            Boolean[] nexts = new Boolean[rows + 1];
            for (int i = 0; i < rows; i++) nexts[i] = true;
            nexts[rows] = false;
            when(rs.next()).thenReturn(nexts[0],
                    java.util.Arrays.copyOfRange(nexts, 1, nexts.length));
            // ids 1..rows on consecutive rows
            Long[] ids = new Long[rows];
            for (int i = 0; i < rows; i++) ids[i] = (long) (i + 1);
            if (rows == 1) {
                when(rs.getLong("id")).thenReturn(ids[0]);
            } else {
                when(rs.getLong("id")).thenReturn(ids[0],
                        java.util.Arrays.copyOfRange(ids, 1, ids.length));
            }
            Timestamp eventTs = Timestamp.from(Instant.parse("2025-01-01T00:00:00Z"));
            when(rs.getTimestamp("local_time")).thenReturn(eventTs);
            when(rs.getTimestamp("created_at")).thenReturn(eventTs);
            when(rs.getString("ip")).thenReturn("1.2.3.4");
            when(rs.getString("ua")).thenReturn("Mozilla/5.0");
            when(rs.getString("country")).thenReturn("US");
            when(rs.getString("region")).thenReturn("UT");
            when(rs.getString("city")).thenReturn("Salt Lake City");
            when(rs.getObject("latitude")).thenReturn(40.76);
            when(rs.getObject("longitude")).thenReturn(-111.89);
            return rs;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static ResultSet mockVisitorClickResultSet() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getLong("id")).thenReturn(1L, 2L);
        Timestamp eventTs = Timestamp.from(Instant.parse("2025-01-01T00:00:00Z"));
        when(rs.getTimestamp("local_time")).thenReturn(eventTs);
        when(rs.getTimestamp("created_at")).thenReturn(eventTs);
        when(rs.getString("ip")).thenReturn("9.9.9.9");
        when(rs.getString("ua")).thenReturn("Mozilla/5.0");
        when(rs.getString("country")).thenReturn("US");
        when(rs.getString("region")).thenReturn("CA");
        when(rs.getString("city")).thenReturn("San Francisco");
        when(rs.getString("target_url")).thenReturn("https://yuqi.site/works");
        when(rs.getString("click_event")).thenReturn("nav_click");
        when(rs.getString("event")).thenReturn("click");
        when(rs.getObject("latitude")).thenReturn(37.77);
        when(rs.getObject("longitude")).thenReturn(-122.42);
        return rs;
    }
}
