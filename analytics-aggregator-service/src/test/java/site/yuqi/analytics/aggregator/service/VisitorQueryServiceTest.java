package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorQuery;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorSummary;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VisitorQueryServiceTest {

    @Test
    void groupsVisitsWithBoundedParameterizedFiltersBeforePagination() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(String.class)))
                .thenReturn("{\"view\":\"visits\",\"items\":[],\"page\":{\"totalElements\":0}}");
        VisitorQueryService service = new VisitorQueryService(jdbc, new ObjectMapper());
        assertThat(service.visits(new VisitorQuery("yuqi.site",
                Instant.parse("2026-10-11T00:00:00Z"), Instant.parse("2026-10-12T00:00:00Z"),
                "x%' OR 1=1 --", null, null, null, null, null, null, null, "session-1",
                VisitorQueryService.BotFilter.EXCLUDE, false, "/admin", 2, 25)).get("view").asText())
                .isEqualTo("visits");
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<MapSqlParameterSource> params = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        verify(jdbc).queryForObject(sql.capture(), params.capture(), eq(String.class));
        assertThat(sql.getValue()).contains("group by visit_key", "limit :limit offset :offset",
                "r.site_id = :siteId", ":sessionId", ":query", ":excludedPathRoot", ":loopbackUrlPattern", "b.is_bot = false")
                .doesNotContain("x%' OR 1=1 --", "/* visitor_filters */");
        assertThat(params.getValue().getValue("offset")).isEqualTo(50L);
        assertThat(params.getValue().getValue("limit")).isEqualTo(25);
        assertThat(params.getValue().getValue("sessionId")).isEqualTo("session-1");
    }

    @Test
    void serializesVisitorTimestampsAsIso8601Strings() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        Instant timestamp = Instant.parse("2026-07-17T16:00:00Z");
        VisitorQueryService.VisitorLogItem item = new VisitorQueryService.VisitorLogItem(
                "event-1", "page_view", timestamp, timestamp,
                "session-1", "anon-1", "/", null, null, "ua", "127.0.0.1",
                "US", "UT", "Lehi", null, null,
                "desktop", "Chrome", "macOS", false, Map.of());

        String json = mapper.writeValueAsString(item);

        assertThat(json).contains(
                "\"eventTime\":\"2026-07-17T16:00:00Z\"",
                "\"serverTime\":\"2026-07-17T16:00:00Z\"");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void bindsSearchTextInsteadOfInterpolatingItIntoSql() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Long.class)))
                .thenReturn(0L);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(new VisitorSummary(0, 0, 0, 0));
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(List.of());
        VisitorQueryService service = new VisitorQueryService(jdbc, new ObjectMapper());
        String untrusted = "x%' OR 1=1 --";

        service.query(new VisitorQuery(
                "yuqi.site",
                Instant.parse("2026-07-01T00:00:00Z"),
                Instant.parse("2026-07-02T00:00:00Z"),
                untrusted,
                null,
                "/blog_100%",
                null,
                null,
                null,
                null,
                null,
                null,
                VisitorQueryService.BotFilter.ALL,
                false,
                "/admin",
                0,
                50));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<MapSqlParameterSource> params = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        verify(jdbc).query(sql.capture(), params.capture(), any(RowMapper.class));

        assertThat(sql.getValue()).doesNotContain(untrusted).contains(
                ":query", ":path", "nullif(b.page_path, '')", "regexp_replace(coalesce(r.page_url, '')",
                "coalesce(r.page_url, '') ~* :loopbackUrlPattern",
                "coalesce(r.target_url, '') ~* :loopbackUrlPattern",
                "coalesce(r.referrer, '') ~* :loopbackUrlPattern",
                "coalesce(r.ip_address, '') ~* :loopbackIpPattern",
                ":excludedPathRoot",
                ":excludedPathChildren", "limit :limit offset :offset");
        assertThat(params.getValue().getValue("query")).isEqualTo("%x\\%' or 1=1 --%");
        assertThat(params.getValue().getValue("path")).isEqualTo("%/blog\\_100\\%%");
        assertThat(params.getValue().getValue("excludedPathRoot")).isEqualTo("/admin");
        assertThat(params.getValue().getValue("excludedPathChildren")).isEqualTo("/admin/%");
        assertThat(params.getValue().getValue("loopbackUrlPattern").toString())
                .contains("localhost", "127", "::1");
        assertThat(params.getValue().getValue("loopbackIpPattern").toString())
                .contains("127", "::1");
        assertThat(params.getValue().getValue("limit")).isEqualTo(50);
        assertThat(params.getValue().getValue("offset")).isEqualTo(0L);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void keepsAdminTrafficWhenItIsExplicitlyIncluded() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Long.class)))
                .thenReturn(0L);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(new VisitorSummary(0, 0, 0, 0));
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(List.of());
        VisitorQueryService service = new VisitorQueryService(jdbc, new ObjectMapper());

        service.query(new VisitorQuery(
                "yuqi.site",
                Instant.parse("2026-07-01T00:00:00Z"),
                Instant.parse("2026-07-02T00:00:00Z"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                VisitorQueryService.BotFilter.ALL,
                true,
                "/admin",
                0,
                50));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<MapSqlParameterSource> params = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        verify(jdbc).query(sql.capture(), params.capture(), any(RowMapper.class));

        assertThat(sql.getValue()).doesNotContain(":excludedPathRoot", ":excludedPathChildren");
        assertThat(params.getValue().hasValue("excludedPathRoot")).isFalse();
        assertThat(params.getValue().hasValue("excludedPathChildren")).isFalse();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void appliesBotClassificationToCountsSummaryAndItems() {
        NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Long.class)))
                .thenReturn(0L);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(new VisitorSummary(0, 0, 0, 0));
        when(jdbc.query(anyString(), any(MapSqlParameterSource.class), any(RowMapper.class)))
                .thenReturn(List.of());
        VisitorQueryService service = new VisitorQueryService(jdbc, new ObjectMapper());

        service.query(new VisitorQuery(
                "yuqi.site",
                Instant.parse("2026-07-01T00:00:00Z"),
                Instant.parse("2026-07-02T00:00:00Z"),
                null, null, null, null, null, null, null, null, null,
                VisitorQueryService.BotFilter.EXCLUDE,
                false, "/admin", 0, 50));

        ArgumentCaptor<String> countSql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> summarySql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> itemsSql = ArgumentCaptor.forClass(String.class);
        verify(jdbc).queryForObject(
                countSql.capture(), any(MapSqlParameterSource.class), eq(Long.class));
        verify(jdbc).queryForObject(
                summarySql.capture(), any(MapSqlParameterSource.class), any(RowMapper.class));
        verify(jdbc).query(
                itemsSql.capture(), any(MapSqlParameterSource.class), any(RowMapper.class));

        assertThat(List.of(countSql.getValue(), summarySql.getValue(), itemsSql.getValue()))
                .allMatch(statement -> statement.contains("and b.is_bot = false"));
    }
}
