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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VisitorQueryServiceTest {

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
                0,
                50));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<MapSqlParameterSource> params = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        verify(jdbc).query(sql.capture(), params.capture(), any(RowMapper.class));

        assertThat(sql.getValue()).doesNotContain(untrusted).contains(":query", ":path", "limit :limit offset :offset");
        assertThat(params.getValue().getValue("query")).isEqualTo("%x\\%' or 1=1 --%");
        assertThat(params.getValue().getValue("path")).isEqualTo("%/blog\\_100\\%%");
        assertThat(params.getValue().getValue("limit")).isEqualTo(50);
        assertThat(params.getValue().getValue("offset")).isEqualTo(0L);
    }
}
