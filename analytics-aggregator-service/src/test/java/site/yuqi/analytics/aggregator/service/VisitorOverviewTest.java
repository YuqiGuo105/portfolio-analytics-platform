package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorQuery;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.BotFilter;
import site.yuqi.analytics.aggregator.web.AdminVisitorsController;

import java.time.Instant;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VisitorOverviewTest {
    @Test
    void overviewUsesAllFiltersWithoutPaginationAndKeepsSqlParameterized() throws Exception {
        var jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(String.class)))
                .thenReturn("{\"summary\":{\"events\":150},\"timeline\":[]}");
        var service = new VisitorQueryService(jdbc, new ObjectMapper());
        var controller = new AdminVisitorsController(service, "yuqi.site", 31, 100, "/admin");
        var result = controller.overview("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z", 168,
                "x%' OR 1=1 --", "page_view", "/blogs", "IN", "Jaipur", "mobile", "Safari", "linkedin.com",
                "session-one", "EXCLUDE", false);
        var sql = ArgumentCaptor.forClass(String.class);
        var params = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        verify(jdbc).queryForObject(sql.capture(), params.capture(), eq(String.class));
        assertThat(sql.getValue()).contains("b.is_bot = false", ":siteId", ":country", ":city", ":device", ":browser",
                ":event", ":path", ":referrer", ":sessionId", ":query", ":excludedPathRoot", ":loopbackIpPattern");
        assertThat(sql.getValue()).doesNotContain("OR 1=1", ":limit", ":offset", "/* visitor_filters */");
        assertThat(params.getValue().getValue("bucket")).isEqualTo("day");
        assertThat(params.getValue().getValue("country")).isEqualTo("in");
        assertThat(result).containsKey("summary");
        var transaction = VisitorQueryService.class.getMethod("overview", VisitorQuery.class).getAnnotation(Transactional.class);
        assertThat(transaction.readOnly()).isTrue();
        assertThat(transaction.timeout()).isEqualTo(8);
    }

    @Test
    void overviewRejectsUnboundedOrInvalidRequestsBeforeQuerying() {
        var service = mock(VisitorQueryService.class);
        var controller = new AdminVisitorsController(service, "yuqi.site", 31, 100, "/admin");
        for (int hours : new int[]{0, 745}) assertThatThrownBy(() -> controller.overview(
                null, null, hours, null, null, null, null, null, null, null, null, null, "ALL", false))
                .hasMessageContaining("hours exceeds");
        assertThatThrownBy(() -> controller.overview(null, null, 24, null, null, null, null, null, null,
                null, null, null, "HUMAN", false)).hasMessageContaining("bot must be");
        verifyNoInteractions(service);
    }

    @Test
    void hourlyOverviewDoesNotExcludeUnknownWithoutAnExplicitBotFilter() {
        var jdbc = mock(NamedParameterJdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(String.class))).thenReturn("{}");
        var service = new VisitorQueryService(jdbc, new ObjectMapper());
        service.overview(new VisitorQuery("yuqi.site", Instant.parse("2026-10-01T00:00:00Z"),
                Instant.parse("2026-10-02T00:00:00Z"), null, null, null, null, null, null,
                null, null, null, BotFilter.ALL, true, "/admin", 99, 1));
        var params = ArgumentCaptor.forClass(MapSqlParameterSource.class);
        var sql = ArgumentCaptor.forClass(String.class);
        verify(jdbc).queryForObject(sql.capture(), params.capture(), eq(String.class));
        assertThat(params.getValue().getValue("bucket")).isEqualTo("hour");
        assertThat(sql.getValue()).doesNotContain("and b.is_bot", ":excludedPathRoot");
    }
}
