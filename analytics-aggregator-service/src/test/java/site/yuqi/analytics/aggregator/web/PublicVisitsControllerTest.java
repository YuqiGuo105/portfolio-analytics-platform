package site.yuqi.analytics.aggregator.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PublicVisitsControllerTest {

    private JdbcTemplate jdbc;
    private PublicVisitsController ctrl;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        ctrl = new PublicVisitsController(jdbc);
        org.springframework.test.util.ReflectionTestUtils.setField(ctrl, "siteId", "yuqi.site");
    }

    @Test
    void markersReturnsAggregatedRows() {
        // Endpoint signature is now (window, days, level, latMin, latMax, lngMin, lngMax, limit) —
        // pass legacy days=30 with everything else null to exercise the
        // backward-compat path through resolveWindow().
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("country", "US", "count", 42L, "lat", 39.83, "lng", -98.58)));

        List<Map<String, Object>> out = ctrl.markers(
                null,                 // window
                Integer.valueOf(30),  // days (legacy)
                null,                 // level
                null, null, null, null, // bbox
                null);                // limit

        assertThat(out).hasSize(1);
        assertThat(out.get(0)).containsEntry("country", "US").containsEntry("count", 42L);
    }

    @Test
    void summaryReturnsCompositeMap() {
        when(jdbc.queryForMap(anyString(), any(Object[].class)))
                .thenReturn(Map.of("events", 100L, "clicks", 10L, "pageViews", 90L));
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("country", "US", "count", 10L)));

        Map<String, Object> out = ctrl.summary(null, Integer.valueOf(7));

        assertThat(out)
                .containsEntry("siteId", "yuqi.site")
                .containsEntry("days", 7);
        assertThat(out).containsKey("totals");
        assertThat(out).containsKey("topCountries");
        assertThat(out).containsKey("topDevices");
        assertThat(out).containsKey("timeSeries");
    }

    @Test
    void summaryAcceptsWindowParam() {
        // window="all" takes precedence over days; resolveWindow exposes
        // it as days=0 in the response payload.
        when(jdbc.queryForMap(anyString(), any(Object[].class)))
                .thenReturn(Map.of("events", 1L, "clicks", 0L, "pageViews", 1L));
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of());

        Map<String, Object> out = ctrl.summary("all", null);

        assertThat(out).containsEntry("window", "all").containsEntry("days", 0);
    }
}
