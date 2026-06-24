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
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("country", "US", "count", 42L, "lat", 39.83, "lng", -98.58)));

        List<Map<String, Object>> out = ctrl.markers(30);

        assertThat(out).hasSize(1);
        assertThat(out.get(0)).containsEntry("country", "US").containsEntry("count", 42L);
    }

    @Test
    void summaryReturnsCompositeMap() {
        when(jdbc.queryForMap(anyString(), any(Object[].class)))
                .thenReturn(Map.of("events", 100L, "clicks", 10L, "pageViews", 90L));
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("country", "US", "count", 10L)));

        Map<String, Object> out = ctrl.summary(7);

        assertThat(out)
                .containsEntry("siteId", "yuqi.site")
                .containsEntry("days", 7);
        assertThat(out).containsKey("totals");
        assertThat(out).containsKey("topCountries");
        assertThat(out).containsKey("topDevices");
        assertThat(out).containsKey("timeSeries");
    }
}
