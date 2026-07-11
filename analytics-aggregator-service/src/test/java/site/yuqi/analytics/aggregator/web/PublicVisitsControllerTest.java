package site.yuqi.analytics.aggregator.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.ResponseEntity;
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
        // Build a disabled ResponseCache so all calls fall through to DB
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        ResponseCache cache = new ResponseCache(redis, new ObjectMapper(), false, 30);
        ctrl = new PublicVisitsController(jdbc, cache);
        org.springframework.test.util.ReflectionTestUtils.setField(ctrl, "siteId", "yuqi.site");
        org.springframework.test.util.ReflectionTestUtils.setField(ctrl, "minBucketCount", 5);
    }

    @SuppressWarnings("unchecked")
    @Test
    void markersReturnsAggregatedRows() {
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("country", "US", "count", 42L, "lat", 39.83, "lng", -98.58)));

        ResponseEntity<?> resp = ctrl.markers(
                null,                 // window
                Integer.valueOf(30),  // days (legacy)
                null,                 // level
                null, null, null, null, // bbox
                null,                 // limit
                null);                // If-None-Match

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        List<Map<String, Object>> out = (List<Map<String, Object>>) resp.getBody();
        assertThat(out).hasSize(1);
        assertThat(out.get(0)).containsEntry("country", "US").containsEntry("count", 42L);
    }

    @SuppressWarnings("unchecked")
    @Test
    void summaryReturnsCompositeMap() {
        when(jdbc.queryForMap(anyString(), any(Object[].class)))
                .thenReturn(Map.of("events", 100L, "clicks", 10L, "pageViews", 90L));
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("country", "US", "count", 10L)));

        ResponseEntity<?> resp = ctrl.summary(null, Integer.valueOf(7), null);
        assertThat(resp.getStatusCode().value()).isEqualTo(200);

        Map<String, Object> out = (Map<String, Object>) resp.getBody();
        assertThat(out)
                .containsEntry("siteId", "yuqi.site")
                .containsEntry("days", 7);
        assertThat(out).containsKey("totals");
        assertThat(out).containsKey("topCountries");
        assertThat(out).containsKey("topDevices");
        assertThat(out).containsKey("timeSeries");
    }

    @SuppressWarnings("unchecked")
    @Test
    void summaryAcceptsWindowParam() {
        when(jdbc.queryForMap(anyString(), any(Object[].class)))
                .thenReturn(Map.of("events", 1L, "clicks", 0L, "pageViews", 1L));
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of());

        ResponseEntity<?> resp = ctrl.summary("all", null, null);
        Map<String, Object> out = (Map<String, Object>) resp.getBody();
        assertThat(out).containsEntry("window", "all").containsEntry("days", 0);
    }

    @SuppressWarnings("unchecked")
    @Test
    void topPagesReturnsOnlyBackendFilteredBuckets() {
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("page", "/blog/a", "views", 12L, "uniqueSessions", 8L)));

        ResponseEntity<?> resp = ctrl.topPages("7d", null, null);
        Map<String, Object> out = (Map<String, Object>) resp.getBody();

        assertThat(out).containsEntry("window", "7d").containsEntry("minBucketCount", 5);
        assertThat((List<?>) out.get("items")).hasSize(1);
    }

    @SuppressWarnings("unchecked")
    @Test
    void referrersNeverReturnsFullUrls() {
        when(jdbc.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("referrer", "google.com", "count", 9L)));

        Map<String, Object> out = (Map<String, Object>) ctrl.referrers("7d", null, null).getBody();
        Map<String, Object> first = (Map<String, Object>) ((List<?>) out.get("items")).get(0);
        assertThat(first).containsEntry("referrer", "google.com");
    }

    @SuppressWarnings("unchecked")
    @Test
    void engagementComputesCompletionRate() {
        when(jdbc.queryForMap(anyString(), any(Object[].class))).thenReturn(Map.of(
                "contentSessions", 10L, "completedSessions", 4L, "engagedSeconds", 300L));

        Map<String, Object> body = (Map<String, Object>) ctrl.engagement("30d", null).getBody();
        Map<String, Object> totals = (Map<String, Object>) body.get("totals");
        assertThat(totals).containsEntry("completionRate", 0.4d);
    }

    @SuppressWarnings("unchecked")
    @Test
    void recommendationsComputesCtrAndSuppressesItemsInSql() {
        when(jdbc.queryForMap(anyString(), any(Object[].class)))
                .thenReturn(Map.of("impressions", 20L, "clicks", 5L, "dismissals", 1L));
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        Map<String, Object> body = (Map<String, Object>) ctrl.recommendations("30d", null).getBody();
        Map<String, Object> totals = (Map<String, Object>) body.get("totals");
        assertThat(totals).containsEntry("clickThroughRate", 0.25d);
    }
}
