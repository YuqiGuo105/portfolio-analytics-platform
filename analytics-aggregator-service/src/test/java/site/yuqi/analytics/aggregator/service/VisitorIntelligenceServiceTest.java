package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VisitorIntelligenceServiceTest {

    @Test
    void buildsCompleteOverviewFromDeterministicReadModels() {
        StubJdbcTemplate jdbc = new StubJdbcTemplate();
        VisitorIntelligenceService service =
                new VisitorIntelligenceService(jdbc, new ObjectMapper(), 24);

        VisitorIntelligenceService.VisitorIntelligenceOverview result = service.overview(
                "yuqi.site",
                Instant.parse("2026-07-01T00:00:00Z"),
                Instant.parse("2026-07-08T00:00:00Z"),
                false,
                "/admin");

        assertThat(result.totalEvents()).isEqualTo(12);
        assertThat(result.uniqueVisitors()).isEqualTo(4);
        assertThat(result.policy().version()).isEqualTo(2);
        assertThat(result.funnel()).hasSize(2);
        assertThat(result.funnel().get(1).fromStartRate()).isEqualTo(0.5);
        assertThat(result.attribution()).singleElement()
                .satisfies(source -> assertThat(source.source()).isEqualTo("GitHub"));
        assertThat(result.topContent()).singleElement()
                .satisfies(content -> assertThat(content.title()).isEqualTo("Platform"));
        assertThat(result.cohort().returningRate()).isEqualTo(0.5);
        assertThat(result.highIntent()).singleElement()
                .satisfies(journey -> {
                    assertThat(journey.intentLevel()).isEqualTo("HIGH");
                    assertThat(journey.dimensionScores()).containsEntry("career_interest", 60);
                    assertThat(journey.steps()).extracting(
                            VisitorIntelligenceService.JourneyStep::eventName)
                            .containsExactly("page_view", "read_progress");
                });
        assertThat(result.recentJourneys()).hasSize(1);
        assertThat(jdbc.journeyStepQueries).isEqualTo(2);
        assertThat(jdbc.journeyQueries)
                .anySatisfy(sql -> assertThat(sql)
                        .contains("and i.intent_level in ('HIGH', 'MEDIUM')\n")
                        .contains("order by i.score desc, s.last_event desc")
                        .doesNotContain("')order", "byi.score"));
    }

    private static final class StubJdbcTemplate extends JdbcTemplate {

        private final List<String> journeyQueries = new ArrayList<>();
        private int journeyStepQueries;

        @Override
        public <T> T queryForObject(String sql, RowMapper<T> mapper, Object... args) {
            if (sql.contains("count(*) as total_events")) {
                return mapped(mapper, row(Map.of(
                        "total_events", 12L,
                        "unique_visitors", 4L)));
            }
            if (sql.contains("with identities as")) {
                return mapped(mapper, row(Map.of(
                        "visitors", 4L,
                        "returning_visitors", 2L,
                        "multi_step_visitors", 3L,
                        "average_events", 3.0)));
            }
            throw new AssertionError("Unexpected queryForObject: " + sql);
        }

        @Override
        public <T> List<T> query(String sql, RowMapper<T> mapper, Object... args) {
            if (sql.contains("from public.visitor_intent_policies")) {
                return List.of(mapped(mapper, row(Map.of(
                        "policy_id", "intent-v2",
                        "version", 2,
                        "name", "Intent policy",
                        "medium_threshold", 25,
                        "high_threshold", 55,
                        "max_score", 100,
                        "activated_at", Timestamp.from(Instant.parse("2026-06-01T00:00:00Z"))))));
            }
            if (sql.contains("from public.visitor_journey_funnel_rules")) {
                return List.of(
                        mapped(mapper, row(Map.of(
                                "step_key", "home",
                                "step_label", "Homepage",
                                "step_description", "Visited homepage",
                                "step_order", 1,
                                "visitors", 4L))),
                        mapped(mapper, row(Map.of(
                                "step_key", "project",
                                "step_label", "Project",
                                "step_description", "Visited a project",
                                "step_order", 2,
                                "visitors", 2L))));
            }
            if (sql.contains("with per_session as")) {
                return List.of(mapped(mapper, row(Map.of(
                        "source", "GitHub",
                        "source_type", "developer",
                        "visitors", 2L,
                        "events", 7L,
                        "quality_score", 68))));
            }
            if (sql.contains("with content_events as")) {
                return List.of(mapped(mapper, row(Map.of(
                        "path", "/work-single/project-1",
                        "content_type", "PROJECT",
                        "title", "Platform",
                        "cover_url", "https://www.yuqi.site/cover.png",
                        "events", 5L,
                        "visitors", 2L,
                        "engaged_seconds", 90L))));
            }
            if (sql.contains("from public.sessions s")
                    && sql.contains("visitor_intent_snapshots")) {
                journeyQueries.add(sql);
                return List.of(mapped(mapper, row(Map.ofEntries(
                        Map.entry("session_id", "session-1"),
                        Map.entry("first_event", Timestamp.from(Instant.parse("2026-07-02T10:00:00Z"))),
                        Map.entry("last_event", Timestamp.from(Instant.parse("2026-07-02T10:02:00Z"))),
                        Map.entry("duration_ms", 120_000L),
                        Map.entry("entry_page", "/"),
                        Map.entry("exit_page", "/work-single/project-1"),
                        Map.entry("device_type", "desktop"),
                        Map.entry("browser", "Chrome"),
                        Map.entry("country", "US"),
                        Map.entry("geo_area_id", "US:UT:Lehi"),
                        Map.entry("policy_version", 2),
                        Map.entry("score", 72),
                        Map.entry("intent_level", "HIGH"),
                        Map.entry("dominant_intent", "career_interest"),
                        Map.entry("dimension_scores", "{\"career_interest\":60,\"exploration\":12}"),
                        Map.entry("contributing_signals", "[{\"signalKey\":\"resume\",\"score\":60}]")))));
            }
            if (sql.contains("from public.visitor_journey_steps")
                    && sql.contains("journey_step_rank")) {
                journeyStepQueries++;
                return List.of(
                        mapped(mapper, row(Map.ofEntries(
                                Map.entry("event_id", "event-2"),
                                Map.entry("session_id", "session-1"),
                                Map.entry("event_name", "read_progress"),
                                Map.entry("event_time", Timestamp.from(Instant.parse("2026-07-02T10:02:00Z"))),
                                Map.entry("page_path", "/work-single/project-1"),
                                Map.entry("target_path", "/work-single/project-1"),
                                Map.entry("content_id", "project-1"),
                                Map.entry("content_type", "PROJECT"),
                                Map.entry("engaged_seconds", 90),
                                Map.entry("progress_percent", 75)))),
                        mapped(mapper, row(Map.ofEntries(
                                Map.entry("event_id", "event-1"),
                                Map.entry("session_id", "session-1"),
                                Map.entry("event_name", "page_view"),
                                Map.entry("event_time", Timestamp.from(Instant.parse("2026-07-02T10:00:00Z"))),
                                Map.entry("page_path", "/"),
                                Map.entry("target_path", ""),
                                Map.entry("content_id", ""),
                                Map.entry("content_type", ""),
                                Map.entry("engaged_seconds", 0),
                                Map.entry("progress_percent", 0)))));
            }
            throw new AssertionError("Unexpected query: " + sql);
        }

        private static <T> T mapped(RowMapper<T> mapper, ResultSet resultSet) {
            try {
                return mapper.mapRow(resultSet, 0);
            } catch (SQLException exception) {
                throw new IllegalStateException(exception);
            }
        }
    }

    private static ResultSet row(Map<String, Object> values) {
        ResultSet resultSet = mock(ResultSet.class);
        try {
            when(resultSet.getString(anyString()))
                    .thenAnswer(invocation -> string(values.get(invocation.getArgument(0))));
            when(resultSet.getLong(anyString()))
                    .thenAnswer(invocation -> number(values.get(invocation.getArgument(0))).longValue());
            when(resultSet.getInt(anyString()))
                    .thenAnswer(invocation -> number(values.get(invocation.getArgument(0))).intValue());
            when(resultSet.getDouble(anyString()))
                    .thenAnswer(invocation -> number(values.get(invocation.getArgument(0))).doubleValue());
            when(resultSet.getTimestamp(anyString()))
                    .thenAnswer(invocation -> values.get(invocation.getArgument(0)));
            when(resultSet.getObject(anyString()))
                    .thenAnswer(invocation -> values.get(invocation.getArgument(0)));
        } catch (SQLException exception) {
            throw new IllegalStateException(exception);
        }
        return resultSet;
    }

    private static String string(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Number number(Object value) {
        return value instanceof Number number ? number : 0;
    }
}
