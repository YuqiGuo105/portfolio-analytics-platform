package site.yuqi.analytics.aggregator.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.server.ResponseStatusException;
import site.yuqi.analytics.aggregator.service.JourneyIntentProjectionService;
import site.yuqi.analytics.aggregator.service.ContentIntentMetadataService;
import site.yuqi.analytics.aggregator.service.VisitorIntelligenceService;
import site.yuqi.analytics.aggregator.service.VisitorIntelligenceService.VisitorIntelligenceOverview;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdminVisitorIntelligenceControllerTest {

    private VisitorIntelligenceService intelligence;
    private JourneyIntentProjectionService projection;
    private ContentIntentMetadataService contentMetadata;
    private AdminVisitorIntelligenceController controller;

    @BeforeEach
    void setUp() {
        intelligence = mock(VisitorIntelligenceService.class);
        projection = mock(JourneyIntentProjectionService.class);
        contentMetadata = mock(ContentIntentMetadataService.class);
        controller = new AdminVisitorIntelligenceController(
                intelligence, projection, contentMetadata, "yuqi.site", 31, 2000, "/admin/");
        when(intelligence.overview(anyString(), any(), any(), anyBoolean(), anyString()))
                .thenReturn(new VisitorIntelligenceOverview(
                        Instant.EPOCH, Instant.EPOCH, 0, 0, null,
                        List.of(), List.of(), List.of(), null, List.of(), List.of()));
    }

    @Test
    void buildsBoundedOverviewWindowAndExcludesAdminTraffic() {
        controller.overview(
                "2026-07-01T00:00:00Z", "2026-07-08T00:00:00Z", null, false);

        ArgumentCaptor<Instant> from = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> to = ArgumentCaptor.forClass(Instant.class);
        verify(intelligence).overview(
                org.mockito.ArgumentMatchers.eq("yuqi.site"),
                from.capture(),
                to.capture(),
                org.mockito.ArgumentMatchers.eq(false),
                org.mockito.ArgumentMatchers.eq("/admin"));
        assertThat(Duration.between(from.getValue(), to.getValue())).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void rejectsUnboundedOverviewAndReplay() {
        assertThatThrownBy(() -> controller.overview(null, null, 745, false))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("hours exceeds");

        assertThatThrownBy(() -> controller.replay(null, null, 24, 2001))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("maxSessions");
    }

    @Test
    void replayUsesCanonicalWindow() {
        when(projection.replay(anyString(), any(), any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(new JourneyIntentProjectionService.ReplayResult(4, 2, 2));

        JourneyIntentProjectionService.ReplayResult result =
                controller.replay("2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z", null, 100);

        assertThat(result.insertedSteps()).isEqualTo(4);
        verify(projection).replay(
                org.mockito.ArgumentMatchers.eq("yuqi.site"),
                org.mockito.ArgumentMatchers.eq(Instant.parse("2026-07-01T00:00:00Z")),
                org.mockito.ArgumentMatchers.eq(Instant.parse("2026-07-02T00:00:00Z")),
                org.mockito.ArgumentMatchers.eq(100));
    }

    @Test
    void rejectsInvalidContentMetadataBeforeWriting() {
        ContentIntentMetadataService.ContentIntentMetadataInput input =
                new ContentIntentMetadataService.ContentIntentMetadataInput(
                        "project", "id-1", "/work-single/id-1", "Project", null,
                        "career_interest", List.of("Java"), "ADVANCED",
                        101, "backend", 10, 1, true);

        assertThatThrownBy(() -> controller.upsertContentMetadata(input))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("between 0 and 100");
    }
}
