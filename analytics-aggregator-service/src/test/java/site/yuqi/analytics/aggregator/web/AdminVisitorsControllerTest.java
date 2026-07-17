package site.yuqi.analytics.aggregator.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.server.ResponseStatusException;
import site.yuqi.analytics.aggregator.service.VisitorQueryService;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.PageInfo;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorQuery;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorQueryResult;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorSummary;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AdminVisitorsControllerTest {

    private VisitorQueryService service;
    private AdminVisitorsController controller;

    @BeforeEach
    void setUp() {
        service = mock(VisitorQueryService.class);
        controller = new AdminVisitorsController(service, "yuqi.site", 31, 100, "/admin/");
    }

    @Test
    void buildsBoundedStructuredQuery() {
        VisitorQueryResult result = new VisitorQueryResult(
                List.of(), new VisitorSummary(0, 0, 0, 0), new PageInfo(2, 25, 0, 0), null, null);
        when(service.query(org.mockito.ArgumentMatchers.any())).thenReturn(result);

        controller.visitors(
                "2026-07-01T00:00:00Z", "2026-07-08T00:00:00Z", null,
                "Seattle", "page_view", "/blog", "US", "Seattle", "desktop",
                "Chrome", "google.com", "session-1", false, 2, 25);

        ArgumentCaptor<VisitorQuery> captor = ArgumentCaptor.forClass(VisitorQuery.class);
        verify(service).query(captor.capture());
        VisitorQuery query = captor.getValue();
        assertThat(query.siteId()).isEqualTo("yuqi.site");
        assertThat(query.query()).isEqualTo("Seattle");
        assertThat(query.event()).isEqualTo("page_view");
        assertThat(query.includeAdminTraffic()).isFalse();
        assertThat(query.excludedPathPrefix()).isEqualTo("/admin");
        assertThat(query.page()).isEqualTo(2);
        assertThat(query.size()).isEqualTo(25);
    }

    @Test
    void rejectsRangeBeyondRawRetentionWindow() {
        assertThatThrownBy(() -> controller.visitors(
                "2026-01-01T00:00:00Z", "2026-03-15T00:00:00Z", null,
                null, null, null, null, null, null, null, null, null, false, 0, 50))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("query range exceeds");
    }

    @Test
    void rejectsOversizedPageAndUnboundedText() {
        assertThatThrownBy(() -> controller.visitors(
                null, null, 24, null, null, null, null, null, null, null, null, null, false, 0, 101))
                .isInstanceOf(ResponseStatusException.class);

        String oversized = "x".repeat(201);
        assertThatThrownBy(() -> controller.visitors(
                null, null, 24, oversized, null, null, null, null, null, null, null, null, false, 0, 50))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("q is too long");
    }
}
