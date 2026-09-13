package site.yuqi.analytics.alerts.web;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import site.yuqi.analytics.alerts.service.RuleTemplateService;
import site.yuqi.analytics.alerts.service.VisitorIntelligenceService;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class VisitorIntelligenceControllerTest {
    @Test void omittedEventDefaultsToPersistedPageViewName() throws Exception {
        var service = mock(VisitorIntelligenceService.class);
        var mvc = MockMvcBuilders.standaloneSetup(new VisitorIntelligenceController(service, mock(RuleTemplateService.class))).build();
        mvc.perform(get("/api/visitor-intelligence/segment-preview").param("siteId", "yuqi.site"))
                .andExpect(status().isOk());
        verify(service).segmentPreview("yuqi.site", "page_view", "REGION", null, 24, 20);
    }

    @Test void invalidWindowReturnsClientErrorInsteadOfServerFailure() throws Exception {
        var service = mock(VisitorIntelligenceService.class);
        when(service.segmentPreview("yuqi.site", "page_view", "REGION", null, 2161, 20))
                .thenThrow(new IllegalArgumentException("hours must be between 1 and 2160"));
        var mvc = MockMvcBuilders.standaloneSetup(new VisitorIntelligenceController(service, mock(RuleTemplateService.class))).build();
        mvc.perform(get("/api/visitor-intelligence/segment-preview").param("siteId", "yuqi.site").param("hours", "2161"))
                .andExpect(status().isBadRequest());
    }
}
