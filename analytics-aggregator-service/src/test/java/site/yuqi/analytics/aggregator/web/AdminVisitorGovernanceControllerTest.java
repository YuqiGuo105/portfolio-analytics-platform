package site.yuqi.analytics.aggregator.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.aggregator.governance.*;

import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class AdminVisitorGovernanceControllerTest {
    private VisitorGovernanceService service;
    private GovernanceAudit audit;
    private MockMvc http;
    private static final String BASE = "/api/admin/visitor-governance";
    private static final String ID = "c7b7c6fc-6466-405e-9b8c-2994e7ac421c";

    @BeforeEach
    void setup() {
        service = mock(VisitorGovernanceService.class);
        audit = mock(GovernanceAudit.class);
        http = build(true);
    }

    private MockMvc build(boolean enabled) {
        return MockMvcBuilders.standaloneSetup(new AdminVisitorGovernanceController(service), new TestVisitors())
                .setControllerAdvice(new GovernanceErrorAdvice(), new GovernanceRequestAdvice())
                .addInterceptors(new GovernanceAuditInterceptor(
                        new GovernancePolicy(enabled, false, 90, 90, "test"), audit))
                .addFilters(new AdminTokenFilter("local-test-token")).build();
    }

    @Test
    void allRoutesRequireTokenAndActorAloneDoesNotAuthenticate() throws Exception {
        http.perform(get(BASE + "/overview").header("X-Admin-Actor", ID)).andExpect(status().isUnauthorized());
        http.perform(get(BASE + "/export").param("batchId", ID)).andExpect(status().isUnauthorized());
        for (String route : new String[]{"archive", "verify", "hold"}) {
            http.perform(post(BASE + "/" + route).contentType("application/json").content("{}"))
                    .andExpect(status().isUnauthorized());
        }
        http.perform(delete(BASE + "/hold").contentType("application/json").content("{}"))
                .andExpect(status().isUnauthorized());
        verifyNoInteractions(service, audit);
    }

    @Test
    void disabledRoutesReturn404WithoutServiceReadsAndPreserveOldReads() throws Exception {
        http = build(false);
        http.perform(get(BASE + "/overview").header("X-Internal-Token", "local-test-token"))
                .andExpect(status().isNotFound()).andExpect(jsonPath("$.error").value("governance_disabled"));
        http.perform(get("/api/admin/visitors").header("X-Internal-Token", "local-test-token"))
                .andExpect(status().isOk());
        verifyNoInteractions(service, audit);
    }

    @Test
    void missingActorUsesServiceFallbackAndDefaultBound() throws Exception {
        when(service.archive("internal-service", 100)).thenReturn(new GovernanceModels.ArchiveResult("empty", null, 0));
        http.perform(post(BASE + "/archive").header("X-Internal-Token", "local-test-token"))
                .andExpect(status().isOk()).andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.status").value("empty")).andExpect(jsonPath("$.archivedCount").value(0))
                .andExpect(content().json("{\"status\":\"empty\",\"batch\":null,\"archivedCount\":0}"));
        verify(service).archive("internal-service", 100);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"limit\":0}", "{\"limit\":501}", "{\"limit\":1.5}", "{\"limit\":\"10\"}",
            "{\"limit\":null}", "{\"limit\":999999999999}", "{\"rawQuery\":\"secret\"}", "[]", "{"})
    void rejectsInvalidArchiveBodies(String body) throws Exception {
        http.perform(post(BASE + "/archive").header("X-Internal-Token", "local-test-token")
                        .contentType("application/json").content(body))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("invalid_request"));
        verify(service, never()).archive(any(), anyInt());
    }

    @Test
    void rejectsEmailsAndInvalidUuidsAndFreeTextReasons() throws Exception {
        http.perform(get(BASE + "/overview").header("X-Internal-Token", "local-test-token")
                        .header("X-Admin-Actor", "person@example.test"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("invalid_actor"));
        http.perform(post(BASE + "/verify").header("X-Internal-Token", "local-test-token")
                        .contentType("application/json").content("{\"batchId\":\"1-1-1-1-1\"}"))
                .andExpect(status().isBadRequest());
        http.perform(post(BASE + "/hold").header("X-Internal-Token", "local-test-token")
                        .contentType("application/json").content("{\"batchId\":\"" + ID + "\",\"reason\":\"person@example.test\"}"))
                .andExpect(status().isBadRequest());
        verify(service, never()).hold(any(), any(), any(), anyBoolean());
    }

    @Test
    void deleteHoldConsumesJsonAndVerifiedActor() throws Exception {
        http.perform(delete(BASE + "/hold").header("X-Internal-Token", "local-test-token")
                        .header("X-Admin-Actor", ID).contentType("application/json")
                        .content("{\"batchId\":\"" + ID + "\",\"reason\":\"data_quality\"}"))
                .andExpect(status().isOk());
        verify(service).hold(ID, UUID.fromString(ID), "data_quality", false);
    }

    @Test
    void readAuditConsumesOnlyFixedMetadataNeverQueryValues() throws Exception {
        http.perform(get("/api/admin/visitors").header("X-Internal-Token", "local-test-token")
                        .header("X-Admin-Actor", "verified-admin").header("X-Admin-Action", "visitor_export")
                        .header("X-Admin-Request-Id", ID).header("X-Admin-Export-Format", "csv")
                        .param("q", "private@example.test").param("ip", "192.0.2.1"))
                .andExpect(status().isOk());
        verify(audit).append("verified-admin", GovernanceAudit.Action.VISITOR_EXPORT, GovernanceAudit.Target.VISITORS,
                null, null, 0, GovernanceAudit.Outcome.REQUESTED, UUID.fromString(ID), "csv");
        verifyNoMoreInteractions(audit);
    }

    @Test
    void invalidAuditActionIsNotStoredAndAuditFailureBlocksRead() throws Exception {
        http.perform(get("/api/admin/visitors").header("X-Internal-Token", "local-test-token")
                        .header("X-Admin-Action", "freeform private text"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(audit);
        doThrow(new DataAccessResourceFailureException("sensitive database detail"))
                .when(audit).append(anyString(), any(), any(), any(), any(), anyInt(), any(), any(), any());
        http.perform(get("/api/admin/visitors").header("X-Internal-Token", "local-test-token"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("governance_unavailable"));
    }

    @RestController
    static class TestVisitors {
        @GetMapping("/api/admin/visitors") public Map<String, Boolean> visitors() { return Map.of("ok", true); }
    }
}
