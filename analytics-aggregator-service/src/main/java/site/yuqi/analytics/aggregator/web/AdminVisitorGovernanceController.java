package site.yuqi.analytics.aggregator.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import site.yuqi.analytics.aggregator.governance.GovernanceAudit;
import site.yuqi.analytics.aggregator.governance.GovernanceException;
import site.yuqi.analytics.aggregator.governance.GovernancePolicy;
import site.yuqi.analytics.aggregator.governance.VisitorGovernanceService;

import java.util.Set;
import java.util.UUID;

import static site.yuqi.analytics.aggregator.governance.GovernanceModels.*;

@RestController
@RequestMapping("/api/admin/visitor-governance")
public class AdminVisitorGovernanceController {
    private final VisitorGovernanceService service;

    public AdminVisitorGovernanceController(VisitorGovernanceService service) { this.service = service; }

    @GetMapping("/overview")
    public Overview overview(@RequestHeader(value = "X-Admin-Actor", required = false) String actor) {
        return service.overview(GovernanceAudit.actor(actor));
    }

    @PostMapping("/archive")
    public ArchiveResult archive(@RequestHeader(value = "X-Admin-Actor", required = false) String actor,
                                 @RequestBody(required = false) JsonNode body) {
        service.requireEnabled();
        int limit = GovernancePolicy.DEFAULT_BATCH;
        if (body != null) {
            fields(body, Set.of("limit"));
            if (body.has("limit")) {
                if (!body.get("limit").isIntegralNumber() || !body.get("limit").canConvertToInt()) {
                    throw GovernanceException.invalid();
                }
                limit = body.get("limit").intValue();
            }
        }
        if (limit < 1 || limit > GovernancePolicy.MAX_BATCH) throw GovernanceException.invalid();
        return service.archive(GovernanceAudit.actor(actor), limit);
    }

    @PostMapping("/verify")
    public Verification verify(@RequestHeader(value = "X-Admin-Actor", required = false) String actor,
                                @RequestBody JsonNode body) {
        service.requireEnabled();
        fields(body, Set.of("batchId"));
        return service.verify(GovernanceAudit.actor(actor), uuid(text(body, "batchId")));
    }

    @GetMapping("/export")
    public ResponseEntity<ExportResult> export(
            @RequestHeader(value = "X-Admin-Actor", required = false) String actor,
            @RequestParam String batchId) {
        service.requireEnabled();
        UUID id = uuid(batchId);
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"visitor-evidence-" + id + ".json\"")
                .body(service.export(GovernanceAudit.actor(actor), id));
    }

    @PostMapping("/hold")
    public HoldResult hold(@RequestHeader(value = "X-Admin-Actor", required = false) String actor,
                            @RequestBody JsonNode body) {
        return updateHold(actor, body, true);
    }

    @DeleteMapping("/hold")
    public HoldResult releaseHold(@RequestHeader(value = "X-Admin-Actor", required = false) String actor,
                                   @RequestBody JsonNode body) {
        return updateHold(actor, body, false);
    }

    private HoldResult updateHold(String actor, JsonNode body, boolean held) {
        service.requireEnabled();
        fields(body, Set.of("batchId", "reason"));
        String reason = text(body, "reason");
        if (!Set.of("incident_review", "legal_preservation", "data_quality").contains(reason)) {
            throw GovernanceException.invalid();
        }
        return service.hold(GovernanceAudit.actor(actor), uuid(text(body, "batchId")), reason, held);
    }

    public static UUID uuid(String value) {
        if (value == null || !value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) {
            throw GovernanceException.invalid();
        }
        return UUID.fromString(value);
    }

    private static String text(JsonNode body, String name) {
        if (!body.has(name) || !body.get(name).isTextual()) throw GovernanceException.invalid();
        return body.get(name).textValue();
    }

    private static void fields(JsonNode body, Set<String> allowed) {
        if (body == null || !body.isObject()) throw GovernanceException.invalid();
        body.fieldNames().forEachRemaining(name -> {
            if (!allowed.contains(name)) throw GovernanceException.invalid();
        });
    }
}
