package site.yuqi.analytics.alerts.web;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.alerts.dto.AlertRuleRequest;
import site.yuqi.analytics.alerts.dto.PreparedChange;
import site.yuqi.analytics.alerts.service.RuleTemplateService;
import site.yuqi.analytics.alerts.service.VisitorIntelligenceService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/visitor-intelligence")
@RequiredArgsConstructor
public class VisitorIntelligenceController {
    private final VisitorIntelligenceService visitors;
    private final RuleTemplateService templates;

    @GetMapping("/segment-preview")
    public List<Map<String, Object>> preview(
            @RequestParam String siteId, @RequestParam(defaultValue = "PAGE_VIEW") String eventType,
            @RequestParam(defaultValue = "REGION") String geoLevel,
            @RequestParam(required = false) String geoAreaId,
            @RequestParam(defaultValue = "24") int hours, @RequestParam(defaultValue = "20") int limit) {
        return visitors.segmentPreview(siteId, eventType, geoLevel, geoAreaId, hours, limit);
    }

    @PostMapping("/rule-test")
    public Map<String, Object> test(@Valid @RequestBody AlertRuleRequest request) { return visitors.test(request); }

    @GetMapping("/explain-match")
    public Map<String, Object> explain(@RequestParam long ruleId) { return visitors.explain(ruleId); }

    @GetMapping("/rule-templates")
    public List<Map<String, Object>> templates() { return templates.list(); }

    @PostMapping("/rule-templates/prepare")
    public PreparedChange prepareTemplate(@RequestBody TemplateRequest request) {
        return templates.createFromTemplate(request.templateId(), request.siteId(), request.name(),
                request.geoAreaId(), request.threshold(), request.actor());
    }

    public record TemplateRequest(String templateId, String siteId, String name,
                                  String geoAreaId, Long threshold, String actor) {}
}
