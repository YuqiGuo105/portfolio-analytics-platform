package site.yuqi.analytics.alerts.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.alerts.dto.AlertRulePatch;
import site.yuqi.analytics.alerts.dto.PrepareChangeRequest;
import site.yuqi.analytics.alerts.dto.PreparedChange;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class RuleTemplateService {
    private final AlertRuleChangeService changes;

    public List<Map<String, Object>> list() {
        return List.of(
                template("regional-interest", "Regional visitor interest", "PAGE_VIEW", "REGION", 1, "1d"),
                template("recruiter-company", "Recruiter company traffic", "PAGE_VIEW", "GLOBAL", 3, "1d"),
                template("returning-visitors", "Returning visitor activity", "RETURN_VISIT", "GLOBAL", 2, "1d"),
                template("traffic-anomaly", "Traffic anomaly", "PAGE_VIEW", "GLOBAL", 50, "5m"));
    }

    public PreparedChange createFromTemplate(String templateId, String siteId, String name,
                                              String geoAreaId, Long threshold, String actor) {
        Map<String, Object> template = list().stream().filter(t -> templateId.equals(t.get("id"))).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown template: " + templateId));
        AlertRulePatch patch = new AlertRulePatch(
                name == null || name.isBlank() ? String.valueOf(template.get("name")) : name,
                String.valueOf(template.get("eventType")), String.valueOf(template.get("geoLevel")), geoAreaId,
                String.valueOf(template.get("granularity")), threshold == null ? (Long) template.get("threshold") : threshold,
                ">=", 1800, true, siteId);
        return changes.prepare(new PrepareChangeRequest("CREATE", null, patch,
                "Created from template " + templateId, actor));
    }

    private Map<String, Object> template(String id, String name, String eventType,
                                         String geoLevel, long threshold, String granularity) {
        return Map.of("id", id, "name", name, "eventType", eventType, "geoLevel", geoLevel,
                "threshold", threshold, "granularity", granularity);
    }
}
