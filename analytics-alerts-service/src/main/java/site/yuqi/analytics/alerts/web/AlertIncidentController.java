package site.yuqi.analytics.alerts.web;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.alerts.dto.AlertIncidentPage;
import site.yuqi.analytics.alerts.repo.AlertIncidentRepository;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@RestController
@RequestMapping("/api/incidents")
@RequiredArgsConstructor
public class AlertIncidentController {

    private final AlertIncidentRepository incidents;

    @GetMapping
    public AlertIncidentPage list(
            @RequestParam(defaultValue = "24") int hours,
            @RequestParam(required = false) String siteId,
            @RequestParam(required = false) Long ruleId,
            @RequestParam(required = false) Boolean notified,
            @RequestParam(defaultValue = "50") int limit) {
        int safeHours = Math.max(1, Math.min(hours, 24 * 90));
        int safeLimit = Math.max(1, Math.min(limit, 200));
        return incidents.findRecent(
                Instant.now().minus(safeHours, ChronoUnit.HOURS),
                siteId,
                ruleId,
                notified,
                safeLimit);
    }
}
