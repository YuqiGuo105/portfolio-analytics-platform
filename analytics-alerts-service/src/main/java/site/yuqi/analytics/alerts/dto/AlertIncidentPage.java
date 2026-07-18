package site.yuqi.analytics.alerts.dto;

import java.util.List;

public record AlertIncidentPage(
        List<AlertIncident> items,
        AlertIncidentSummary summary
) {}
