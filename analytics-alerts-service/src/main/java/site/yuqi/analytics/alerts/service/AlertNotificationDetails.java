package site.yuqi.analytics.alerts.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.alerts.dto.AlertIncident;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class AlertNotificationDetails {
    private final JdbcTemplate jdbc;
    private final DateTimeFormatter time;

    public AlertNotificationDetails(JdbcTemplate jdbc,
            @Value("${analytics.alerts.display-zone:UTC}") String displayZone) {
        this.jdbc = jdbc;
        this.time = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss VV").withZone(ZoneId.of(displayZone));
    }

    public String summaryFor(AlertIncident incident) {
        String saved = stored(incident.incidentId());
        if (saved != null) return saved;

        Instant end = incident.bucketTime().plusSeconds("1d".equals(incident.granularity()) ? 86400 : 300);
        // Use the rule snapshot and the same event-time window/geo ancestors as the rollup.
        // Only canonical, privacy-safe facts are selected; never raw IPs or coordinates.
        List<Visit> visits = incident.measuredValue() == 0 ? List.of() : jdbc.query("""
                select e.event_id, e.event_name, e.event_time, e.server_time, e.page_path,
                       e.device_type, e.browser, e.country, e.region,
                       case when g.geo_level = 'METRO' then g.name end as city
                from incidents i
                join public.behavior_events e on e.site_id = i.site_id
                  and e.event_name = i.rule_snapshot->>'eventType'
                left join public.geo_areas g on g.geo_area_id = e.geo_area_id
                cross join lateral (select case i.rule_snapshot->>'geoLevel'
                    when 'GLOBAL' then 'GLOBAL'
                    when 'COUNTRY' then 'COUNTRY:' || nullif(e.country, '')
                    when 'REGION' then 'REGION:' || nullif(e.country, '') || ':' || nullif(e.region, '')
                    when 'METRO' then case when e.geo_area_id like 'METRO:%' then e.geo_area_id end
                    end as area) scope
                where i.incident_id = ? and e.event_time >= ? and e.event_time < ?
                  and scope.area is not null
                  and (coalesce(i.rule_snapshot->>'geoAreaId', '') = ''
                       or scope.area = i.rule_snapshot->>'geoAreaId')
                order by e.event_time desc, e.event_id
                limit 3
                """, (rs, row) -> new Visit(
                rs.getString("event_id"), rs.getString("event_name"),
                rs.getTimestamp("event_time").toInstant(), rs.getTimestamp("server_time").toInstant(),
                rs.getString("page_path"), rs.getString("city"), rs.getString("region"),
                rs.getString("country"), rs.getString("device_type"), rs.getString("browser")),
                incident.incidentId(), Timestamp.from(incident.bucketTime()), Timestamp.from(end));

        StringBuilder body = new StringBuilder()
                .append("Rule: ").append(field(incident.ruleName(), 160)).append('\n')
                .append("Condition: count ").append(incident.comparator()).append(' ').append(incident.threshold())
                .append("; measured ").append(incident.measuredValue()).append('\n')
                .append("Area: ").append(field(incident.geoAreaId(), 160)).append('\n')
                .append("Window start: ").append(time.format(incident.bucketTime())).append('\n')
                .append("Window end (exclusive): ").append(time.format(end)).append('\n')
                .append("Alert detected at: ").append(time.format(incident.createdAt())).append('\n')
                .append("Incident ID: ").append(incident.incidentId()).append('\n');
        if (visits.isEmpty()) {
            body.append("\nVisit details: No individual matching record is available. The window is not an exact visit time.\n");
        } else {
            body.append("\nLatest matching visits (up to 3; the threshold uses the full window):\n");
            for (int i = 0; i < visits.size(); i++) {
                Visit visit = visits.get(i);
                body.append("\nVisit ").append(i + 1).append('\n')
                        .append("Occurred at (event time): ").append(time.format(visit.occurred())).append('\n')
                        .append("Received at (server time): ").append(time.format(visit.received())).append('\n')
                        .append("Approximate location: ").append(field(visit.city(), 100)).append(", ")
                        .append(field(visit.region(), 80)).append(", ").append(field(visit.country(), 40)).append('\n')
                        .append("Page: ").append(field(visit.path(), 300)).append('\n')
                        .append("Event: ").append(field(visit.event(), 80)).append('\n')
                        .append("Client: ").append(field(visit.device(), 60)).append(" / ")
                        .append(field(visit.browser(), 60)).append('\n')
                        .append("Event ID: ").append(field(visit.id(), 200)).append('\n');
            }
        }
        body.append("\nLocation is approximate city/region-level network geolocation, not a street address.\n")
                .append("Visit times remain unchanged during delayed delivery or replay.\n")
                .append("Administrator sign-in is required to view visitor records.");
        // A concurrent retry must send the first persisted snapshot, not a newer interpretation.
        jdbc.update("update incidents set notification_summary = ? where incident_id = ? and notification_summary is null",
                body.toString(), incident.incidentId());
        return stored(incident.incidentId());
    }

    private String stored(long incidentId) {
        return jdbc.queryForObject("select notification_summary from incidents where incident_id = ?",
                String.class, incidentId);
    }

    private static String field(String value, int max) {
        if (value == null || value.isBlank()) return "Unknown";
        String clean = value.replaceAll("[\\p{Cntrl}\\p{Cf}]", " ").trim();
        return clean.length() <= max ? clean : clean.substring(0, max) + "...";
    }

    private record Visit(String id, String event, Instant occurred, Instant received, String path,
                         String city, String region, String country, String device, String browser) {}
}
