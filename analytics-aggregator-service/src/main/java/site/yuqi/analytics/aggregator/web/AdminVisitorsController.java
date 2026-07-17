package site.yuqi.analytics.aggregator.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import site.yuqi.analytics.aggregator.service.VisitorQueryService;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorQuery;
import site.yuqi.analytics.aggregator.service.VisitorQueryService.VisitorQueryResult;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/** Admin-only, bounded visitor-log query API. */
@RestController
@RequestMapping("/api/admin")
public class AdminVisitorsController {

    private final VisitorQueryService queryService;
    private final String siteId;
    private final int maxRangeDays;
    private final int maxPageSize;

    public AdminVisitorsController(
            VisitorQueryService queryService,
            @Value("${analytics.backfill.site-id:yuqi.site}") String siteId,
            @Value("${analytics.admin.query.max-range-days:31}") int maxRangeDays,
            @Value("${analytics.admin.query.max-page-size:100}") int maxPageSize) {
        this.queryService = queryService;
        this.siteId = siteId;
        this.maxRangeDays = Math.max(1, maxRangeDays);
        this.maxPageSize = Math.max(1, maxPageSize);
    }

    @GetMapping("/visitors")
    public VisitorQueryResult visitors(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) Integer hours,
            @RequestParam(value = "q", required = false) String query,
            @RequestParam(required = false) String event,
            @RequestParam(required = false) String path,
            @RequestParam(required = false) String country,
            @RequestParam(required = false) String city,
            @RequestParam(required = false) String device,
            @RequestParam(required = false) String browser,
            @RequestParam(required = false) String referrer,
            @RequestParam(required = false) String sessionId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        if (page < 0 || page > 10_000) badRequest("page must be between 0 and 10000");
        if (size < 1 || size > maxPageSize) badRequest("size must be between 1 and " + maxPageSize);

        Instant end = parseInstant(to, Instant.now(), "to");
        int requestedHours = hours == null ? 24 : hours;
        if (requestedHours < 1 || requestedHours > maxRangeDays * 24) {
            badRequest("hours exceeds the configured query range");
        }
        Instant start = parseInstant(from, end.minus(Duration.ofHours(requestedHours)), "from");
        if (!start.isBefore(end)) badRequest("from must be before to");
        if (Duration.between(start, end).compareTo(Duration.ofDays(maxRangeDays)) > 0) {
            badRequest("query range exceeds " + maxRangeDays + " days");
        }

        return queryService.query(new VisitorQuery(
                siteId,
                start,
                end,
                bounded(query, 200, "q"),
                bounded(event, 100, "event"),
                bounded(path, 512, "path"),
                bounded(country, 100, "country"),
                bounded(city, 100, "city"),
                bounded(device, 100, "device"),
                bounded(browser, 100, "browser"),
                bounded(referrer, 512, "referrer"),
                bounded(sessionId, 256, "sessionId"),
                page,
                size));
    }

    private static Instant parseInstant(String value, Instant fallback, String name) {
        if (value == null || value.isBlank()) return fallback;
        try {
            return Instant.parse(value.trim());
        } catch (DateTimeParseException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, name + " must be an ISO-8601 instant");
        }
    }

    private static String bounded(String value, int maxLength, String name) {
        if (value == null) return null;
        String trimmed = value.trim();
        if (trimmed.length() > maxLength) badRequest(name + " is too long");
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static void badRequest(String message) {
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
