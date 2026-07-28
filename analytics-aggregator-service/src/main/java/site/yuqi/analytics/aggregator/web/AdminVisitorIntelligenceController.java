package site.yuqi.analytics.aggregator.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import site.yuqi.analytics.aggregator.service.JourneyIntentProjectionService;
import site.yuqi.analytics.aggregator.service.JourneyIntentProjectionService.ReplayResult;
import site.yuqi.analytics.aggregator.service.ContentIntentMetadataService;
import site.yuqi.analytics.aggregator.service.ContentIntentMetadataService.ContentIntentMetadata;
import site.yuqi.analytics.aggregator.service.ContentIntentMetadataService.ContentIntentMetadataInput;
import site.yuqi.analytics.aggregator.service.VisitorIntelligenceService;
import site.yuqi.analytics.aggregator.service.VisitorIntelligenceService.VisitorIntelligenceOverview;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;

/** Admin-only visitor journey, intent scoring and replay endpoints. */
@RestController
@RequestMapping("/api/admin/visitor-intelligence")
public class AdminVisitorIntelligenceController {

    private final VisitorIntelligenceService intelligence;
    private final JourneyIntentProjectionService projection;
    private final ContentIntentMetadataService contentMetadata;
    private final String siteId;
    private final int maxRangeDays;
    private final int maxReplaySessions;
    private final String excludedPathPrefix;

    public AdminVisitorIntelligenceController(
            VisitorIntelligenceService intelligence,
            JourneyIntentProjectionService projection,
            ContentIntentMetadataService contentMetadata,
            @Value("${analytics.backfill.site-id:yuqi.site}") String siteId,
            @Value("${analytics.admin.query.max-range-days:31}") int maxRangeDays,
            @Value("${analytics.intent.replay.max-sessions:2000}") int maxReplaySessions,
            @Value("${analytics.admin.query.excluded-path-prefix:/admin}") String excludedPathPrefix) {
        this.intelligence = intelligence;
        this.projection = projection;
        this.contentMetadata = contentMetadata;
        this.siteId = siteId;
        this.maxRangeDays = Math.max(1, maxRangeDays);
        this.maxReplaySessions = Math.max(1, maxReplaySessions);
        this.excludedPathPrefix = normalizePathPrefix(excludedPathPrefix);
    }

    @GetMapping
    public VisitorIntelligenceOverview overview(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) Integer hours,
            @RequestParam(defaultValue = "false") boolean includeAdmin) {
        TimeRange range = timeRange(from, to, hours);
        return intelligence.overview(
                siteId, range.from(), range.to(), includeAdmin, excludedPathPrefix);
    }

    @PostMapping("/replay")
    public ReplayResult replay(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) Integer hours,
            @RequestParam(defaultValue = "1000") int maxSessions) {
        if (maxSessions < 1 || maxSessions > maxReplaySessions) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "maxSessions must be between 1 and " + maxReplaySessions);
        }
        TimeRange range = timeRange(from, to, hours);
        return projection.replay(siteId, range.from(), range.to(), maxSessions);
    }

    @GetMapping("/content-metadata")
    public List<ContentIntentMetadata> contentMetadata(
            @RequestParam(defaultValue = "100") int limit) {
        if (limit < 1 || limit > 500) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be between 1 and 500");
        }
        return contentMetadata.list(siteId, limit);
    }

    @PutMapping("/content-metadata")
    public ContentIntentMetadata upsertContentMetadata(
            @RequestBody ContentIntentMetadataInput input) {
        validate(input);
        return contentMetadata.upsert(siteId, new ContentIntentMetadataInput(
                input.contentType().trim(),
                input.contentId().trim(),
                nullable(input.canonicalPath()),
                nullable(input.displayTitle()),
                nullable(input.coverUrl()),
                input.primaryIntent().trim(),
                input.technicalDomains() == null
                        ? List.of()
                        : input.technicalDomains().stream()
                                .map(String::trim)
                                .filter(value -> !value.isEmpty())
                                .distinct()
                                .limit(20)
                                .toList(),
                nullable(input.complexityLevel()),
                input.careerRelevance(),
                nullable(input.recommendationGroup()),
                input.intentWeight(),
                input.metadataVersion(),
                input.active()));
    }

    private TimeRange timeRange(String from, String to, Integer hours) {
        Instant end = parseInstant(to, Instant.now(), "to");
        int requestedHours = hours == null ? 24 : hours;
        if (requestedHours < 1 || requestedHours > maxRangeDays * 24) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "hours exceeds the configured query range");
        }
        Instant start = parseInstant(from, end.minus(Duration.ofHours(requestedHours)), "from");
        if (!start.isBefore(end)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "from must be before to");
        }
        if (Duration.between(start, end).compareTo(Duration.ofDays(maxRangeDays)) > 0) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "query range exceeds " + maxRangeDays + " days");
        }
        return new TimeRange(start, end);
    }

    private static Instant parseInstant(String value, Instant fallback, String name) {
        if (value == null || value.isBlank()) return fallback;
        try {
            return Instant.parse(value.trim());
        } catch (DateTimeParseException exception) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, name + " must be an ISO-8601 instant");
        }
    }

    private static String normalizePathPrefix(String value) {
        if (value == null || value.isBlank()) return "/admin";
        String normalized = value.trim().toLowerCase();
        if (!normalized.startsWith("/")) normalized = "/" + normalized;
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static void validate(ContentIntentMetadataInput input) {
        if (input == null
                || blank(input.contentType())
                || blank(input.contentId())
                || blank(input.primaryIntent())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "contentType, contentId and primaryIntent are required");
        }
        if (input.careerRelevance() < 0 || input.careerRelevance() > 100
                || input.intentWeight() < 0 || input.intentWeight() > 100) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "careerRelevance and intentWeight must be between 0 and 100");
        }
        if (input.metadataVersion() < 1) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "metadataVersion must be at least 1");
        }
        if (!blank(input.complexityLevel())
                && !Set.of("FOUNDATIONAL", "INTERMEDIATE", "ADVANCED")
                        .contains(input.complexityLevel().trim())) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "complexityLevel is not supported");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static String nullable(String value) {
        return blank(value) ? null : value.trim();
    }

    private record TimeRange(Instant from, Instant to) {}
}
