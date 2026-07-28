package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.net.URI;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds replay-safe visitor journeys and deterministic intent scores.
 *
 * <p>All product semantics, weights and thresholds come from versioned
 * database records. This service is intentionally model-free so the same
 * canonical event history always produces the same explainable score.
 */
@Service
@RequiredArgsConstructor
public class JourneyIntentProjectionService {

    private static final String INSERT_STEP = """
            insert into public.visitor_journey_steps
              (event_id, site_id, session_id, event_name, event_time,
               page_path, target_path, referrer_domain, content_id, content_type,
               engaged_seconds, progress_percent)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (event_id) do nothing
            """;

    private static final String SELECT_ACTIVE_POLICY = """
            select policy_id, version, medium_threshold, high_threshold, max_score
              from public.visitor_intent_policies
             where site_id = ? and status = 'ACTIVE'
             limit 1
            """;

    private static final String SELECT_RULES = """
            select rule_key, description, event_name, path_source, path_match_type,
                   path_pattern, content_type, intent_dimension, base_weight,
                   max_occurrences, min_engaged_seconds, min_progress_percent
              from public.visitor_intent_signal_rules
             where policy_id = ? and active = true
             order by rule_key
            """;

    private static final String SELECT_STEPS = """
            select event_id, event_name, event_time, page_path, target_path,
                   content_id, content_type, engaged_seconds, progress_percent
              from public.visitor_journey_steps
             where site_id = ? and session_id = ?
             order by event_time, event_id
            """;

    private static final String SELECT_CONTENT_METADATA = """
            select content_type, content_id, canonical_path, primary_intent,
                   intent_weight, display_title, cover_url
              from public.content_intent_metadata
             where site_id = ? and active = true
            """;

    private static final String UPSERT_SNAPSHOT = """
            insert into public.visitor_intent_snapshots
              (site_id, session_id, policy_id, policy_version, score, intent_level,
               dominant_intent, dimension_scores, contributing_signals,
               first_event, last_event, calculated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, now())
            on conflict (site_id, session_id, policy_id) do update set
              policy_version = excluded.policy_version,
              score = excluded.score,
              intent_level = excluded.intent_level,
              dominant_intent = excluded.dominant_intent,
              dimension_scores = excluded.dimension_scores,
              contributing_signals = excluded.contributing_signals,
              first_event = excluded.first_event,
              last_event = excluded.last_event,
              calculated_at = now()
            """;

    private static final String REPLAY_STEPS = """
            insert into public.visitor_journey_steps
              (event_id, site_id, session_id, event_name, event_time,
               page_path, target_path, referrer_domain, content_id, content_type,
               engaged_seconds, progress_percent)
            select event_id, site_id, session_id, event_name, event_time,
                   page_path, target_path, referrer_domain,
                   nullif(properties ->> 'contentId', ''),
                   nullif(properties ->> 'contentType', ''),
                   case
                     when properties ->> 'engagedSeconds' ~ '^[0-9]+([.][0-9]+)?$'
                     then least(86400, greatest(0, floor((properties ->> 'engagedSeconds')::numeric)::int))
                   end,
                   case
                     when properties ->> 'progressPercent' ~ '^[0-9]+([.][0-9]+)?$'
                     then least(100, greatest(0, floor((properties ->> 'progressPercent')::numeric)::int))
                   end
              from public.behavior_events
             where site_id = ?
               and session_id is not null
               and event_time >= ?
               and event_time < ?
            on conflict (event_id) do nothing
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    @Transactional
    public ProjectionResult processBatch(List<EnrichedEvent> events) {
        if (events == null || events.isEmpty()) return new ProjectionResult(0, 0);

        int inserted = 0;
        Map<String, Set<String>> sessionsBySite = new LinkedHashMap<>();
        for (EnrichedEvent event : events) {
            String sessionId = resolveSessionKey(event);
            if (!valid(event) || sessionId == null) continue;
            inserted += jdbc.update(INSERT_STEP,
                    event.eventId(), event.siteId(), sessionId, event.eventType(),
                    Timestamp.from(event.eventTime()), pathOnly(event.pageUrl()),
                    pathOnly(event.targetUrl()), domainOnly(event.referrer()),
                    propertyText(event.properties(), "contentId"),
                    propertyText(event.properties(), "contentType"),
                    boundedInteger(event.properties(), "engagedSeconds", 86_400),
                    boundedInteger(event.properties(), "progressPercent", 100));
            sessionsBySite.computeIfAbsent(event.siteId(), ignored -> new HashSet<>()).add(sessionId);
        }

        int rebuilt = 0;
        for (Map.Entry<String, Set<String>> entry : sessionsBySite.entrySet()) {
            ScoringContext context = loadContext(entry.getKey());
            if (context == null) continue;
            for (String sessionId : entry.getValue()) {
                if (rebuildScore(entry.getKey(), sessionId, context)) rebuilt++;
            }
        }
        return new ProjectionResult(inserted, rebuilt);
    }

    /**
     * Rebuilds projections from canonical facts, not raw private events.
     * The bounded caller controls the time range and number of sessions.
     */
    @Transactional
    public ReplayResult replay(String siteId, Instant from, Instant to, int maxSessions) {
        int inserted = jdbc.update(REPLAY_STEPS, siteId, Timestamp.from(from), Timestamp.from(to));
        List<String> sessions = jdbc.query("""
                        select distinct session_id
                          from public.behavior_events
                         where site_id = ?
                           and session_id is not null
                           and event_time >= ?
                           and event_time < ?
                         order by session_id
                         limit ?
                        """,
                (rs, rowNum) -> rs.getString(1),
                siteId, Timestamp.from(from), Timestamp.from(to), maxSessions);

        ScoringContext context = loadContext(siteId);
        int rebuilt = 0;
        if (context != null) {
            for (String sessionId : sessions) {
                if (rebuildScore(siteId, sessionId, context)) rebuilt++;
            }
        }
        return new ReplayResult(inserted, rebuilt, sessions.size());
    }

    private ScoringContext loadContext(String siteId) {
        List<Policy> policies = jdbc.query(SELECT_ACTIVE_POLICY,
                (rs, rowNum) -> new Policy(
                        rs.getString("policy_id"),
                        rs.getInt("version"),
                        rs.getInt("medium_threshold"),
                        rs.getInt("high_threshold"),
                        rs.getInt("max_score")),
                siteId);
        if (policies.isEmpty()) return null;

        Policy policy = policies.get(0);
        List<SignalRule> rules = jdbc.query(SELECT_RULES,
                (rs, rowNum) -> new SignalRule(
                        rs.getString("rule_key"),
                        rs.getString("description"),
                        rs.getString("event_name"),
                        rs.getString("path_source"),
                        rs.getString("path_match_type"),
                        rs.getString("path_pattern"),
                        rs.getString("content_type"),
                        rs.getString("intent_dimension"),
                        rs.getInt("base_weight"),
                        rs.getInt("max_occurrences"),
                        nullableInteger(rs.getObject("min_engaged_seconds")),
                        nullableInteger(rs.getObject("min_progress_percent"))),
                policy.policyId());

        List<ContentMetadata> metadata = jdbc.query(SELECT_CONTENT_METADATA,
                (rs, rowNum) -> new ContentMetadata(
                        rs.getString("content_type"),
                        rs.getString("content_id"),
                        rs.getString("canonical_path"),
                        rs.getString("primary_intent"),
                        rs.getInt("intent_weight"),
                        rs.getString("display_title"),
                        rs.getString("cover_url")),
                siteId);
        return new ScoringContext(policy, rules, ContentCatalog.of(metadata));
    }

    private boolean rebuildScore(String siteId, String sessionId, ScoringContext context) {
        List<JourneyStep> steps = jdbc.query(SELECT_STEPS,
                (rs, rowNum) -> new JourneyStep(
                        rs.getString("event_id"),
                        rs.getString("event_name"),
                        rs.getTimestamp("event_time").toInstant(),
                        rs.getString("page_path"),
                        rs.getString("target_path"),
                        rs.getString("content_id"),
                        rs.getString("content_type"),
                        nullableInteger(rs.getObject("engaged_seconds")),
                        nullableInteger(rs.getObject("progress_percent"))),
                siteId, sessionId);
        if (steps.isEmpty()) return false;

        Map<String, Integer> dimensionScores = new LinkedHashMap<>();
        List<Map<String, Object>> signals = new ArrayList<>();
        for (SignalRule rule : context.rules()) {
            int occurrences = 0;
            for (JourneyStep step : steps) {
                if (rule.matches(step)) occurrences++;
            }
            occurrences = Math.min(occurrences, rule.maxOccurrences());
            if (occurrences == 0) continue;
            int score = rule.baseWeight() * occurrences;
            dimensionScores.merge(rule.intentDimension(), score, Integer::sum);
            signals.add(signal(rule.ruleKey(), rule.description(), rule.intentDimension(), occurrences, score));
        }

        Set<String> scoredContent = new HashSet<>();
        for (JourneyStep step : steps) {
            ContentMetadata metadata = context.contentCatalog().find(step);
            if (metadata == null || metadata.intentWeight() <= 0) continue;
            String identity = metadata.contentType() + ":" + metadata.contentId();
            if (!scoredContent.add(identity)) continue;
            dimensionScores.merge(metadata.primaryIntent(), metadata.intentWeight(), Integer::sum);
            signals.add(Map.of(
                    "signalKey", "content:" + identity,
                    "description", metadata.displayTitle() == null
                            ? "Matched stored content intent metadata" : metadata.displayTitle(),
                    "dimension", metadata.primaryIntent(),
                    "occurrences", 1,
                    "score", metadata.intentWeight()));
        }

        Policy policy = context.policy();
        int rawScore = dimensionScores.values().stream().mapToInt(Integer::intValue).sum();
        int score = Math.min(policy.maxScore(), rawScore);
        String level = intentLevel(score, policy.mediumThreshold(), policy.highThreshold());
        String dominant = dimensionScores.entrySet().stream()
                .max(Map.Entry.<String, Integer>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey(Comparator.reverseOrder())))
                .map(Map.Entry::getKey)
                .orElse(null);

        jdbc.update(UPSERT_SNAPSHOT,
                siteId, sessionId, policy.policyId(), policy.version(), score, level, dominant,
                json(dimensionScores), json(signals),
                Timestamp.from(steps.get(0).eventTime()),
                Timestamp.from(steps.get(steps.size() - 1).eventTime()));
        return true;
    }

    private static Map<String, Object> signal(
            String key, String description, String dimension, int occurrences, int score) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("signalKey", key);
        value.put("description", description);
        value.put("dimension", dimension);
        value.put("occurrences", occurrences);
        value.put("score", score);
        return value;
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Intent projection JSON serialization failed", exception);
        }
    }

    private static boolean valid(EnrichedEvent event) {
        return event != null
                && notBlank(event.eventId())
                && notBlank(event.siteId())
                && notBlank(event.eventType())
                && event.eventTime() != null;
    }

    private static String resolveSessionKey(EnrichedEvent event) {
        if (notBlank(event.sessionId())) return event.sessionId();
        if (notBlank(event.anonId())) return event.anonId();
        if (!notBlank(event.ipHash())) return null;
        return event.ipHash() + ":" + (notBlank(event.deviceType()) ? event.deviceType() : "unknown");
    }

    private static String pathOnly(String value) {
        if (!notBlank(value)) return null;
        try {
            URI uri = value.startsWith("/") ? URI.create("https://local" + value) : URI.create(value);
            return truncate(uri.getPath(), 512);
        } catch (IllegalArgumentException ignored) {
            return truncate(value.split("[?#]", 2)[0], 512);
        }
    }

    private static String domainOnly(String value) {
        if (!notBlank(value)) return null;
        try {
            URI uri = value.contains("://") ? URI.create(value) : URI.create("https://" + value);
            return truncate(uri.getHost(), 255);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static String propertyText(Map<String, Object> properties, String key) {
        if (properties == null) return null;
        Object value = properties.get(key);
        return value == null ? null : truncate(String.valueOf(value).trim(), 256);
    }

    private static Integer boundedInteger(Map<String, Object> properties, String key, int max) {
        if (properties == null) return null;
        Object value = properties.get(key);
        if (value == null) return null;
        try {
            int parsed = value instanceof Number number
                    ? number.intValue() : (int) Math.floor(Double.parseDouble(String.valueOf(value)));
            return Math.min(max, Math.max(0, parsed));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Integer nullableInteger(Object value) {
        return value instanceof Number number ? number.intValue() : null;
    }

    private static String truncate(String value, int max) {
        return value == null || value.length() <= max ? value : value.substring(0, max);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    static String intentLevel(int score, int mediumThreshold, int highThreshold) {
        return score >= highThreshold ? "HIGH" : score >= mediumThreshold ? "MEDIUM" : "LOW";
    }

    static boolean ruleMatches(
            String ruleEventName,
            String pathSource,
            String pathMatchType,
            String pathPattern,
            String requiredContentType,
            Integer minEngagedSeconds,
            Integer minProgressPercent,
            String stepEventName,
            String pagePath,
            String targetPath,
            String contentType,
            Integer engagedSeconds,
            Integer progressPercent) {
        if (notBlank(ruleEventName) && !ruleEventName.equals(stepEventName)) return false;
        if (notBlank(requiredContentType) && !requiredContentType.equalsIgnoreCase(contentType)) return false;
        if (minEngagedSeconds != null
                && (engagedSeconds == null || engagedSeconds < minEngagedSeconds)) return false;
        if (minProgressPercent != null
                && (progressPercent == null || progressPercent < minProgressPercent)) return false;
        if ("ANY".equals(pathMatchType)) return true;

        List<String> paths = new ArrayList<>(2);
        if (("ANY".equals(pathSource) || "PAGE".equals(pathSource)) && pagePath != null) {
            paths.add(pagePath);
        }
        if (("ANY".equals(pathSource) || "TARGET".equals(pathSource)) && targetPath != null) {
            paths.add(targetPath);
        }
        return paths.stream().anyMatch(path -> switch (pathMatchType) {
            case "EXACT" -> path.equals(pathPattern);
            case "PREFIX" -> path.startsWith(pathPattern);
            default -> false;
        });
    }

    public record ProjectionResult(int insertedSteps, int rebuiltSessions) {}

    public record ReplayResult(int insertedSteps, int rebuiltSessions, int matchedSessions) {}

    private record Policy(
            String policyId,
            int version,
            int mediumThreshold,
            int highThreshold,
            int maxScore) {}

    private record SignalRule(
            String ruleKey,
            String description,
            String eventName,
            String pathSource,
            String pathMatchType,
            String pathPattern,
            String contentType,
            String intentDimension,
            int baseWeight,
            int maxOccurrences,
            Integer minEngagedSeconds,
            Integer minProgressPercent) {

        boolean matches(JourneyStep step) {
            return ruleMatches(
                    eventName, pathSource, pathMatchType, pathPattern, contentType,
                    minEngagedSeconds, minProgressPercent,
                    step.eventName(), step.pagePath(), step.targetPath(), step.contentType(),
                    step.engagedSeconds(), step.progressPercent());
        }
    }

    private record JourneyStep(
            String eventId,
            String eventName,
            Instant eventTime,
            String pagePath,
            String targetPath,
            String contentId,
            String contentType,
            Integer engagedSeconds,
            Integer progressPercent) {}

    private record ContentMetadata(
            String contentType,
            String contentId,
            String canonicalPath,
            String primaryIntent,
            int intentWeight,
            String displayTitle,
            String coverUrl) {}

    private record ContentCatalog(
            Map<String, ContentMetadata> byIdentity,
            Map<String, ContentMetadata> byPath) {

        static ContentCatalog of(List<ContentMetadata> metadata) {
            Map<String, ContentMetadata> byIdentity = new HashMap<>();
            Map<String, ContentMetadata> byPath = new HashMap<>();
            for (ContentMetadata item : metadata) {
                byIdentity.put(identity(item.contentType(), item.contentId()), item);
                if (notBlank(item.canonicalPath())) byPath.put(item.canonicalPath(), item);
            }
            return new ContentCatalog(byIdentity, byPath);
        }

        ContentMetadata find(JourneyStep step) {
            if (notBlank(step.contentType()) && notBlank(step.contentId())) {
                ContentMetadata direct = byIdentity.get(identity(step.contentType(), step.contentId()));
                if (direct != null) return direct;
            }
            ContentMetadata page = byPath.get(step.pagePath());
            return page != null ? page : byPath.get(step.targetPath());
        }

        private static String identity(String type, String id) {
            return String.valueOf(type).toLowerCase() + ":" + String.valueOf(id);
        }
    }

    private record ScoringContext(
            Policy policy,
            List<SignalRule> rules,
            ContentCatalog contentCatalog) {}
}
