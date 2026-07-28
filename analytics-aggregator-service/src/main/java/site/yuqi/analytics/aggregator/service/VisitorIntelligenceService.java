package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Read model for deterministic visitor journeys and intent scores. */
@Service
@RequiredArgsConstructor
public class VisitorIntelligenceService {

    private static final String PUBLIC_PATH_CONDITION =
            "(? or coalesce(j.page_path, '') not like ?)";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public VisitorIntelligenceOverview overview(
            String siteId, Instant from, Instant to, boolean includeAdmin, String excludedPathPrefix) {
        String excludedLike = excludedPathPrefix + "%";
        WindowSummary summary = loadSummary(siteId, from, to, includeAdmin, excludedLike);
        PolicySummary policy = loadPolicy(siteId);
        List<FunnelStep> funnel = loadFunnel(siteId, from, to, includeAdmin, excludedLike);
        List<AttributionSource> attribution =
                loadAttribution(siteId, from, to, includeAdmin, excludedLike);
        List<TopContent> topContent =
                loadTopContent(siteId, from, to, includeAdmin, excludedLike);
        CohortSummary cohort = loadCohort(siteId, from, to);
        List<JourneySummary> highIntent =
                loadJourneys(siteId, from, to, includeAdmin, excludedLike, true, 12);
        List<JourneySummary> recentJourneys =
                loadJourneys(siteId, from, to, includeAdmin, excludedLike, false, 12);
        return new VisitorIntelligenceOverview(
                from, to, summary.totalEvents(), summary.uniqueVisitors(),
                policy, funnel, attribution, topContent, cohort, highIntent, recentJourneys);
    }

    private WindowSummary loadSummary(
            String siteId, Instant from, Instant to, boolean includeAdmin, String excludedLike) {
        return jdbc.queryForObject("""
                        select count(*) as total_events,
                               count(distinct session_id) as unique_visitors
                          from public.visitor_journey_steps j
                         where j.site_id = ?
                           and j.event_time >= ?
                           and j.event_time < ?
                           and """ + PUBLIC_PATH_CONDITION,
                (rs, rowNum) -> new WindowSummary(
                        rs.getLong("total_events"),
                        rs.getLong("unique_visitors")),
                siteId, Timestamp.from(from), Timestamp.from(to), includeAdmin, excludedLike);
    }

    private PolicySummary loadPolicy(String siteId) {
        List<PolicySummary> values = jdbc.query("""
                        select policy_id, version, name, medium_threshold,
                               high_threshold, max_score, activated_at
                          from public.visitor_intent_policies
                         where site_id = ? and status = 'ACTIVE'
                         limit 1
                        """,
                (rs, rowNum) -> new PolicySummary(
                        rs.getString("policy_id"),
                        rs.getInt("version"),
                        rs.getString("name"),
                        rs.getInt("medium_threshold"),
                        rs.getInt("high_threshold"),
                        rs.getInt("max_score"),
                        instant(rs.getTimestamp("activated_at"))),
                siteId);
        return values.isEmpty() ? null : values.get(0);
    }

    private List<FunnelStep> loadFunnel(
            String siteId, Instant from, Instant to, boolean includeAdmin, String excludedLike) {
        List<FunnelStep> values = jdbc.query("""
                        select r.step_key,
                               max(r.step_label) as step_label,
                               max(r.step_description) as step_description,
                               min(r.step_order) as step_order,
                               count(distinct j.session_id) as visitors
                          from public.visitor_journey_funnel_rules r
                          left join public.visitor_journey_steps j
                            on j.site_id = r.site_id
                           and j.event_time >= ?
                           and j.event_time < ?
                           and (? or coalesce(j.page_path, '') not like ?)
                           and (r.event_name is null or r.event_name = j.event_name)
                           and (
                             r.path_match_type = 'ANY'
                             or (
                               r.path_source in ('ANY', 'PAGE')
                               and (
                                 (r.path_match_type = 'EXACT' and j.page_path = r.path_pattern)
                                 or (r.path_match_type = 'PREFIX' and j.page_path like r.path_pattern || '%')
                               )
                             )
                             or (
                               r.path_source in ('ANY', 'TARGET')
                               and (
                                 (r.path_match_type = 'EXACT' and j.target_path = r.path_pattern)
                                 or (r.path_match_type = 'PREFIX' and j.target_path like r.path_pattern || '%')
                               )
                             )
                           )
                         where r.site_id = ? and r.active = true
                           and r.funnel_version = (
                             select max(funnel_version)
                               from public.visitor_journey_funnel_rules
                              where site_id = ? and active = true
                           )
                         group by r.step_key
                         order by min(r.step_order)
                        """,
                (rs, rowNum) -> new FunnelStep(
                        rs.getString("step_key"),
                        rs.getString("step_label"),
                        rs.getString("step_description"),
                        rs.getInt("step_order"),
                        rs.getLong("visitors"),
                        0,
                        0),
                Timestamp.from(from), Timestamp.from(to), includeAdmin, excludedLike, siteId, siteId);

        long start = values.isEmpty() ? 0 : Math.max(1, values.get(0).visitors());
        long previous = start;
        List<FunnelStep> rates = new ArrayList<>(values.size());
        for (FunnelStep value : values) {
            double fromStart = value.visitors() / (double) start;
            double fromPrevious = value.visitors() / (double) Math.max(1, previous);
            rates.add(new FunnelStep(
                    value.key(), value.label(), value.description(), value.order(),
                    value.visitors(), fromStart, fromPrevious));
            if (value.visitors() > 0) previous = value.visitors();
        }
        return rates;
    }

    private List<AttributionSource> loadAttribution(
            String siteId, Instant from, Instant to, boolean includeAdmin, String excludedLike) {
        return jdbc.query("""
                        with per_session as (
                          select j.session_id,
                                 (array_agg(j.referrer_domain order by j.event_time)
                                   filter (where j.referrer_domain is not null))[1] as referrer_domain,
                                 count(*) as events,
                                 coalesce(max(i.score), 0) as intent_score
                            from public.visitor_journey_steps j
                            left join public.visitor_intent_snapshots i
                              on i.site_id = j.site_id and i.session_id = j.session_id
                           where j.site_id = ?
                             and j.event_time >= ?
                             and j.event_time < ?
                             and (? or coalesce(j.page_path, '') not like ?)
                           group by j.session_id
                        ),
                        mapped as (
                          select p.*,
                                 coalesce(rule.source_label,
                                   case when p.referrer_domain is null then 'Direct' else p.referrer_domain end) as source,
                                 coalesce(rule.source_type,
                                   case when p.referrer_domain is null then 'direct' else 'referral' end) as source_type
                            from per_session p
                            left join lateral (
                              select r.source_label, r.source_type
                                from public.visitor_attribution_rules r
                               where r.site_id = ?
                                 and r.active = true
                                 and position(lower(r.host_pattern) in lower(p.referrer_domain)) > 0
                               order by r.priority
                               limit 1
                            ) rule on true
                        )
                        select source, source_type, count(*) as visitors,
                               sum(events) as events,
                               round(avg(intent_score))::int as quality_score
                          from mapped
                         group by source, source_type
                         order by quality_score desc, events desc
                         limit 8
                        """,
                (rs, rowNum) -> new AttributionSource(
                        rs.getString("source"),
                        rs.getString("source_type"),
                        rs.getLong("visitors"),
                        rs.getLong("events"),
                        rs.getInt("quality_score")),
                siteId, Timestamp.from(from), Timestamp.from(to),
                includeAdmin, excludedLike, siteId);
    }

    private List<TopContent> loadTopContent(
            String siteId, Instant from, Instant to, boolean includeAdmin, String excludedLike) {
        return jdbc.query("""
                        with content_events as (
                          select j.*,
                                 coalesce(j.target_path, j.page_path) as content_path
                            from public.visitor_journey_steps j
                           where j.site_id = ?
                             and j.event_time >= ?
                             and j.event_time < ?
                             and (? or coalesce(j.page_path, '') not like ?)
                             and coalesce(j.target_path, j.page_path) is not null
                             and coalesce(j.target_path, j.page_path) <> '/'
                        )
                        select c.content_path as path,
                               coalesce(max(m.content_type), max(c.content_type), 'page') as content_type,
                               coalesce(max(m.display_title), c.content_path) as title,
                               max(m.cover_url) as cover_url,
                               count(*) as events,
                               count(distinct c.session_id) as visitors,
                               coalesce(sum(c.engaged_seconds), 0) as engaged_seconds
                          from content_events c
                          left join public.content_intent_metadata m
                            on m.site_id = c.site_id and m.active = true
                           and (
                             (m.content_id = c.content_id and m.content_type = c.content_type)
                             or m.canonical_path = c.content_path
                           )
                         group by c.content_path
                         order by visitors desc, events desc, engaged_seconds desc
                         limit 8
                        """,
                (rs, rowNum) -> new TopContent(
                        rs.getString("path"),
                        rs.getString("content_type"),
                        rs.getString("title"),
                        rs.getString("cover_url"),
                        rs.getLong("events"),
                        rs.getLong("visitors"),
                        rs.getLong("engaged_seconds")),
                siteId, Timestamp.from(from), Timestamp.from(to), includeAdmin, excludedLike);
    }

    private CohortSummary loadCohort(String siteId, Instant from, Instant to) {
        return jdbc.queryForObject("""
                        with identities as (
                          select coalesce(nullif(anon_id, ''), session_id) as visitor_id,
                                 count(*) as sessions,
                                 sum(page_views + clicks) as events
                            from public.sessions
                           where site_id = ? and last_event >= ? and first_event < ?
                           group by coalesce(nullif(anon_id, ''), session_id)
                        ),
                        journey_depth as (
                          select coalesce(nullif(s.anon_id, ''), s.session_id) as visitor_id,
                                 count(distinct j.page_path) filter (where j.page_path is not null) as visited_pages
                            from public.sessions s
                            join public.visitor_journey_steps j
                              on j.site_id = s.site_id and j.session_id = s.session_id
                           where s.site_id = ? and s.last_event >= ? and s.first_event < ?
                           group by coalesce(nullif(s.anon_id, ''), s.session_id)
                        )
                        select count(*) as visitors,
                               count(*) filter (where i.sessions > 1) as returning_visitors,
                               count(*) filter (where coalesce(d.visited_pages, 0) >= 2) as multi_step_visitors,
                               coalesce(avg(i.events), 0) as average_events
                          from identities i
                          left join journey_depth d on d.visitor_id = i.visitor_id
                        """,
                (rs, rowNum) -> {
                    long visitors = rs.getLong("visitors");
                    long returning = rs.getLong("returning_visitors");
                    long multiStep = rs.getLong("multi_step_visitors");
                    return new CohortSummary(
                            visitors,
                            returning,
                            multiStep,
                            rs.getDouble("average_events"),
                            visitors == 0 ? 0 : returning / (double) visitors,
                            visitors == 0 ? 0 : multiStep / (double) visitors);
                },
                siteId, Timestamp.from(from), Timestamp.from(to),
                siteId, Timestamp.from(from), Timestamp.from(to));
    }

    private List<JourneySummary> loadJourneys(
            String siteId,
            Instant from,
            Instant to,
            boolean includeAdmin,
            String excludedLike,
            boolean highIntentOnly,
            int limit) {
        String intentCondition = highIntentOnly
                ? " and i.intent_level in ('HIGH', 'MEDIUM')\n" : "";
        String orderBy = highIntentOnly
                ? "i.score desc, s.last_event desc"
                : "s.last_event desc";
        String query = """
                        select s.session_id, s.first_event, s.last_event, s.duration_ms,
                               s.entry_page, s.exit_page, s.device_type, s.browser,
                               s.country, s.geo_area_id,
                               i.policy_version, i.score, i.intent_level, i.dominant_intent,
                               i.dimension_scores, i.contributing_signals
                          from public.sessions s
                          join public.visitor_intent_snapshots i
                            on i.site_id = s.site_id and i.session_id = s.session_id
                         where s.site_id = ?
                           and s.last_event >= ?
                           and s.first_event < ?
                           and (? or coalesce(s.entry_page, '') not like ?)
                        """ + intentCondition + """
                         order by %s
                         limit ?
                        """.formatted(orderBy);
        List<JourneyRow> rows = jdbc.query(
                query,
                (rs, rowNum) -> new JourneyRow(
                        rs.getString("session_id"),
                        rs.getTimestamp("first_event").toInstant(),
                        rs.getTimestamp("last_event").toInstant(),
                        rs.getLong("duration_ms"),
                        rs.getString("entry_page"),
                        rs.getString("exit_page"),
                        rs.getString("device_type"),
                        rs.getString("browser"),
                        rs.getString("country"),
                        rs.getString("geo_area_id"),
                        rs.getInt("policy_version"),
                        rs.getInt("score"),
                        rs.getString("intent_level"),
                        rs.getString("dominant_intent"),
                        map(rs.getString("dimension_scores")),
                        list(rs.getString("contributing_signals"))),
                siteId, Timestamp.from(from), Timestamp.from(to),
                includeAdmin, excludedLike, limit);

        List<JourneySummary> results = new ArrayList<>(rows.size());
        for (JourneyRow row : rows) {
            List<JourneyStep> steps = new ArrayList<>(jdbc.query("""
                            select event_id, event_name, event_time, page_path, target_path,
                                   content_id, content_type, engaged_seconds, progress_percent
                              from public.visitor_journey_steps
                             where site_id = ? and session_id = ?
                             order by event_time desc, event_id desc
                             limit 8
                            """,
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
                    siteId, row.sessionId()));
            Collections.reverse(steps);
            results.add(row.withSteps(steps));
        }
        return results;
    }

    private Map<String, Integer> map(String json) {
        try {
            return mapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException exception) {
            return Map.of();
        }
    }

    private List<Map<String, Object>> list(String json) {
        try {
            return mapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException exception) {
            return List.of();
        }
    }

    private static Instant instant(Timestamp value) {
        return value == null ? null : value.toInstant();
    }

    private static Integer nullableInteger(Object value) {
        return value instanceof Number number ? number.intValue() : null;
    }

    private record WindowSummary(long totalEvents, long uniqueVisitors) {}

    public record VisitorIntelligenceOverview(
            Instant from,
            Instant to,
            long totalEvents,
            long uniqueVisitors,
            PolicySummary policy,
            List<FunnelStep> funnel,
            List<AttributionSource> attribution,
            List<TopContent> topContent,
            CohortSummary cohort,
            List<JourneySummary> highIntent,
            List<JourneySummary> recentJourneys) {}

    public record PolicySummary(
            String policyId,
            int version,
            String name,
            int mediumThreshold,
            int highThreshold,
            int maxScore,
            Instant activatedAt) {}

    public record FunnelStep(
            String key,
            String label,
            String description,
            int order,
            long visitors,
            double fromStartRate,
            double fromPreviousRate) {}

    public record AttributionSource(
            String source,
            String sourceType,
            long visitors,
            long events,
            int qualityScore) {}

    public record TopContent(
            String path,
            String type,
            String title,
            String coverUrl,
            long events,
            long visitors,
            long engagedSeconds) {}

    public record CohortSummary(
            long visitors,
            long returningVisitors,
            long multiStepVisitors,
            double averageEventsPerVisitor,
            double returningRate,
            double multiStepRate) {}

    public record JourneySummary(
            String sessionId,
            Instant firstEvent,
            Instant lastEvent,
            long durationMs,
            String entryPage,
            String exitPage,
            String deviceType,
            String browser,
            String country,
            String geoAreaId,
            int policyVersion,
            int score,
            String intentLevel,
            String dominantIntent,
            Map<String, Integer> dimensionScores,
            List<Map<String, Object>> contributingSignals,
            List<JourneyStep> steps) {}

    public record JourneyStep(
            String eventId,
            String eventName,
            Instant eventTime,
            String pagePath,
            String targetPath,
            String contentId,
            String contentType,
            Integer engagedSeconds,
            Integer progressPercent) {}

    private record JourneyRow(
            String sessionId,
            Instant firstEvent,
            Instant lastEvent,
            long durationMs,
            String entryPage,
            String exitPage,
            String deviceType,
            String browser,
            String country,
            String geoAreaId,
            int policyVersion,
            int score,
            String intentLevel,
            String dominantIntent,
            Map<String, Integer> dimensionScores,
            List<Map<String, Object>> contributingSignals) {

        JourneySummary withSteps(List<JourneyStep> steps) {
            return new JourneySummary(
                    sessionId, firstEvent, lastEvent, durationMs, entryPage, exitPage,
                    deviceType, browser, country, geoAreaId, policyVersion, score,
                    intentLevel, dominantIntent, dimensionScores, contributingSignals, steps);
        }
    }
}
