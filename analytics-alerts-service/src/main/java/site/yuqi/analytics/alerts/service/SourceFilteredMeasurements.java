package site.yuqi.analytics.alerts.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import site.yuqi.analytics.alerts.dto.AlertRule;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Source dimensions are absent from geo rollups; use bounded, exact event facts. */
final class SourceFilteredMeasurements {
    private SourceFilteredMeasurements() {}

    private static final String MATCH = """
             e.site_id = r.site_id and e.event_name = r.event_type
             and lower(e.referrer_domain) = r.referrer_domain
             and (r.bot_filter = 'ALL' or (r.bot_filter = 'EXCLUDE' and e.is_bot is false)
                  or (r.bot_filter = 'ONLY' and e.is_bot is true))
             and case r.geo_level
                 when 'GLOBAL' then ''
                 when 'COUNTRY' then 'COUNTRY:' || nullif(e.country, '')
                 when 'REGION' then 'REGION:' || nullif(e.country, '') || ':' || nullif(e.region, '')
                 when 'METRO' then case when e.geo_area_id like 'METRO:%' then e.geo_area_id end
                 end is not null
             and (r.geo_area_id = '' or r.geo_area_id = case r.geo_level
                 when 'GLOBAL' then ''
                 when 'COUNTRY' then 'COUNTRY:' || nullif(e.country, '')
                 when 'REGION' then 'REGION:' || nullif(e.country, '') || ':' || nullif(e.region, '')
                 when 'METRO' then case when e.geo_area_id like 'METRO:%' then e.geo_area_id end end)
            """;

    private static String requested(List<AlertRule> rules, List<Object> args) {
        for (AlertRule r : rules) {
            args.add(r.ruleId()); args.add(r.siteId()); args.add(r.eventType()); args.add(r.geoLevel());
            args.add(r.geoAreaId() == null ? "" : r.geoAreaId()); args.add(r.filters().bot().name());
            args.add(r.filters().referrerDomain());
        }
        return "with requested(rule_id,site_id,event_type,geo_level,geo_area_id,bot_filter,referrer_domain) as (values "
                + rules.stream().map(r -> "(cast(? as bigint),cast(? as text),cast(? as text),cast(? as text),cast(? as text),cast(? as text),cast(? as text))")
                .collect(Collectors.joining(",")) + ") ";
    }

    static Map<Long, Long> count(JdbcTemplate jdbc, List<AlertRule> rules, Instant bucket) {
        if (rules.isEmpty()) return Map.of();
        List<Object> args = new ArrayList<>();
        String sql = requested(rules, args) + "select r.rule_id,count(e.event_id) as measured from requested r left join public.behavior_events e on "
                + MATCH + " and e.event_time >= ? and e.event_time < ? group by r.rule_id";
        args.add(Timestamp.from(bucket));
        args.add(Timestamp.from(bucket.plusSeconds("1d".equals(rules.getFirst().granularity()) ? 86400 : 300)));
        Map<Long, Long> result = new HashMap<>();
        jdbc.query(sql, (RowCallbackHandler) rs -> result.put(rs.getLong("rule_id"), rs.getLong("measured")), args.toArray());
        return result;
    }

    record Bucket(Instant time, long measured) {}

    static List<Bucket> replay(JdbcTemplate jdbc, AlertRule rule, Instant from, Instant to) {
        List<Object> args = new ArrayList<>();
        int seconds = "1d".equals(rule.granularity()) ? 86400 : 300;
        // Compare bucket starts like rollup replay, counting the whole matching bucket.
        String bucket = "to_timestamp(floor(extract(epoch from e.event_time)/" + seconds + ")*" + seconds + ")";
        String sql = requested(List.of(rule), args) + "select " + bucket + " as bucket_time,count(*) as measured from requested r join public.behavior_events e on "
                + MATCH + " where e.event_time >= ? and e.event_time < ? and " + bucket + " >= ? and " + bucket
                + " < ? group by bucket_time having count(*) >= ? order by bucket_time desc limit 1";
        long first = Math.floorDiv(from.getEpochSecond(), seconds) * seconds;
        long end = (Math.floorDiv(to.getEpochSecond(), seconds) + 1) * seconds;
        args.add(Timestamp.from(Instant.ofEpochSecond(first)));
        args.add(Timestamp.from(Instant.ofEpochSecond(end)));
        args.add(Timestamp.from(from)); args.add(Timestamp.from(to)); args.add(rule.threshold());
        return jdbc.query(sql, (rs, row) -> new Bucket(rs.getTimestamp("bucket_time").toInstant(), rs.getLong("measured")), args.toArray());
    }
}
