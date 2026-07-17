package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Parameterized, bounded queries over the restricted visitor event tier. */
@Service
@RequiredArgsConstructor
public class VisitorQueryService {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public VisitorQueryResult query(VisitorQuery query) {
        QueryParts parts = buildWhere(query);
        long total = jdbc.queryForObject(
                "select count(*) " + parts.fromAndWhere(), parts.params(), Long.class);

        VisitorSummary summary = jdbc.queryForObject("""
                select count(*) as total_events,
                       count(distinct coalesce(nullif(r.session_id, ''), nullif(r.anon_id, ''), nullif(r.ip_address, ''))) as unique_visitors,
                       count(distinct nullif(r.country, '')) as countries,
                       count(distinct nullif(r.city, '')) as cities
                """ + parts.fromAndWhere(), parts.params(), (rs, rowNum) -> new VisitorSummary(
                rs.getLong("total_events"),
                rs.getLong("unique_visitors"),
                rs.getLong("countries"),
                rs.getLong("cities")));

        MapSqlParameterSource itemParams = new MapSqlParameterSource(parts.params().getValues())
                .addValue("limit", query.size())
                .addValue("offset", (long) query.page() * query.size());
        List<VisitorLogItem> items = jdbc.query("""
                select r.event_id, r.event_name, r.event_time, r.server_time,
                       r.session_id, r.anon_id, r.page_url, r.target_url,
                       r.referrer, r.user_agent, r.ip_address, r.country,
                       r.region, r.city, r.latitude, r.longitude, r.properties::text as properties,
                       b.device_type, b.browser, b.os, b.is_bot
                """ + parts.fromAndWhere() + " order by r.event_time desc limit :limit offset :offset",
                itemParams, this::mapItem);

        int totalPages = total == 0 ? 0 : (int) Math.ceil((double) total / query.size());
        return new VisitorQueryResult(
                items,
                summary == null ? new VisitorSummary(0, 0, 0, 0) : summary,
                new PageInfo(query.page(), query.size(), total, totalPages),
                query.from(),
                query.to());
    }

    private QueryParts buildWhere(VisitorQuery query) {
        StringBuilder sql = new StringBuilder("""
                from analytics_private.behavior_events_raw r
                left join public.behavior_events b on b.event_id = r.event_id
                where r.site_id = :siteId and r.event_time >= :from and r.event_time < :to
                """);
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("siteId", query.siteId())
                .addValue("from", Timestamp.from(query.from()))
                .addValue("to", Timestamp.from(query.to()));

        addExact(sql, params, "event", "r.event_name", query.event());
        addContains(sql, params, "path", "coalesce(r.page_url, '') || ' ' || coalesce(r.target_url, '')", query.path());
        addExact(sql, params, "country", "r.country", query.country());
        addExact(sql, params, "city", "r.city", query.city());
        addExact(sql, params, "device", "b.device_type", query.device());
        addExact(sql, params, "browser", "b.browser", query.browser());
        addContains(sql, params, "referrer", "coalesce(r.referrer, '')", query.referrer());
        addExact(sql, params, "sessionId", "r.session_id", query.sessionId());

        if (hasText(query.query())) {
            sql.append("""
                     and lower(concat_ws(' ', r.event_name, r.page_url, r.target_url, r.referrer,
                         r.session_id, r.anon_id, r.ip_address, r.country, r.region, r.city,
                         b.device_type, b.browser, b.os)) like :query escape '\\'
                    """);
            params.addValue("query", containsPattern(query.query()));
        }
        return new QueryParts(sql.toString(), params);
    }

    private static void addExact(StringBuilder sql, MapSqlParameterSource params,
                                 String name, String column, String value) {
        if (!hasText(value)) return;
        sql.append(" and lower(coalesce(").append(column).append(", '')) = :").append(name);
        params.addValue(name, value.trim().toLowerCase());
    }

    private static void addContains(StringBuilder sql, MapSqlParameterSource params,
                                    String name, String expression, String value) {
        if (!hasText(value)) return;
        sql.append(" and lower(").append(expression).append(") like :").append(name).append(" escape '\\'");
        params.addValue(name, containsPattern(value));
    }

    private static String containsPattern(String value) {
        String escaped = value.trim().toLowerCase()
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_");
        return "%" + escaped + "%";
    }

    private VisitorLogItem mapItem(ResultSet rs, int rowNum) throws SQLException {
        Timestamp eventTime = rs.getTimestamp("event_time");
        Timestamp serverTime = rs.getTimestamp("server_time");
        return new VisitorLogItem(
                rs.getString("event_id"),
                rs.getString("event_name"),
                instant(eventTime),
                instant(serverTime),
                rs.getString("session_id"),
                rs.getString("anon_id"),
                rs.getString("page_url"),
                rs.getString("target_url"),
                rs.getString("referrer"),
                rs.getString("user_agent"),
                rs.getString("ip_address"),
                rs.getString("country"),
                rs.getString("region"),
                rs.getString("city"),
                nullableDouble(rs, "latitude"),
                nullableDouble(rs, "longitude"),
                rs.getString("device_type"),
                rs.getString("browser"),
                rs.getString("os"),
                rs.getObject("is_bot") == null ? null : rs.getBoolean("is_bot"),
                parseProperties(rs.getString("properties")));
    }

    private Map<String, Object> parseProperties(String json) {
        if (!hasText(json)) return Map.of();
        try {
            return mapper.readValue(json, MAP_TYPE);
        } catch (Exception ignored) {
            return Map.of();
        }
    }

    private static Double nullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static Instant instant(Timestamp value) {
        return value == null ? null : value.toInstant();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record QueryParts(String fromAndWhere, MapSqlParameterSource params) {}

    public record VisitorQuery(
            String siteId,
            Instant from,
            Instant to,
            String query,
            String event,
            String path,
            String country,
            String city,
            String device,
            String browser,
            String referrer,
            String sessionId,
            int page,
            int size) {}

    public record VisitorLogItem(
            String eventId,
            String eventName,
            Instant eventTime,
            Instant serverTime,
            String sessionId,
            String anonymousId,
            String pageUrl,
            String targetUrl,
            String referrer,
            String userAgent,
            String ipAddress,
            String country,
            String region,
            String city,
            Double latitude,
            Double longitude,
            String deviceType,
            String browser,
            String os,
            Boolean bot,
            Map<String, Object> properties) {}

    public record VisitorSummary(long totalEvents, long uniqueVisitors, long countries, long cities) {}

    public record PageInfo(int number, int size, long totalElements, int totalPages) {}

    public record VisitorQueryResult(
            List<VisitorLogItem> items,
            VisitorSummary summary,
            PageInfo page,
            Instant from,
            Instant to) {}
}
