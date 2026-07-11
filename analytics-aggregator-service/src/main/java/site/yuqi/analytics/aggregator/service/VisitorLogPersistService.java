package site.yuqi.analytics.aggregator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.RawEvent;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Writes one event batch to three deliberately separated data tiers:
 * restricted exact raw facts, privacy-safe canonical behavior facts, and the
 * legacy visitor_logs compatibility table. All writes share one transaction.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VisitorLogPersistService {

    private static final Set<String> PROPERTY_ALLOWLIST = Set.of(
            "contentId", "contentType", "category", "progressPercent", "engagedSeconds",
            "component", "action", "campaign", "experimentId", "variant",
            "recommendationRequestId", "rank", "modelVersion", "resultCount");

    private static final String INSERT_RAW = """
            insert into analytics_private.behavior_events_raw
              (event_id, schema_version, site_id, event_name, event_time, server_time,
               session_id, anon_id, page_url, target_url, referrer, user_agent, ip_address,
               country, region, city, latitude, longitude, properties)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            on conflict (event_id) do nothing
            """;

    private static final String INSERT_FACT = """
            insert into public.behavior_events
              (event_id, schema_version, site_id, event_name, event_time, server_time,
               session_id, anon_id_hash, consent_state, page_path, target_path,
               referrer_domain, device_type, browser, os, is_bot, ip_hash,
               country, region, geo_area_id, properties)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            on conflict (event_id) do nothing
            """;

    private static final String INSERT_LEGACY = """
            insert into public.visitor_logs
              (event_id, ip, local_time, event, ua, country, region, city,
               latitude, longitude, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict (event_id) where event_id is not null do nothing
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    @Transactional
    public void persistBatch(List<RawEvent> rawEvents, List<EnrichedEvent> enrichedEvents) {
        Objects.requireNonNull(rawEvents, "rawEvents must not be null");
        Objects.requireNonNull(enrichedEvents, "enrichedEvents must not be null");
        List<RawEvent> raw = rawEvents.stream().filter(VisitorLogPersistService::hasId).toList();
        List<EnrichedEvent> facts = enrichedEvents.stream().filter(VisitorLogPersistService::hasId).toList();
        if (raw.isEmpty() || facts.isEmpty()) return;

        persistRaw(raw);
        persistFacts(facts);
        persistLegacy(facts);
        log.info("{\"event\":\"behavior_batch_insert\",\"raw\":{},\"facts\":{}}", raw.size(), facts.size());
    }

    /** Compatibility entry point retained for legacy callers and focused tests. */
    @Transactional
    public void persistBatch(List<RawEvent> rawEvents) {
        Objects.requireNonNull(rawEvents, "rawEvents must not be null");
        List<RawEvent> raw = rawEvents.stream().filter(VisitorLogPersistService::hasId).toList();
        if (!raw.isEmpty()) {
            persistRaw(raw);
        }
    }

    private void persistRaw(List<RawEvent> events) {
        jdbc.batchUpdate(INSERT_RAW, new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement ps, int i) throws SQLException {
                RawEvent r = events.get(i);
                GeoHint g = r.geoHint();
                ps.setString(1, r.eventId());
                ps.setInt(2, r.schemaVersion() == null ? 1 : r.schemaVersion());
                ps.setString(3, r.siteId());
                ps.setString(4, r.eventType());
                ps.setTimestamp(5, timestamp(r.eventTime()));
                ps.setTimestamp(6, timestamp(r.serverTime()));
                ps.setString(7, r.sessionId());
                ps.setString(8, r.anonId());
                ps.setString(9, truncate(r.pageUrl(), 2048));
                ps.setString(10, truncate(r.targetUrl(), 2048));
                ps.setString(11, truncate(r.referrer(), 2048));
                ps.setString(12, truncate(r.uaRaw(), 1024));
                ps.setString(13, r.ipRaw());
                ps.setString(14, g == null ? null : g.country());
                ps.setString(15, g == null ? null : g.region());
                ps.setString(16, g == null ? null : g.city());
                setDouble(ps, 17, g == null ? null : g.lat());
                setDouble(ps, 18, g == null ? null : g.lng());
                ps.setString(19, json(sanitizeProperties(r.properties())));
            }
            @Override public int getBatchSize() { return events.size(); }
        });
    }

    private void persistFacts(List<EnrichedEvent> events) {
        jdbc.batchUpdate(INSERT_FACT, new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement ps, int i) throws SQLException {
                EnrichedEvent e = events.get(i);
                ps.setString(1, e.eventId());
                ps.setInt(2, e.schemaVersion());
                ps.setString(3, e.siteId());
                ps.setString(4, e.eventType());
                ps.setTimestamp(5, timestamp(e.eventTime()));
                ps.setTimestamp(6, timestamp(e.serverTime()));
                ps.setString(7, behaviorSessionKey(e));
                ps.setString(8, e.anonId());
                ps.setString(9, normalizeConsent(e.consentState()));
                ps.setString(10, pathOnly(e.pageUrl()));
                ps.setString(11, pathOnly(e.targetUrl()));
                ps.setString(12, domainOnly(e.referrer()));
                ps.setString(13, e.deviceType());
                ps.setString(14, e.browser());
                ps.setString(15, e.os());
                ps.setBoolean(16, e.bot());
                ps.setString(17, e.ipHash());
                ps.setString(18, e.geo() == null ? null : e.geo().country());
                ps.setString(19, e.geo() == null ? null : e.geo().region());
                ps.setString(20, e.geo() == null ? null : e.geo().geoAreaId());
                ps.setString(21, json(sanitizeProperties(e.properties())));
            }
            @Override public int getBatchSize() { return events.size(); }
        });
    }

    private void persistLegacy(List<EnrichedEvent> events) {
        jdbc.batchUpdate(INSERT_LEGACY, new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement ps, int i) throws SQLException {
                EnrichedEvent e = events.get(i);
                ps.setString(1, e.eventId());
                ps.setString(2, e.ipHash() == null ? "" : e.ipHash());
                ps.setTimestamp(3, timestamp(e.eventTime()));
                ps.setString(4, e.eventType());
                ps.setString(5, "device=" + e.deviceType() + ";browser=" + e.browser() + ";os=" + e.os());
                ps.setString(6, e.geo() == null ? null : e.geo().country());
                ps.setString(7, e.geo() == null ? null : e.geo().region());
                ps.setNull(8, Types.VARCHAR);
                ps.setNull(9, Types.DOUBLE);
                ps.setNull(10, Types.DOUBLE);
                ps.setTimestamp(11, timestamp(e.serverTime()));
            }
            @Override public int getBatchSize() { return events.size(); }
        });
    }

    private static boolean hasId(RawEvent event) {
        return event != null && event.eventId() != null && !event.eventId().isBlank();
    }

    private static boolean hasId(EnrichedEvent event) {
        return event != null && event.eventId() != null && !event.eventId().isBlank();
    }

    private static Timestamp timestamp(Instant value) {
        return Timestamp.from(value == null ? Instant.now() : value);
    }

    private static void setDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) ps.setNull(index, Types.DOUBLE); else ps.setDouble(index, value);
    }

    private static Map<String, Object> sanitizeProperties(Map<String, Object> input) {
        if (input == null || input.isEmpty()) return Map.of();
        return input.entrySet().stream()
                .filter(entry -> PROPERTY_ALLOWLIST.contains(entry.getKey()))
                .filter(entry -> entry.getValue() instanceof String
                        || entry.getValue() instanceof Number
                        || entry.getValue() instanceof Boolean)
                .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private String json(Map<String, Object> value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid analytics properties", e);
        }
    }

    private static String normalizeConsent(String value) {
        return Set.of("granted", "denied", "unknown").contains(value) ? value : "unknown";
    }

    private static String behaviorSessionKey(EnrichedEvent event) {
        if (event.sessionId() != null && !event.sessionId().isBlank()) return event.sessionId();
        if (event.anonId() != null && !event.anonId().isBlank()) return event.anonId();
        if (event.ipHash() == null || event.ipHash().isBlank()) return null;
        String device = event.deviceType() == null || event.deviceType().isBlank()
                ? "unknown" : event.deviceType();
        return event.ipHash() + ":" + device;
    }

    private static String pathOnly(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            URI uri = value.startsWith("/") ? URI.create("https://local" + value) : URI.create(value);
            return truncate(uri.getPath(), 512);
        } catch (IllegalArgumentException ignored) {
            return truncate(value.split("[?#]", 2)[0], 512);
        }
    }

    private static String domainOnly(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            URI uri = value.contains("://") ? URI.create(value) : URI.create("https://" + value);
            return truncate(uri.getHost(), 255);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static String truncate(String value, int max) {
        return value == null || value.length() <= max ? value : value.substring(0, max);
    }
}
