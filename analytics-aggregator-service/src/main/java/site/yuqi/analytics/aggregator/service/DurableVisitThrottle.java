package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import site.yuqi.analytics.common.event.EnrichedEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

/** Transactional replacement for the pre-commit Redis refresh throttle. */
@Service
@RequiredArgsConstructor
public class DurableVisitThrottle {

    private final JdbcTemplate jdbc;

    @Value("${analytics.dedup.throttle-seconds:300}")
    private long throttleSeconds;

    public boolean shouldProcess(EnrichedEvent event) {
        if (!"page_view".equals(event.eventType())) return true;
        String session = sessionKey(event);
        if (session == null || event.pageUrl() == null || event.pageUrl().isBlank()) return true;

        Instant eventTime = event.eventTime() == null ? Instant.now() : event.eventTime();
        Instant expiresAt = eventTime.plusSeconds(Math.max(1, throttleSeconds));
        String key = sha256(event.siteId() + "\n" + session + "\n" + event.pageUrl());

        List<String> claimed = jdbc.query("""
                insert into public.analytics_visit_throttle
                    (throttle_key, event_id, expires_at)
                values (?, ?, ?)
                on conflict (throttle_key) do update
                    set event_id = excluded.event_id,
                        expires_at = excluded.expires_at,
                        updated_at = now()
                where public.analytics_visit_throttle.expires_at <= ?
                returning event_id
                """,
                (rs, rowNum) -> rs.getString(1),
                key, event.eventId(), Timestamp.from(expiresAt), Timestamp.from(eventTime));
        return !claimed.isEmpty();
    }

    private static String sessionKey(EnrichedEvent event) {
        if (notBlank(event.sessionId())) return event.sessionId();
        if (notBlank(event.anonId())) return event.anonId();
        if (!notBlank(event.ipHash())) return null;
        return event.ipHash() + ":" + (notBlank(event.deviceType()) ? event.deviceType() : "unknown");
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
