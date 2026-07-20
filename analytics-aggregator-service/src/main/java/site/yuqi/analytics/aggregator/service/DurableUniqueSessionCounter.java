package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.HexFormat;

/** Exact, transactional unique-session delta for rollup projection updates. */
@Service
@RequiredArgsConstructor
public class DurableUniqueSessionCounter {

    private final JdbcTemplate jdbc;

    public long addAndDelta(String siteId, Timestamp bucketTime, String granularity,
                            String geoLevel, String geoAreaId, String eventType,
                            String sessionKey) {
        if (sessionKey == null || sessionKey.isBlank()) return 0L;
        int inserted = jdbc.update("""
                insert into public.analytics_rollup_sessions
                    (site_id, bucket_time, granularity, geo_level, geo_area_id,
                     event_type, session_hash)
                values (?, ?, ?, ?, ?, ?, ?)
                on conflict do nothing
                """,
                siteId, bucketTime, granularity, geoLevel, geoAreaId,
                eventType, sha256(sessionKey));
        return inserted == 1 ? 1L : 0L;
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
