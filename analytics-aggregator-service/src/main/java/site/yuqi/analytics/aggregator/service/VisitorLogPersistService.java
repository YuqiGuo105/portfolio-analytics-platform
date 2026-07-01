package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.RawEvent;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Persists raw visitor events into {@code public.visitor_logs} in batch.
 *
 * <p>Historically this table was written per-request by the Portfolio
 * Next.js API route ({@code /api/track}). With the Kafka-primary
 * ingestion switch, the aggregator's {@code RawEventConsumer} now owns
 * this write — it drains one Kafka poll into a single
 * {@code jdbc.batchUpdate}, replacing N per-event Supabase round-trips
 * with one round-trip per poll.
 *
 * <p><b>Idempotency:</b> Kafka delivery is at-least-once and the
 * consumer only ack's after a successful batch persist + rollup upsert.
 * A retried batch would otherwise duplicate rows, so we key on the
 * UUIDv7 {@code event_id} the API route stamps onto every event and
 * use {@code ON CONFLICT (event_id) DO NOTHING} (the partial unique
 * index is created by V6). Rollups have their own upstream dedup guard
 * (see {@code DedupService}), so both writes are independently safe on
 * redelivery.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VisitorLogPersistService {

    /**
     * event_id is the source-of-truth idempotency key; the natural
     * columns (ip / ua / local_time / geo) mirror the shape the legacy
     * API route wrote so downstream consumers of visitor_logs need no
     * changes. latitude and longitude come straight from the geoHint
     * (edge-header derived) rather than from enrichment output — the
     * raw source-of-truth table should retain what the client actually
     * reported, not the coarsened/snapped enrichment result.
     */
    private static final String INSERT_SQL = """
            insert into public.visitor_logs
                (event_id, ip, local_time, event, ua, country, region, city,
                 latitude, longitude, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            on conflict do nothing
            """;

    private final JdbcTemplate jdbc;

    /**
     * Persist a whole poll's worth of raw events in a single
     * {@code batchUpdate} round-trip. Events without an {@code eventId}
     * are skipped (the ON CONFLICT key would be null and the row could
     * duplicate on redelivery — safer to log and drop, since the
     * pipeline requires eventId anyway).
     *
     * @param rawEvents non-null; individual events may be null and are skipped.
     */
    @Transactional
    public void persistBatch(List<RawEvent> rawEvents) {
        Objects.requireNonNull(rawEvents, "rawEvents must not be null");
        if (rawEvents.isEmpty()) return;

        // Filter out unusable rows first so batchUpdate's size matches the
        // number of parameter sets we plan to bind.
        final List<RawEvent> insertable = rawEvents.stream()
                .filter(r -> r != null && r.eventId() != null && !r.eventId().isBlank())
                .toList();
        if (insertable.isEmpty()) return;

        jdbc.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RawEvent r = insertable.get(i);
                GeoHint g = r.geoHint();
                // event_id (idempotency key)
                ps.setString(1, r.eventId());
                // ip is NOT NULL in the table — coerce a missing ipRaw to
                // empty string so we don't blow up the whole batch.
                ps.setString(2, r.ipRaw() == null ? "" : r.ipRaw());
                // local_time = client wall-clock
                ps.setTimestamp(3, r.eventTime() != null
                        ? Timestamp.from(r.eventTime())
                        : Timestamp.from(Instant.now()));
                ps.setString(4, r.eventType());
                ps.setString(5, r.uaRaw());
                if (g == null) {
                    ps.setNull(6, Types.VARCHAR);
                    ps.setNull(7, Types.VARCHAR);
                    ps.setNull(8, Types.VARCHAR);
                    ps.setNull(9, Types.DOUBLE);
                    ps.setNull(10, Types.DOUBLE);
                } else {
                    ps.setString(6, g.country());
                    ps.setString(7, g.region());
                    ps.setString(8, g.city());
                    if (g.lat() != null) ps.setDouble(9, g.lat()); else ps.setNull(9, Types.DOUBLE);
                    if (g.lng() != null) ps.setDouble(10, g.lng()); else ps.setNull(10, Types.DOUBLE);
                }
                // created_at = server wall-clock
                ps.setTimestamp(11, r.serverTime() != null
                        ? Timestamp.from(r.serverTime())
                        : Timestamp.from(Instant.now()));
            }

            @Override
            public int getBatchSize() {
                return insertable.size();
            }
        });

        log.info("{\"event\":\"visitor_logs_batch_insert\",\"attempted\":{},\"skipped_no_id\":{}}",
                insertable.size(), rawEvents.size() - insertable.size());
    }
}
