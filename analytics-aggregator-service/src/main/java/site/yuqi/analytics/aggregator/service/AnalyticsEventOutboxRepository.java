package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class AnalyticsEventOutboxRepository {

    private final JdbcTemplate jdbc;

    @Transactional
    public List<OutboxEvent> claimReady(int batchSize, int leaseSeconds) {
        return jdbc.query("""
                with claimed as (
                    select id
                      from public.analytics_event_outbox
                     where (status in ('PENDING', 'FAILED') and next_attempt_at <= now())
                        or (status = 'PROCESSING' and lease_until <= now())
                     order by created_at
                     for update skip locked
                     limit ?
                )
                update public.analytics_event_outbox outbox
                   set status = 'PROCESSING',
                       attempt_count = attempt_count + 1,
                       lease_until = now() + (? * interval '1 second'),
                       updated_at = now()
                  from claimed
                 where outbox.id = claimed.id
                returning outbox.id, outbox.event_id, outbox.partition_key,
                          outbox.payload::text, outbox.attempt_count
                """,
                (rs, rowNum) -> new OutboxEvent(
                        rs.getLong("id"),
                        rs.getString("event_id"),
                        rs.getString("partition_key"),
                        rs.getString("payload"),
                        rs.getInt("attempt_count")),
                batchSize,
                leaseSeconds);
    }

    public void markSent(long id) {
        jdbc.update("""
                update public.analytics_event_outbox
                   set status = 'SENT', sent_at = now(), lease_until = null,
                       last_error = null, updated_at = now()
                 where id = ? and status = 'PROCESSING'
                """, id);
    }

    public void markFailed(long id, String error) {
        jdbc.update("""
                update public.analytics_event_outbox
                   set status = 'FAILED', lease_until = null,
                       next_attempt_at = now() +
                           (least(300, power(2, least(attempt_count, 8))) * interval '1 second'),
                       last_error = left(?, 1000), updated_at = now()
                 where id = ? and status = 'PROCESSING'
                """, error == null ? "Kafka publish failed" : error, id);
    }

    public record OutboxEvent(
            long id,
            String eventId,
            String partitionKey,
            String payload,
            int attemptCount) {
    }
}

