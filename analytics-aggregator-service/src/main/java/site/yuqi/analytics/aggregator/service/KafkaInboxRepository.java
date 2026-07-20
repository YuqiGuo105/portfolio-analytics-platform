package site.yuqi.analytics.aggregator.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import site.yuqi.analytics.aggregator.kafka.RawEventEnvelope;

@Repository
@RequiredArgsConstructor
public class KafkaInboxRepository {

    private final JdbcTemplate jdbc;

    @Value("${spring.kafka.consumer.group-id:portfolio-analytics-aggregator}")
    private String consumerGroup;

    /**
     * Claims an event inside the caller's transaction. A committed row means
     * every database projection for the event committed as well.
     */
    public boolean claim(RawEventEnvelope event) {
        int inserted = jdbc.update("""
                insert into public.analytics_kafka_inbox
                    (event_id, consumer_group, kafka_topic, kafka_partition, kafka_offset)
                values (?, ?, ?, ?, ?)
                on conflict do nothing
                """,
                event.enriched().eventId(), consumerGroup,
                event.topic(), event.partition(), event.offset());
        return inserted == 1;
    }
}
