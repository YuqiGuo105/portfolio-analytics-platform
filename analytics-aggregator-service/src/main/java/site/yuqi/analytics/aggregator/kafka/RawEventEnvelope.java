package site.yuqi.analytics.aggregator.kafka;

import site.yuqi.analytics.common.event.EnrichedEvent;
import site.yuqi.analytics.common.event.RawEvent;

/** One parsed Kafka record plus the coordinates used by the durable inbox. */
public record RawEventEnvelope(
        String topic,
        int partition,
        long offset,
        RawEvent raw,
        EnrichedEvent enriched) {
}
