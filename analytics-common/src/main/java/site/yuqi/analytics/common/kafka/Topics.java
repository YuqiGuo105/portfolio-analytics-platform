package site.yuqi.analytics.common.kafka;

/**
 * Canonical Kafka topic names for the analytics pipeline.
 *
 * <p>Aiven Kafka <b>free tier</b> only allows 2 topics, so we have exactly
 * two:
 * <ul>
 *   <li>{@link #RAW} — every event from the Ingestion API. Aggregator
 *       enriches in-process and UPSERTs to Postgres.</li>
 *   <li>{@link #DLQ} — poison-pill records the aggregator couldn't handle.</li>
 * </ul>
 *
 * <p>Both topics use <b>2 partitions</b> (free-tier cap). Retention:
 * {@link #RAW} 7 days, {@link #DLQ} 30 days.
 */
public final class Topics {

    public static final String RAW = "analytics.raw.events";
    public static final String DLQ = "analytics.events.dlq";

    private Topics() {
        // utility
    }
}
