package site.yuqi.analytics.common.kafka;

/**
 * Canonical Kafka topic names for the analytics pipeline.
 *
 * <p>These are kept in {@code analytics-common} so that the Ingestion API
 * (Next.js side, via a generated TS constant) and the three Java services
 * cannot drift independently. Aiven Hobbyist plan: keep partition count
 * at <b>2</b> per topic. Retention: {@link #RAW} / {@link #ENRICHED}
 * 7 days; {@link #DLQ} 30 days.
 */
public final class Topics {

    public static final String RAW      = "analytics.raw.events";
    public static final String ENRICHED = "analytics.enriched.events";
    public static final String DLQ      = "analytics.events.dlq";

    private Topics() {
        // utility
    }
}
