package site.yuqi.analytics.common.event;

import java.time.Duration;
import java.time.Instant;

/**
 * Time-bucket size for rollup rows in {@code geo_time_rollups}.
 *
 * <p><b>Only two granularities exist by design</b> — anything finer than
 * 5 minutes blows up Postgres row count on the cheap plan; anything in
 * between 5m and 1d (e.g. 1h) is a redundant pre-aggregation that the UI
 * can compute cheaply on top of the 5m table.
 */
public enum Granularity {
    FIVE_MIN(Duration.ofMinutes(5), "5m"),
    ONE_DAY(Duration.ofDays(1),    "1d");

    private final Duration bucket;
    private final String code;

    Granularity(Duration bucket, String code) {
        this.bucket = bucket;
        this.code = code;
    }

    public Duration bucket() {
        return bucket;
    }

    /** Short Postgres-friendly code (e.g. {@code "5m"}). */
    public String code() {
        return code;
    }

    /**
     * Floor an instant down to the start of its bucket.
     * Example: {@code 12:07:34 → 12:05:00} for {@link #FIVE_MIN}.
     */
    public Instant floor(Instant t) {
        long sec = bucket.toSeconds();
        return Instant.ofEpochSecond((t.getEpochSecond() / sec) * sec);
    }
}
