package site.yuqi.analytics.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Automated retention policy for the {@code geo_time_rollups} table.
 *
 * <p>The 5-minute granularity rows are very useful for near-real-time
 * alerting and the live dashboard but are too fine-grained to keep
 * forever — they grow at ~288 rows/area/day. Once the 1-day rollups
 * fully cover the same time range, the 5m rows become redundant (the
 * day-level aggregation already captured their totals via additive
 * UPSERT during ingestion).
 *
 * <p>This scheduled task runs once daily at 03:15 UTC and DELETEs 5m
 * rows whose {@code bucket_time} is older than a configurable threshold
 * (default 3 days).
 *
 * <p><b>Safety</b>: uses a batched approach (deletes at most
 * {@code batch-size} rows per iteration, in a loop) to avoid long-held
 * locks on the shared Supabase instance. A hard limit on total deleted
 * rows per run prevents runaway cleanup if configuration is accidentally
 * set too aggressively.
 */
@Service
@Slf4j
public class RetentionService {

    private final JdbcTemplate jdbc;
    private final boolean enabled;
    private final int retainDays;
    private final int batchSize;
    private final int maxPerRun;

    @Value("${analytics.retention.raw-retain-days:30}")
    private int rawRetainDays = 30;

    @Value("${analytics.retention.inbox-retain-days:14}")
    private int inboxRetainDays = 14;

    @Value("${analytics.retention.outbox-retain-days:14}")
    private int outboxRetainDays = 14;

    public RetentionService(
            JdbcTemplate jdbc,
            @Value("${analytics.retention.enabled:true}") boolean enabled,
            @Value("${analytics.retention.five-min-retain-days:3}") int retainDays,
            @Value("${analytics.retention.batch-size:2000}") int batchSize,
            @Value("${analytics.retention.max-per-run:50000}") int maxPerRun) {
        this.jdbc = jdbc;
        this.enabled = enabled;
        this.retainDays = retainDays;
        this.batchSize = batchSize;
        this.maxPerRun = maxPerRun;
    }

    /**
     * Runs daily at 03:15 UTC. Deletes stale 5m rows in batches.
     */
    @Scheduled(cron = "0 15 3 * * *", zone = "UTC")
    public void purgeStale5mRows() {
        if (!enabled) {
            log.info("{\"event\":\"retention_skip\",\"reason\":\"disabled\"}");
            return;
        }

        log.info("{\"event\":\"retention_start\",\"retainDays\":{},\"batchSize\":{}}",
                retainDays, batchSize);

        int totalDeleted = 0;
        int iterations = 0;

        while (totalDeleted < maxPerRun) {
            int deleted = jdbc.update(
                    "DELETE FROM public.geo_time_rollups " +
                    "WHERE ctid IN (" +
                    "  SELECT ctid FROM public.geo_time_rollups " +
                    "  WHERE granularity = '5m' " +
                    "    AND bucket_time < now() - make_interval(days => ?) " +
                    "  LIMIT ?" +
                    ")",
                    retainDays, batchSize);

            totalDeleted += deleted;
            iterations++;

            if (deleted < batchSize) {
                break; // no more rows to delete
            }

            // Small yield so other queries can proceed between batches
            try { Thread.sleep(100); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("{\"event\":\"retention_done\",\"deleted\":{},\"iterations\":{}}",
                totalDeleted, iterations);
    }

    /** Exact raw facts are intentionally short-lived and never used by public APIs. */
    @Scheduled(cron = "0 45 3 * * *", zone = "UTC")
    public void purgeStaleRawEvents() {
        if (!enabled) return;
        int totalDeleted = 0;
        while (totalDeleted < maxPerRun) {
            int deleted = jdbc.update(
                    "DELETE FROM analytics_private.behavior_events_raw WHERE ctid IN (" +
                    " SELECT ctid FROM analytics_private.behavior_events_raw" +
                    " WHERE created_at < now() - make_interval(days => ?) LIMIT ?)",
                    rawRetainDays, batchSize);
            totalDeleted += deleted;
            if (deleted < batchSize) break;
        }
        log.info("{\"event\":\"raw_retention_done\",\"deleted\":{},\"retainDays\":{}}",
                totalDeleted, rawRetainDays);
    }

    /** Keep the inbox longer than Kafka retention; clear expired throttle/session state. */
    @Scheduled(cron = "0 5 4 * * *", zone = "UTC")
    public void purgeOperationalState() {
        if (!enabled) return;
        int inbox = jdbc.update("delete from public.analytics_kafka_inbox " +
                "where processed_at < now() - make_interval(days => ?)", inboxRetainDays);
        int throttle = jdbc.update("delete from public.analytics_visit_throttle where expires_at < now()");
        int sessions5m = jdbc.update("delete from public.analytics_rollup_sessions " +
                "where granularity = '5m' and bucket_time < now() - make_interval(days => ?)", retainDays);
        int sessions1d = jdbc.update("delete from public.analytics_rollup_sessions " +
                "where granularity = '1d' and bucket_time < now() - make_interval(days => ?)", rawRetainDays);
        int outbox = jdbc.update("delete from public.analytics_event_outbox " +
                "where status = 'SENT' and sent_at < now() - make_interval(days => ?)", outboxRetainDays);
        log.info("{\"event\":\"operational_retention_done\",\"inbox\":{},\"throttle\":{}," +
                        "\"sessions5m\":{},\"sessions1d\":{},\"outbox\":{}}",
                inbox, throttle, sessions5m, sessions1d, outbox);
    }
}
