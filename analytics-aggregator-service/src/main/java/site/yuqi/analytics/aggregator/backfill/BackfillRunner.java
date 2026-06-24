package site.yuqi.analytics.aggregator.backfill;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Boots the historical replay. Off by default; flip
 * {@code analytics.backfill.enabled=true} (or {@code ANALYTICS_BACKFILL_ENABLED=true})
 * on a one-shot pod / Render run, let it churn through both tables, then
 * scale back to 0 and turn the flag off.
 *
 * <p>Runs <em>after</em> Flyway because the rollup table must exist, and
 * <em>before</em> the Kafka consumer goes ready (we set
 * {@code analytics.consumer-enabled=false} on the same run by convention).
 */
@Component
@ConditionalOnProperty(name = "analytics.backfill.enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class BackfillRunner implements ApplicationRunner {

    private final BackfillService backfill;

    @Override
    public void run(ApplicationArguments args) {
        long t0 = System.currentTimeMillis();
        int logs = backfill.backfillVisitorLogs();
        int clicks = backfill.backfillVisitorClicks();
        long ms = System.currentTimeMillis() - t0;
        log.info("{\"event\":\"backfill_complete\",\"visitor_logs\":{},\"visitor_clicks\":{},\"elapsed_ms\":{}}",
                logs, clicks, ms);
    }
}
