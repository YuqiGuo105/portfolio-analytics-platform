package site.yuqi.analytics.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.aggregator.service.AnalyticsEventOutboxRelay;

import java.util.Map;

/** Request-scoped wake/drain entry point for a scale-to-zero aggregator. */
@RestController
@RequestMapping("/api/internal/workers")
@RequiredArgsConstructor
public class WorkerRecoveryController {

    private final AnalyticsEventOutboxRelay outbox;

    @PostMapping("/drain")
    public Map<String, Object> drain(@RequestParam(defaultValue = "10000") long maxWaitMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + Math.max(1000, Math.min(maxWaitMs, 30000));
        int claimed = 0;
        do {
            claimed += outbox.drainOnce();
            Thread.sleep(500);
        } while (System.currentTimeMillis() < deadline);
        return Map.of("status", "completed", "claimed", claimed);
    }
}
