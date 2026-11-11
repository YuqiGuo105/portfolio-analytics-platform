package site.yuqi.analytics.aggregator.web;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.aggregator.service.AnalyticsEventOutboxRelay;
import site.yuqi.analytics.aggregator.governance.GovernanceWorker;
import site.yuqi.analytics.aggregator.governance.GovernanceException;
import site.yuqi.analytics.aggregator.governance.GovernanceModels.MaintenanceResult;

import java.util.Map;
import java.util.LinkedHashMap;

/** Request-scoped wake/drain entry point for a scale-to-zero aggregator. */
@RestController
@RequestMapping("/api/internal/workers")
@RequiredArgsConstructor
public class WorkerRecoveryController {

    private final AnalyticsEventOutboxRelay outbox;
    private final GovernanceWorker governance;

    @PostMapping("/drain")
    public Map<String, Object> drain(@RequestParam(defaultValue = "10000") long maxWaitMs,
                                    @RequestParam(required = false) String target)
            throws InterruptedException {
        if (target != null && !target.equals("visitor-governance")) throw GovernanceException.invalid();
        long deadline = System.currentTimeMillis() + Math.max(1000, Math.min(maxWaitMs, 30000));
        int claimed = 0;
        if (target == null) {
            do {
                claimed += outbox.drainOnce();
                Thread.sleep(500);
            } while (System.currentTimeMillis() < deadline);
        }
        MaintenanceResult maintenance = governance.runIfDue();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "completed");
        response.put("claimed", claimed);
        if (maintenance != null) response.put("governance", maintenance);
        return response;
    }
}
