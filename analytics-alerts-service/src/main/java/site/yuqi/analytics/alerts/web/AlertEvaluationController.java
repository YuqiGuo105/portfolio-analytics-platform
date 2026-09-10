package site.yuqi.analytics.alerts.web;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.server.ResponseStatusException;
import site.yuqi.analytics.alerts.service.AlertEvaluator;

import java.util.Map;
import java.time.Instant;

@RestController
@RequestMapping("/api/internal/alert-evaluation")
@RequiredArgsConstructor
public class AlertEvaluationController {

    private final AlertEvaluator evaluator;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> evaluate() {
        evaluator.tick();
        return Map.of("accepted", true);
    }

    @PostMapping("/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> replay(@RequestBody ReplayRequest request) {
        try {
            boolean matched = evaluator.replay(request.ruleId(), request.from(), request.to());
            return Map.of("accepted", true, "matched", matched,
                    "deliveryVerified", false, "ruleId", request.ruleId());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
        }
    }

    public record ReplayRequest(long ruleId, Instant from, Instant to) {}
}
