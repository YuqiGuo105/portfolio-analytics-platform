package site.yuqi.analytics.alerts.web;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.alerts.service.AlertEvaluator;

import java.util.Map;

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
}
