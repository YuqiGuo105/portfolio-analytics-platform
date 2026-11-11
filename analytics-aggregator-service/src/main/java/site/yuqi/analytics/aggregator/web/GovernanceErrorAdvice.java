package site.yuqi.analytics.aggregator.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import site.yuqi.analytics.aggregator.governance.GovernanceException;

import java.util.Map;

@RestControllerAdvice
public class GovernanceErrorAdvice {
    @ExceptionHandler(GovernanceException.class)
    public ResponseEntity<Map<String, String>> error(GovernanceException error) {
        return ResponseEntity.status(error.status()).header("Cache-Control", "no-store")
                .body(Map.of("error", error.code(), "message", error.getMessage()));
    }
}
