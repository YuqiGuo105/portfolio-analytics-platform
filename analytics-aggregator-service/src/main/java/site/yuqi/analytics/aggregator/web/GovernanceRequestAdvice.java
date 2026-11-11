package site.yuqi.analytics.aggregator.web;

import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.Map;

@RestControllerAdvice(assignableTypes = AdminVisitorGovernanceController.class)
public class GovernanceRequestAdvice {
    @ExceptionHandler({HttpMessageNotReadableException.class, MissingServletRequestParameterException.class})
    public ResponseEntity<Map<String, String>> invalid() {
        return ResponseEntity.badRequest().header("Cache-Control", "no-store")
                .body(Map.of("error", "invalid_request", "message", "Invalid governance request"));
    }

    @ExceptionHandler({DataAccessException.class, TransactionTimedOutException.class})
    public ResponseEntity<Map<String, String>> unavailable() {
        return ResponseEntity.status(503).header("Cache-Control", "no-store")
                .body(Map.of("error", "governance_unavailable", "message", "Visitor governance is unavailable"));
    }
}
