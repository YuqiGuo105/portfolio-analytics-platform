package site.yuqi.analytics.alerts.web;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.alerts.dto.*;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;
import site.yuqi.analytics.alerts.service.AlertRuleChangeService;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/alert-rules")
@RequiredArgsConstructor
public class AlertRuleController {

    private final AlertRuleRepository repo;
    private final AlertRuleChangeService changeService;

    @GetMapping
    public List<AlertRule> list(
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) Boolean enabled) {
        List<AlertRule> all = repo.findAll();
        return all.stream()
                .filter(r -> name == null || r.name().toLowerCase().contains(name.toLowerCase()))
                .filter(r -> eventType == null || r.eventType().equalsIgnoreCase(eventType))
                .filter(r -> enabled == null || r.enabled() == enabled)
                .toList();
    }

    @GetMapping("/{id}")
    public ResponseEntity<AlertRule> get(@PathVariable long id) {
        return repo.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public AlertRule create(@Valid @RequestBody AlertRuleRequest body) {
        return repo.insert(body);
    }

    @PutMapping("/{id}")
    public ResponseEntity<AlertRule> update(@PathVariable long id, @Valid @RequestBody AlertRuleRequest body) {
        return repo.update(id, body).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/disable")
    public ResponseEntity<Void> disable(@PathVariable long id) {
        return repo.setEnabled(id, false) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @PatchMapping("/{id}/enable")
    public ResponseEntity<Void> enable(@PathVariable long id) {
        return repo.setEnabled(id, true) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable long id) {
        return repo.delete(id) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    // ─── Prepare / Apply (two-phase change protocol) ─────────────────────

    @PostMapping("/changes/prepare")
    public ResponseEntity<?> prepareChange(@RequestBody PrepareChangeRequest body) {
        try {
            PreparedChange prepared = changeService.prepare(body);
            return ResponseEntity.ok(prepared);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/changes/apply")
    public ResponseEntity<?> applyChange(@RequestBody ApplyChangeRequest body) {
        try {
            Map<String, Object> result = changeService.apply(body);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(409).body(Map.of("error", e.getMessage()));
        }
    }
}
