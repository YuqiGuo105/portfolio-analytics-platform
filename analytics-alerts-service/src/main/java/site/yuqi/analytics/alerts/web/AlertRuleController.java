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
import org.springframework.web.bind.annotation.RestController;
import site.yuqi.analytics.alerts.dto.AlertRule;
import site.yuqi.analytics.alerts.dto.AlertRuleRequest;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import java.util.List;

@RestController
@RequestMapping("/api/alert-rules")
@RequiredArgsConstructor
public class AlertRuleController {

    private final AlertRuleRepository repo;

    @GetMapping
    public List<AlertRule> list() {
        return repo.findAll();
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
}
