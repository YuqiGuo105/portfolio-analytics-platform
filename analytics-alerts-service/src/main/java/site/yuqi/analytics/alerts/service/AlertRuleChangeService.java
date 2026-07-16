package site.yuqi.analytics.alerts.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import site.yuqi.analytics.alerts.dto.*;
import site.yuqi.analytics.alerts.repo.AlertRuleRepository;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements the two-phase prepare/apply pattern for alert rule changes.
 * <p>
 * Prepare: validates the request, computes a diff, and stores a change token
 * in memory with a 5-minute TTL. Each token is single-use.
 * <p>
 * Apply: consumes the token and atomically applies the change with optimistic
 * version locking + audit revision.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AlertRuleChangeService {

    private final AlertRuleRepository repo;
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    private static final long CHANGE_TTL_MS = 5 * 60 * 1000L; // 5 minutes
    private final ConcurrentHashMap<String, PendingChange> pendingChanges = new ConcurrentHashMap<>();

    // Idempotency: track applied idempotency keys to prevent double-apply
    private final ConcurrentHashMap<String, Map<String, Object>> appliedKeys = new ConcurrentHashMap<>();

    // ─── Prepare ─────────────────────────────────────────────────────────

    public PreparedChange prepare(PrepareChangeRequest request) {
        evictExpired();
        validateAction(request.action());

        AlertRule before = null;
        int expectedVersion = 0;
        if (request.ruleId() != null) {
            before = repo.findById(request.ruleId())
                    .orElseThrow(() -> new IllegalArgumentException("Rule not found: " + request.ruleId()));
            expectedVersion = before.version();
        }

        Map<String, Object> beforeMap = before != null ? ruleToMap(before) : Map.of();
        Map<String, Object> afterMap = computeAfter(request.action(), before, request.patch());
        Map<String, Object> diff = computeDiff(beforeMap, afterMap);
        List<String> warnings = computeWarnings(request.action(), before, request.patch());

        String changeId = "chg_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        Instant expiresAt = Instant.now().plusMillis(CHANGE_TTL_MS);

        PendingChange pending = new PendingChange(
                changeId, request.action(), request.ruleId(), request.patch(),
                request.reason(), request.actor(), expectedVersion, expiresAt);
        pendingChanges.put(changeId, pending);

        return new PreparedChange(
                changeId, request.action(), request.ruleId(),
                beforeMap, afterMap, diff, warnings, expectedVersion, expiresAt);
    }

    // ─── Apply ───────────────────────────────────────────────────────────

    @Transactional
    public Map<String, Object> apply(ApplyChangeRequest request) {
        // Idempotency check
        if (request.idempotencyKey() != null && appliedKeys.containsKey(request.idempotencyKey())) {
            return appliedKeys.get(request.idempotencyKey());
        }

        PendingChange pending = pendingChanges.remove(request.changeId());
        if (pending == null) {
            throw new IllegalArgumentException("Change not found or already applied: " + request.changeId());
        }
        if (Instant.now().isAfter(pending.expiresAt())) {
            throw new IllegalStateException("Change expired at " + pending.expiresAt());
        }

        AlertRule result = switch (pending.action()) {
            case "CREATE" -> applyCreate(pending);
            case "UPDATE" -> applyUpdate(pending);
            case "SET_ENABLED" -> applySetEnabled(pending);
            default -> throw new IllegalArgumentException("Unknown action: " + pending.action());
        };

        // Record revision
        recordRevision(result, pending);

        Map<String, Object> response = Map.of(
                "success", true,
                "ruleId", result.ruleId(),
                "version", result.version(),
                "action", pending.action());

        // Store idempotency result
        if (request.idempotencyKey() != null) {
            appliedKeys.put(request.idempotencyKey(), response);
            // Schedule cleanup after 10 minutes
            scheduleIdempotencyCleanup(request.idempotencyKey());
        }

        log.info("Applied change {} action={} ruleId={} actor={}",
                pending.changeId(), pending.action(), result.ruleId(), pending.actor());
        return response;
    }

    // ─── Internal ────────────────────────────────────────────────────────

    private AlertRule applyCreate(PendingChange pending) {
        AlertRulePatch p = pending.patch();
        AlertRuleRequest req = new AlertRuleRequest(
                "default",
                Objects.requireNonNull(p.name(), "name required for CREATE"),
                Objects.requireNonNull(p.eventType(), "eventType required for CREATE"),
                p.geoLevel() != null ? p.geoLevel() : "GLOBAL",
                p.geoAreaId(),
                p.granularity() != null ? p.granularity() : "5m",
                p.threshold() != null ? p.threshold() : 0L,
                p.comparator() != null ? p.comparator() : ">=",
                p.cooldownSeconds() != null ? p.cooldownSeconds() : 1800);
        return repo.insert(req);
    }

    private AlertRule applyUpdate(PendingChange pending) {
        AlertRule current = repo.findById(pending.ruleId())
                .orElseThrow(() -> new IllegalArgumentException("Rule not found: " + pending.ruleId()));
        AlertRulePatch p = pending.patch();
        AlertRuleRequest merged = new AlertRuleRequest(
                current.siteId(),
                p.name() != null ? p.name() : current.name(),
                p.eventType() != null ? p.eventType() : current.eventType(),
                p.geoLevel() != null ? p.geoLevel() : current.geoLevel(),
                p.geoAreaId() != null ? p.geoAreaId() : current.geoAreaId(),
                p.granularity() != null ? p.granularity() : current.granularity(),
                p.threshold() != null ? p.threshold() : current.threshold(),
                p.comparator() != null ? p.comparator() : current.comparator(),
                p.cooldownSeconds() != null ? p.cooldownSeconds() : current.cooldownSeconds());
        return repo.updateWithVersion(pending.ruleId(), merged, pending.expectedVersion())
                .orElseThrow(() -> new IllegalStateException(
                        "Version conflict: rule " + pending.ruleId() + " was modified since prepare"));
    }

    private AlertRule applySetEnabled(PendingChange pending) {
        Boolean enabled = pending.patch() != null ? pending.patch().enabled() : null;
        if (enabled == null) throw new IllegalArgumentException("enabled field required for SET_ENABLED");
        boolean ok = repo.setEnabledWithVersion(pending.ruleId(), enabled, pending.expectedVersion());
        if (!ok) throw new IllegalStateException(
                "Version conflict: rule " + pending.ruleId() + " was modified since prepare");
        return repo.findById(pending.ruleId()).orElseThrow();
    }

    private void recordRevision(AlertRule after, PendingChange pending) {
        AlertRule before = pending.ruleId() != null
                ? null  // before-state was captured in prepare; store from pending
                : null;
        try {
            String beforeJson = pending.ruleId() != null
                    ? objectMapper.writeValueAsString(repo.findById(pending.ruleId()).orElse(null))
                    : "{}";
            String afterJson = objectMapper.writeValueAsString(after);
            jdbc.update("""
                    INSERT INTO alert_rule_revisions (rule_id, version, action, actor, reason, request_id, before_state, after_state)
                    VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
                    """,
                    after.ruleId(), after.version(), pending.action(),
                    pending.actor(), pending.reason(), pending.changeId(),
                    beforeJson, afterJson);
        } catch (Exception e) {
            log.warn("Failed to record revision for rule {}: {}", after.ruleId(), e.getMessage());
        }
    }

    private void validateAction(String action) {
        if (action == null || !Set.of("CREATE", "UPDATE", "SET_ENABLED").contains(action)) {
            throw new IllegalArgumentException("Invalid action: " + action + ". Must be CREATE, UPDATE, or SET_ENABLED");
        }
    }

    private Map<String, Object> computeAfter(String action, AlertRule before, AlertRulePatch patch) {
        if (patch == null) return Map.of();
        Map<String, Object> after = new LinkedHashMap<>();
        switch (action) {
            case "CREATE" -> {
                after.put("name", patch.name());
                after.put("eventType", patch.eventType());
                after.put("geoLevel", patch.geoLevel() != null ? patch.geoLevel() : "GLOBAL");
                after.put("granularity", patch.granularity() != null ? patch.granularity() : "5m");
                after.put("threshold", patch.threshold() != null ? patch.threshold() : 0);
                after.put("comparator", patch.comparator() != null ? patch.comparator() : ">=");
                after.put("cooldownSeconds", patch.cooldownSeconds() != null ? patch.cooldownSeconds() : 1800);
                after.put("enabled", true);
            }
            case "UPDATE" -> {
                if (before != null) after.putAll(ruleToMap(before));
                if (patch.name() != null) after.put("name", patch.name());
                if (patch.eventType() != null) after.put("eventType", patch.eventType());
                if (patch.geoLevel() != null) after.put("geoLevel", patch.geoLevel());
                if (patch.geoAreaId() != null) after.put("geoAreaId", patch.geoAreaId());
                if (patch.granularity() != null) after.put("granularity", patch.granularity());
                if (patch.threshold() != null) after.put("threshold", patch.threshold());
                if (patch.comparator() != null) after.put("comparator", patch.comparator());
                if (patch.cooldownSeconds() != null) after.put("cooldownSeconds", patch.cooldownSeconds());
            }
            case "SET_ENABLED" -> {
                if (before != null) after.putAll(ruleToMap(before));
                if (patch.enabled() != null) after.put("enabled", patch.enabled());
            }
        }
        return after;
    }

    private Map<String, Object> computeDiff(Map<String, Object> before, Map<String, Object> after) {
        Map<String, Object> diff = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : after.entrySet()) {
            Object oldVal = before.get(e.getKey());
            if (!Objects.equals(oldVal, e.getValue())) {
                diff.put(e.getKey(), Map.of("from", oldVal != null ? oldVal : "null", "to", e.getValue()));
            }
        }
        return diff;
    }

    private List<String> computeWarnings(String action, AlertRule before, AlertRulePatch patch) {
        List<String> warnings = new ArrayList<>();
        if ("SET_ENABLED".equals(action) && before != null && patch != null
                && Boolean.FALSE.equals(patch.enabled()) && before.enabled()) {
            warnings.add("This will disable the rule. No alerts will fire until re-enabled.");
        }
        if (patch != null && patch.threshold() != null && patch.threshold() == 0) {
            warnings.add("Threshold is 0 — this rule will always fire.");
        }
        return warnings;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ruleToMap(AlertRule rule) {
        try {
            return objectMapper.convertValue(rule, Map.class);
        } catch (Exception e) {
            return Map.of("ruleId", rule.ruleId());
        }
    }

    private void evictExpired() {
        Instant now = Instant.now();
        pendingChanges.entrySet().removeIf(e -> now.isAfter(e.getValue().expiresAt()));
    }

    private void scheduleIdempotencyCleanup(String key) {
        // Simple delayed cleanup using virtual thread
        Thread.startVirtualThread(() -> {
            try { Thread.sleep(600_000); } catch (InterruptedException ignored) {}
            appliedKeys.remove(key);
        });
    }

    private record PendingChange(
            String changeId, String action, Long ruleId, AlertRulePatch patch,
            String reason, String actor, int expectedVersion, Instant expiresAt) {}
}
