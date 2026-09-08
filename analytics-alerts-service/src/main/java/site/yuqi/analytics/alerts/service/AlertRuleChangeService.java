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

/**
 * Implements the two-phase prepare/apply pattern for alert rule changes.
 * <p>
 * Prepare: validates the request, computes a diff, and stores a change token
 * in the database with a 5-minute TTL. Each token is single-use.
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

    // ─── Prepare ─────────────────────────────────────────────────────────

    public PreparedChange prepare(PrepareChangeRequest request) {
        validateAction(request.action());
        validatePatch(request.action(), request.patch());

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
                request.reason(), request.actor(), expectedVersion, beforeMap, expiresAt);
        jdbc.update("insert into alert_rule_changes(change_id,pending_json,expires_at) values (?,?,?)",
                changeId, encode(pending), java.sql.Timestamp.from(expiresAt));

        return new PreparedChange(
                changeId, request.action(), request.ruleId(),
                beforeMap, afterMap, diff, warnings, expectedVersion, expiresAt);
    }

    // ─── Apply ───────────────────────────────────────────────────────────

    @Transactional
    public Map<String, Object> apply(ApplyChangeRequest request) {
        if(request.idempotencyKey()==null || request.idempotencyKey().isBlank() || request.idempotencyKey().length()>200)
            throw new IllegalArgumentException("idempotencyKey is required (maximum 200 characters)");
        var rows=jdbc.queryForList("select * from alert_rule_changes where change_id=? for update",request.changeId());
        if(rows.isEmpty()) throw new IllegalArgumentException("Change not found: "+request.changeId());
        var row=rows.get(0);
        if(row.get("response_json")!=null) {
            if(!request.idempotencyKey().equals(row.get("idempotency_key")))
                throw new IllegalStateException("Change already applied using a different idempotency key");
            return decode((String)row.get("response_json"),Map.class);
        }
        var reused=jdbc.queryForList("select change_id from alert_rule_changes where idempotency_key=?",request.idempotencyKey());
        if(!reused.isEmpty()) throw new IllegalStateException("Idempotency key already belongs to another change");
        PendingChange pending=decode((String)row.get("pending_json"),PendingChange.class);
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

        jdbc.update("update alert_rule_changes set idempotency_key=?,response_json=?,applied_at=now() where change_id=?",
                request.idempotencyKey(),encode(response),request.changeId());

        log.info("Applied change {} action={} ruleId={} actor={}",
                pending.changeId(), pending.action(), result.ruleId(), pending.actor());
        return response;
    }

    // ─── Internal ────────────────────────────────────────────────────────

    private AlertRule applyCreate(PendingChange pending) {
        AlertRulePatch p = pending.patch();
        AlertRuleRequest req = new AlertRuleRequest(
                Objects.requireNonNull(p.siteId(), "siteId required for CREATE"),
                Objects.requireNonNull(p.name(), "name required for CREATE"),
                Objects.requireNonNull(p.eventType(), "eventType required for CREATE"),
                p.geoLevel() != null ? p.geoLevel() : "GLOBAL",
                p.geoAreaId(),
                p.granularity() != null ? p.granularity() : "5m",
                p.threshold() != null ? p.threshold() : 0L,
                p.comparator() != null ? p.comparator() : ">=",
                p.cooldownSeconds() != null ? p.cooldownSeconds() : 1800);
        return repo.insert(req, p.enabled() == null || p.enabled());
    }

    private AlertRule applyUpdate(PendingChange pending) {
        AlertRule current = repo.findById(pending.ruleId())
                .orElseThrow(() -> new IllegalArgumentException("Rule not found: " + pending.ruleId()));
        AlertRulePatch p = pending.patch();
        AlertRuleRequest merged = new AlertRuleRequest(
                p.siteId() != null ? p.siteId() : current.siteId(),
                p.name() != null ? p.name() : current.name(),
                p.eventType() != null ? p.eventType() : current.eventType(),
                p.geoLevel() != null ? p.geoLevel() : current.geoLevel(),
                p.geoAreaId() != null ? p.geoAreaId() : current.geoAreaId(),
                p.granularity() != null ? p.granularity() : current.granularity(),
                p.threshold() != null ? p.threshold() : current.threshold(),
                p.comparator() != null ? p.comparator() : current.comparator(),
                p.cooldownSeconds() != null ? p.cooldownSeconds() : current.cooldownSeconds());
        return repo.updateWithVersion(
                        pending.ruleId(), merged, p.enabled(), pending.expectedVersion())
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
        try {
            String beforeJson = objectMapper.writeValueAsString(pending.beforeState());
            String afterJson = objectMapper.writeValueAsString(after);
            jdbc.update("""
                    INSERT INTO alert_rule_revisions (rule_id, version, action, actor, reason, request_id, before_state, after_state)
                    VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)
                    """,
                    after.ruleId(), after.version(), pending.action(),
                    pending.actor(), pending.reason(), pending.changeId(),
                    beforeJson, afterJson);
        } catch (Exception e) {
            throw new IllegalStateException("Could not persist rule revision",e);
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
                after.put("siteId", patch.siteId());
                after.put("name", patch.name());
                after.put("eventType", patch.eventType());
                after.put("geoLevel", patch.geoLevel() != null ? patch.geoLevel() : "GLOBAL");
                after.put("granularity", patch.granularity() != null ? patch.granularity() : "5m");
                after.put("threshold", patch.threshold() != null ? patch.threshold() : 0);
                after.put("comparator", patch.comparator() != null ? patch.comparator() : ">=");
                after.put("cooldownSeconds", patch.cooldownSeconds() != null ? patch.cooldownSeconds() : 1800);
                after.put("enabled", patch.enabled() == null || patch.enabled());
            }
            case "UPDATE" -> {
                if (before != null) after.putAll(ruleToMap(before));
                if (patch.siteId() != null) after.put("siteId", patch.siteId());
                if (patch.name() != null) after.put("name", patch.name());
                if (patch.eventType() != null) after.put("eventType", patch.eventType());
                if (patch.geoLevel() != null) after.put("geoLevel", patch.geoLevel());
                if (patch.geoAreaId() != null) after.put("geoAreaId", patch.geoAreaId());
                if (patch.granularity() != null) after.put("granularity", patch.granularity());
                if (patch.threshold() != null) after.put("threshold", patch.threshold());
                if (patch.comparator() != null) after.put("comparator", patch.comparator());
                if (patch.cooldownSeconds() != null) after.put("cooldownSeconds", patch.cooldownSeconds());
                if (patch.enabled() != null) after.put("enabled", patch.enabled());
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
                Map<String,Object> change=new LinkedHashMap<>();
                change.put("from",oldVal); change.put("to",e.getValue()); diff.put(e.getKey(),change);
            }
        }
        return diff;
    }

    private List<String> computeWarnings(String action, AlertRule before, AlertRulePatch patch) {
        List<String> warnings = new ArrayList<>();
        if (("SET_ENABLED".equals(action) || "UPDATE".equals(action)) && before != null && patch != null
                && Boolean.FALSE.equals(patch.enabled()) && before.enabled()) {
            warnings.add("This will disable the rule. No alerts will fire until re-enabled.");
        }
        if (patch != null && patch.threshold() != null && patch.threshold() == 0) {
            warnings.add("Threshold is 0 — this rule will always fire.");
        }
        return warnings;
    }

    private void validatePatch(String action, AlertRulePatch patch) {
        if (patch == null) {
            throw new IllegalArgumentException("patch is required");
        }
        if ("CREATE".equals(action)) {
            requireText(patch.siteId(), "siteId");
            requireText(patch.name(), "name");
            requireText(patch.eventType(), "eventType");
        } else {
            validateOptionalText(patch.siteId(), "siteId");
            validateOptionalText(patch.name(), "name");
            validateOptionalText(patch.eventType(), "eventType");
        }
        if (patch.geoLevel() != null && !Set.of("GLOBAL", "COUNTRY", "REGION", "METRO").contains(patch.geoLevel())) {
            throw new IllegalArgumentException("geoLevel must be GLOBAL, COUNTRY, REGION, or METRO");
        }
        if (patch.granularity() != null && !Set.of("5m", "1d").contains(patch.granularity())) {
            throw new IllegalArgumentException("granularity must be 5m or 1d");
        }
        if (patch.comparator() != null && !Set.of(">=", "<=").contains(patch.comparator())) {
            throw new IllegalArgumentException("comparator must be >= or <=");
        }
        if (patch.threshold() != null && patch.threshold() < 0) {
            throw new IllegalArgumentException("threshold must be at least 0");
        }
        if (patch.cooldownSeconds() != null && patch.cooldownSeconds() < 60) {
            throw new IllegalArgumentException("cooldownSeconds must be at least 60");
        }
    }

    private void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }

    private void validateOptionalText(String value, String field) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ruleToMap(AlertRule rule) {
        try {
            return objectMapper.convertValue(rule, Map.class);
        } catch (Exception e) {
            return Map.of("ruleId", rule.ruleId());
        }
    }

    private String encode(Object value) {
        try { return objectMapper.copy().findAndRegisterModules().writeValueAsString(value); }
        catch(Exception e) { throw new IllegalArgumentException("Cannot encode prepared change",e); }
    }
    private <T> T decode(String value,Class<T> type) {
        try { return objectMapper.copy().findAndRegisterModules().readValue(value,type); }
        catch(Exception e) { throw new IllegalStateException("Cannot read stored change",e); }
    }

    private record PendingChange(
            String changeId, String action, Long ruleId, AlertRulePatch patch,
            String reason, String actor, int expectedVersion,
            Map<String, Object> beforeState, Instant expiresAt) {}
}
