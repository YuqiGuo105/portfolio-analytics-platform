package site.yuqi.analytics.alerts.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;

/** Uses the aggregator classification, not proof that a visitor is human. */
public record AlertRuleFilters(Bot bot) {
    public enum Bot { ALL, EXCLUDE, ONLY }

    public AlertRuleFilters {
        if (bot == null) throw new IllegalArgumentException("filters.bot is required: ALL, EXCLUDE, or ONLY");
    }

    public static AlertRuleFilters all() {
        return new AlertRuleFilters(Bot.ALL);
    }

    @JsonAnySetter
    public void rejectUnknown(String name, Object value) {
        throw new IllegalArgumentException("Unknown rule filter: " + name);
    }
}
