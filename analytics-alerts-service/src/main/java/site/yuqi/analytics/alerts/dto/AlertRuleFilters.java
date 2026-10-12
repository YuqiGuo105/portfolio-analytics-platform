package site.yuqi.analytics.alerts.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Locale;
import java.util.Map;

/** Uses the aggregator classification, not proof that a visitor is human. */
public record AlertRuleFilters(Bot bot, @JsonInclude(JsonInclude.Include.NON_NULL) String referrerDomain) {
    public enum Bot { ALL, EXCLUDE, ONLY }

    public AlertRuleFilters {
        if (bot == null) throw new IllegalArgumentException("filters.bot is required: ALL, EXCLUDE, or ONLY");
        if (referrerDomain != null) {
            referrerDomain = referrerDomain.trim().toLowerCase(Locale.ROOT);
            if (!referrerDomain.isEmpty() && (referrerDomain.length() > 253 || !referrerDomain.matches("(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z]{2,63}"))) {
                throw new IllegalArgumentException("filters.referrerDomain must be a hostname, not a URL");
            }
        }
    }

    public AlertRuleFilters(Bot bot) { this(bot, null); }
    public boolean hasReferrer() { return referrerDomain != null && !referrerDomain.isEmpty(); }
    public Map<String, Object> asMap() {
        return referrerDomain != null ? Map.of("bot", bot.name(), "referrerDomain", referrerDomain) : Map.of("bot", bot.name());
    }
    public AlertRuleFilters mergedWith(AlertRuleFilters before) {
        return new AlertRuleFilters(bot, referrerDomain != null ? referrerDomain : before == null ? "" : before.referrerDomain());
    }

    public static AlertRuleFilters all() {
        return new AlertRuleFilters(Bot.ALL);
    }

    @JsonAnySetter
    public void rejectUnknown(String name, Object value) {
        throw new IllegalArgumentException("Unknown rule filter: " + name);
    }
}
