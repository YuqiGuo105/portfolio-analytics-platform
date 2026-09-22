package site.yuqi.analytics.aggregator.enrich;

import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Bot likelihood scorer. A server-verified managed assessment wins when it
 * is present; otherwise the deterministic UA/referrer heuristic is used. The score is recorded on every
 * enriched event and the {@code bot} boolean flips on at {@code >= 0.8}
 * so the aggregator can split the {@code bot_ratio} metric.
 *
 * <p>The browser cannot supply these assessment fields directly. The public
 * ingestion API strips them from user properties and adds them only after
 * validating a single-use token with Google reCAPTCHA Enterprise.
 */
@Service
public class BotScoreService {

    public double score(String deviceType, String referrer) {
        return heuristicScore(deviceType, referrer);
    }

    public double score(String deviceType, String referrer, Map<String, Object> properties) {
        Double managedScore = managedBotLikelihood(properties);
        return managedScore == null ? heuristicScore(deviceType, referrer) : managedScore;
    }

    private double heuristicScore(String deviceType, String referrer) {
        double s = 0.0;
        if ("bot".equals(deviceType)) {
            s = 1.0;
        } else if ("unknown".equals(deviceType)) {
            s = 0.5;
        }
        // No referrer + non-mobile is a weak bot signal — bump only a little.
        if ((referrer == null || referrer.isBlank()) && !"mobile".equals(deviceType)) {
            s = Math.min(1.0, s + 0.1);
        }
        return s;
    }

    private Double managedBotLikelihood(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) return null;
        if (!"GOOGLE_RECAPTCHA_ENTERPRISE".equals(properties.get("botAssessmentProvider"))
                || !"VALID".equals(properties.get("botAssessmentStatus"))) {
            return null;
        }
        if (Boolean.TRUE.equals(properties.get("botAssessmentVerifiedBot"))) return 1.0;

        Object value = properties.get("botAssessmentHumanScore");
        if (!(value instanceof Number number)) return null;
        double humanScore = number.doubleValue();
        if (!Double.isFinite(humanScore) || humanScore < 0.0 || humanScore > 1.0) return null;
        return 1.0 - humanScore;
    }

    public boolean isBot(double score) {
        return score >= 0.8;
    }
}
