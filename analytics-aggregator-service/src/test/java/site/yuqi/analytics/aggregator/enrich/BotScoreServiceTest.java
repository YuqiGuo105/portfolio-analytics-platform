package site.yuqi.analytics.aggregator.enrich;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BotScoreServiceTest {

    private final BotScoreService svc = new BotScoreService();

    @Test
    void detectedBotUaScoresOne() {
        assertThat(svc.score("bot", "https://example.com")).isEqualTo(1.0);
        assertThat(svc.isBot(1.0)).isTrue();
    }

    @Test
    void unknownUaWithoutReferrerScoresAboveBaseline() {
        double s = svc.score("unknown", null);
        assertThat(s).isGreaterThan(0.5);
        assertThat(svc.isBot(s)).isFalse();
    }

    @Test
    void desktopWithReferrerScoresZero() {
        assertThat(svc.score("desktop", "https://google.com")).isEqualTo(0.0);
    }

    @Test
    void mobileWithoutReferrerStaysZero() {
        // Mobile without referrer is normal (deep link, share-sheet, etc.)
        // — do NOT bump the score for it.
        assertThat(svc.score("mobile", null)).isEqualTo(0.0);
    }

    @Test
    void scoreNeverExceedsOne() {
        assertThat(svc.score("unknown", null)).isLessThanOrEqualTo(1.0);
    }

    @Test
    void validManagedAssessmentOverridesHeuristic() {
        double score = svc.score("bot", null, Map.of(
                "botAssessmentProvider", "GOOGLE_RECAPTCHA_ENTERPRISE",
                "botAssessmentStatus", "VALID",
                "botAssessmentHumanScore", 0.95));

        assertThat(score).isCloseTo(0.05, org.assertj.core.data.Offset.offset(0.000001));
        assertThat(svc.isBot(score)).isFalse();
    }

    @Test
    void verifiedManagedBotAlwaysScoresOne() {
        double score = svc.score("desktop", "https://google.com", Map.of(
                "botAssessmentProvider", "GOOGLE_RECAPTCHA_ENTERPRISE",
                "botAssessmentStatus", "VALID",
                "botAssessmentHumanScore", 0.9,
                "botAssessmentVerifiedBot", true));

        assertThat(score).isEqualTo(1.0);
        assertThat(svc.isBot(score)).isTrue();
    }

    @Test
    void malformedOrUnverifiedProviderDataFallsBackToHeuristic() {
        for (Map<String, Object> properties : java.util.List.of(
                Map.<String, Object>of("botAssessmentProvider", "CLIENT", "botAssessmentStatus", "VALID", "botAssessmentHumanScore", 1),
                Map.<String, Object>of("botAssessmentProvider", "GOOGLE_RECAPTCHA_ENTERPRISE", "botAssessmentStatus", "ACTION_MISMATCH", "botAssessmentHumanScore", 1),
                Map.<String, Object>of("botAssessmentProvider", "GOOGLE_RECAPTCHA_ENTERPRISE", "botAssessmentStatus", "VALID", "botAssessmentHumanScore", 7))) {
            assertThat(svc.score("unknown", null, properties)).isEqualTo(0.6);
        }
    }
}
