package site.yuqi.analytics.aggregator.enrich;

import org.junit.jupiter.api.Test;

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
}
