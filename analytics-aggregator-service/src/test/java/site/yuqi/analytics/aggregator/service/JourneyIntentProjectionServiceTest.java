package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JourneyIntentProjectionServiceTest {

    @Test
    void matchesDatabaseRuleWithoutProductSpecificJavaBranches() {
        boolean matched = JourneyIntentProjectionService.ruleMatches(
                "page_view", "PAGE", "PREFIX", "/work-single/", null,
                null, null,
                "page_view", "/work-single/project-1", null, null, null, null);

        assertThat(matched).isTrue();
    }

    @Test
    void appliesThresholdAndPathConstraintsDeterministically() {
        assertThat(JourneyIntentProjectionService.ruleMatches(
                "read_progress", "ANY", "ANY", null, null,
                null, 50,
                "read_progress", "/blog/a", null, null, null, 49)).isFalse();
        assertThat(JourneyIntentProjectionService.ruleMatches(
                "page_view", "TARGET", "EXACT", "/contact", null,
                null, null,
                "page_view", "/about", "/contact", null, null, null)).isTrue();

        assertThat(JourneyIntentProjectionService.intentLevel(24, 25, 55)).isEqualTo("LOW");
        assertThat(JourneyIntentProjectionService.intentLevel(25, 25, 55)).isEqualTo("MEDIUM");
        assertThat(JourneyIntentProjectionService.intentLevel(55, 25, 55)).isEqualTo("HIGH");
    }
}
