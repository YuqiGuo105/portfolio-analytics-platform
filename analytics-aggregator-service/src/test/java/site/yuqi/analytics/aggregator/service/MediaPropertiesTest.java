package site.yuqi.analytics.aggregator.service;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class MediaPropertiesTest {
    @Test
    @SuppressWarnings("unchecked")
    void networkReadingsSurviveWhileInvalidAndPrivateFieldsAreDropped() throws Exception {
        var method = VisitorLogPersistService.class.getDeclaredMethod("sanitizeProperties", Map.class);
        method.setAccessible(true);
        var valid = Map.of("bitrateKbps", 2500, "bandwidthKbps", 8500,
                "downlinkMbps", 9.5, "effectiveType", "4g");
        assertThat((Map<String, Object>) method.invoke(null, valid)).containsAllEntriesOf(valid);
        for (Object invalid : new Object[] { -1, Double.NaN, Double.POSITIVE_INFINITY, 1_000_001, "fast" }) {
            var result = (Map<String, Object>) method.invoke(null, Map.of(
                    "bitrateKbps", invalid, "bandwidthKbps", invalid, "downlinkMbps", invalid,
                    "effectiveType", "private-network-name", "signedUrl", "private"));
            assertThat(result).isEmpty();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void mediaMetadataSurvivesPersistenceWithoutPrivateContent() throws Exception {
        var method = VisitorLogPersistService.class.getDeclaredMethod("sanitizeProperties", Map.class);
        method.setAccessible(true);
        var result = (Map<String, Object>) method.invoke(null, Map.of(
                "mediaId", "asset-v2", "playbackId", "play-1", "watchedSeconds", 15.5,
                "coveragePercent", 25, "placement", "home_intro", "sequence", 3,
                "botAssessmentStatus", "NOT_REQUESTED_COST_POLICY",
                "signedUrl", "private", "transcript", "private"));
        assertThat(result).containsEntry("mediaId", "asset-v2")
                .containsEntry("watchedSeconds", 15.5).containsEntry("sequence", 3)
                .containsEntry("botAssessmentStatus", "NOT_REQUESTED_COST_POLICY")
                .doesNotContainKeys("signedUrl", "transcript");
    }
}
