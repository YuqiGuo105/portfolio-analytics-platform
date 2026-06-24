package site.yuqi.analytics.common.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsTest {

    @Test
    void topicNamesAreFrozenWireContract() {
        // These names appear in the Next.js ingestion API and any downstream
        // consumer. Changing them is a breaking change — this test exists so
        // that's loud rather than silent.
        assertThat(Topics.RAW).isEqualTo("analytics.raw.events");
        assertThat(Topics.ENRICHED).isEqualTo("analytics.enriched.events");
        assertThat(Topics.DLQ).isEqualTo("analytics.events.dlq");
    }
}
