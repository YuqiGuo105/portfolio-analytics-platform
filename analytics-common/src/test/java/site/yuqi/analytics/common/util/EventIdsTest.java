package site.yuqi.analytics.common.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventIdsTest {

    @Test
    void newIdProducesParsableUuidString() {
        String id = EventIds.newId();
        assertThat(UUID.fromString(id)).isNotNull();
    }

    @Test
    void layoutIsUuidV7() {
        UUID u = EventIds.newId(Instant.parse("2026-06-23T00:00:00Z"));
        // Version nibble is the high nibble of the 7th byte = bits 12-15 of MSB.
        assertThat(u.version()).isEqualTo(7);
        // IETF variant ("10xx") — Java reports 2.
        assertThat(u.variant()).isEqualTo(2);
    }

    @Test
    void timeOrderedAcrossSuccessiveCallsAtDifferentMillis() {
        // Two ids generated at strictly different timestamps must compare in
        // wall-clock order via their 48-bit timestamp prefix.
        UUID earlier = EventIds.newId(Instant.parse("2026-06-23T00:00:00Z"));
        UUID later   = EventIds.newId(Instant.parse("2026-06-23T00:00:01Z"));
        assertThat(earlier.toString().compareTo(later.toString())).isLessThan(0);
    }
}
