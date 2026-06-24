package site.yuqi.analytics.common.event;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class GranularityTest {

    @Test
    void fiveMinFloorRoundsDownToTheNearestFiveMinuteBoundary() {
        Instant t = ZonedDateTime.of(2026, 6, 23, 12, 7, 34, 0, ZoneOffset.UTC).toInstant();
        Instant expected = ZonedDateTime.of(2026, 6, 23, 12, 5, 0, 0, ZoneOffset.UTC).toInstant();
        assertThat(Granularity.FIVE_MIN.floor(t)).isEqualTo(expected);
    }

    @Test
    void oneDayFloorRoundsDownToMidnightUtc() {
        Instant t = ZonedDateTime.of(2026, 6, 23, 23, 59, 59, 0, ZoneOffset.UTC).toInstant();
        Instant expected = ZonedDateTime.of(2026, 6, 23, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
        assertThat(Granularity.ONE_DAY.floor(t)).isEqualTo(expected);
    }

    @Test
    void codesAreStableForPostgresStorage() {
        assertThat(Granularity.FIVE_MIN.code()).isEqualTo("5m");
        assertThat(Granularity.ONE_DAY.code()).isEqualTo("1d");
    }

    @Test
    void bucketDurationsMatchEnumName() {
        assertThat(Granularity.FIVE_MIN.bucket().toMinutes()).isEqualTo(5);
        assertThat(Granularity.ONE_DAY.bucket().toDays()).isEqualTo(1);
    }
}
