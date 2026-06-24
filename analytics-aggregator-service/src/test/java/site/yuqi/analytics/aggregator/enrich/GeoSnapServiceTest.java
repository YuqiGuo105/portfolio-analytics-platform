package site.yuqi.analytics.aggregator.enrich;

import org.junit.jupiter.api.Test;
import site.yuqi.analytics.common.event.EnrichedGeo;
import site.yuqi.analytics.common.event.GeoHint;
import site.yuqi.analytics.common.event.GeoLevel;

import static org.assertj.core.api.Assertions.assertThat;

class GeoSnapServiceTest {

    private final GeoSnapService svc = new GeoSnapService();

    @Test
    void nullHintBecomesGlobalBucket() {
        EnrichedGeo g = svc.snap(null);
        assertThat(g.geoLevel()).isEqualTo(GeoLevel.GLOBAL);
        assertThat(g.geoAreaId()).isEqualTo("GLOBAL");
        assertThat(g.metro()).isNull();
    }

    @Test
    void cityAndCountryYieldMetroBucket() {
        EnrichedGeo g = svc.snap(new GeoHint("US", "UT", "Salt Lake City", 40.76, -111.89, "vercel"));
        assertThat(g.geoLevel()).isEqualTo(GeoLevel.METRO);
        assertThat(g.geoAreaId()).startsWith("METRO:US:UT:Salt Lake City");
        assertThat(g.country()).isEqualTo("US");
        assertThat(g.region()).isEqualTo("UT");
        assertThat(g.metro()).isEqualTo("Salt Lake City");
    }

    @Test
    void regionWithoutCityFallsBackToRegionBucket() {
        EnrichedGeo g = svc.snap(new GeoHint("US", "UT", null, null, null, "vercel"));
        assertThat(g.geoLevel()).isEqualTo(GeoLevel.REGION);
        assertThat(g.geoAreaId()).isEqualTo("REGION:US:UT");
        assertThat(g.metro()).isNull();
    }

    @Test
    void countryOnlyFallsBackToCountryBucket() {
        EnrichedGeo g = svc.snap(new GeoHint("US", null, null, null, null, "vercel"));
        assertThat(g.geoLevel()).isEqualTo(GeoLevel.COUNTRY);
        assertThat(g.geoAreaId()).isEqualTo("COUNTRY:US");
    }

    @Test
    void hintWithNothingUsefulFallsToGlobal() {
        EnrichedGeo g = svc.snap(new GeoHint(null, null, null, null, null, "none"));
        assertThat(g.geoLevel()).isEqualTo(GeoLevel.GLOBAL);
    }
}
