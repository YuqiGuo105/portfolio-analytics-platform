package site.yuqi.analytics.common.event;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Output of {@code GeoSnapService} — the only spatial representation that
 * survives enrichment. There is no raw lat/lng field and there cannot be:
 * adding one is a privacy regression.
 *
 * @param geoLevel  Bucket coarseness ({@link GeoLevel#METRO} is the floor).
 * @param geoAreaId FK into {@code geo_areas.geo_area_id}.
 * @param country   ISO 3166-1 alpha-2.
 * @param region    First-level subdivision (ISO 3166-2 suffix when known).
 * @param metro     Human-readable metro name when {@code geoLevel == METRO}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EnrichedGeo(
        GeoLevel geoLevel,
        String geoAreaId,
        String country,
        String region,
        String metro) {
}
