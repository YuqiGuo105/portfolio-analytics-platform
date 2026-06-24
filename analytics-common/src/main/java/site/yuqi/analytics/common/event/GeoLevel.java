package site.yuqi.analytics.common.event;

/**
 * Spatial bucket coarseness for both the {@code geo_areas} dimension table
 * and the {@code geo_time_rollups} fact rows.
 *
 * <p><b>METRO is the finest bucket.</b> No part of this platform stores
 * city-, zip-, or coordinate-precision data after enrichment — that is a
 * privacy invariant, not a performance choice.
 */
public enum GeoLevel {
    GLOBAL,
    COUNTRY,
    REGION,
    METRO
}
