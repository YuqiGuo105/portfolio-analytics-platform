-- ============================================================================
-- analytics-aggregator-service — V5: reset the V3-seeded country centroids
-- so live visitor coordinates can replace them.
--
-- V3 (V3__seed_country_centroids.sql) populated ~51 COUNTRY rows with
-- population-weighted GeoNames centroids. Several of them land in
-- water / wilderness rather than on a city the visitor would recognise:
--   CA (56.13, -106.35)  → central Saskatchewan
--   RU (61.52, 105.32)   → central Siberia
--   AU (-25.27, 133.78)  → uninhabited central desert
--   NO (60.47, 8.47)     → middle of a forest
--   BR (-14.24, -51.93)  → middle of nothing
--   ...
--
-- Until now the GeoAreaCentroidService UPSERT used
-- `coalesce(existing, excluded)`, so the V3 value was never overwritten
-- by a real visitor coordinate. The Java service has just been flipped
-- to prefer `excluded` (the new, coarsened visitor coord) so going
-- forward live traffic will replace these. To make that happen
-- deterministically (and not "eventually, when the next visitor from
-- that country shows up"), we NULL the V3 entries here. The next event
-- through GeoAreaCentroidService.ensureChain for any of these IDs will
-- now hit `coalesce(excluded, NULL)` and write the visitor coord.
--
-- Why NULL and not DELETE: keeping the row preserves the FK target for
-- REGION/METRO children and keeps the public `name` so the dashboard
-- legend still reads "United States" instead of "US".
--
-- The WHERE clause matches BOTH the geo_area_id list AND the V3 seed
-- coordinates exactly, so we never clobber a row a real visitor might
-- have already updated (paranoia — under the old coalesce it can't
-- have happened, but this migration also re-runs cleanly on dev DBs).
-- ============================================================================

update public.geo_areas
   set center_lat = null,
       center_lng = null
 where geo_level = 'COUNTRY'
   and geo_area_id in (
        'COUNTRY:CN','COUNTRY:IN','COUNTRY:JP','COUNTRY:KR','COUNTRY:GB',
        'COUNTRY:DE','COUNTRY:FR','COUNTRY:CA','COUNTRY:AU','COUNTRY:BR',
        'COUNTRY:RU','COUNTRY:MX','COUNTRY:ES','COUNTRY:IT','COUNTRY:NL',
        'COUNTRY:SE','COUNTRY:FI','COUNTRY:SG','COUNTRY:HK','COUNTRY:TW',
        'COUNTRY:ID','COUNTRY:TH','COUNTRY:VN','COUNTRY:PH','COUNTRY:MY',
        'COUNTRY:NZ','COUNTRY:AR','COUNTRY:CL','COUNTRY:CO','COUNTRY:ZA',
        'COUNTRY:EG','COUNTRY:NG','COUNTRY:AE','COUNTRY:SA','COUNTRY:TR',
        'COUNTRY:PL','COUNTRY:CZ','COUNTRY:CH','COUNTRY:AT','COUNTRY:BE',
        'COUNTRY:DK','COUNTRY:NO','COUNTRY:IE','COUNTRY:PT','COUNTRY:GR',
        'COUNTRY:IL','COUNTRY:PK','COUNTRY:BD','COUNTRY:UA','COUNTRY:RO',
        'COUNTRY:HU'
   );
