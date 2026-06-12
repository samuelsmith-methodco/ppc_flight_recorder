-- =============================================================================
-- Lighthouse rates flight recorder: add rate_shop (lowest | best_flex)
-- =============================================================================
-- See leonardo/backend/snowflake/sql/migrate-lighthouse-rate-shop.sql
-- =============================================================================

ALTER TABLE lighthouse_rates_flight_recorder ADD COLUMN IF NOT EXISTS rate_shop VARCHAR(32);
UPDATE lighthouse_rates_flight_recorder SET rate_shop = 'lowest' WHERE rate_shop IS NULL;
ALTER TABLE lighthouse_rates_flight_recorder ALTER COLUMN rate_shop SET NOT NULL;

ALTER TABLE lighthouse_rates_flight_recorder DROP PRIMARY KEY;
ALTER TABLE lighthouse_rates_flight_recorder ADD PRIMARY KEY (
    snapshot_date, subscription_id, ota, hotel_id, arrival_date, los, rate_shop
);

COMMENT ON COLUMN lighthouse_rates_flight_recorder.rate_shop IS
    'Rate shop type: lowest (absolute min) or best_flex (bar=true API shop).';

ALTER TABLE lighthouse_roomtype_rates_flight_recorder ADD COLUMN IF NOT EXISTS rate_shop VARCHAR(32);
UPDATE lighthouse_roomtype_rates_flight_recorder SET rate_shop = 'lowest' WHERE rate_shop IS NULL;
ALTER TABLE lighthouse_roomtype_rates_flight_recorder ALTER COLUMN rate_shop SET NOT NULL;

ALTER TABLE lighthouse_roomtype_rates_flight_recorder DROP PRIMARY KEY;
ALTER TABLE lighthouse_roomtype_rates_flight_recorder ADD PRIMARY KEY (
    snapshot_date, subscription_id, ota, hotel_id, arrival_date, los, room_type, rate_shop
);

COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.rate_shop IS
    'Rate shop type: lowest (absolute min) or best_flex (bar=true API shop).';
