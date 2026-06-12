-- =============================================================================
-- Lighthouse rates flight recorder: shop dimensions (persons, meal_type)
-- =============================================================================
-- Flight recorder now records multiple shop configurations per day:
--   los (1-45), persons (guest count), meal_type (0 = any meal, 1-4 = meal filter)
-- Compset primary + secondary are fetched in one API call (compsetIds=1,2);
-- Rate Insight filters competitors via lighthouse_hotel_competitors.
-- Existing rows are backfilled with persons=2, meal_type=0 (previous defaults).
-- =============================================================================

ALTER TABLE lighthouse_rates_flight_recorder ADD COLUMN IF NOT EXISTS persons NUMBER;
UPDATE lighthouse_rates_flight_recorder SET persons = 2 WHERE persons IS NULL;
ALTER TABLE lighthouse_rates_flight_recorder ALTER COLUMN persons SET NOT NULL;

ALTER TABLE lighthouse_rates_flight_recorder ADD COLUMN IF NOT EXISTS meal_type NUMBER;
UPDATE lighthouse_rates_flight_recorder SET meal_type = 0 WHERE meal_type IS NULL;
ALTER TABLE lighthouse_rates_flight_recorder ALTER COLUMN meal_type SET NOT NULL;

ALTER TABLE lighthouse_rates_flight_recorder DROP PRIMARY KEY;
ALTER TABLE lighthouse_rates_flight_recorder ADD PRIMARY KEY (
    snapshot_date, subscription_id, ota, hotel_id, arrival_date, los, persons, meal_type, rate_shop
);

COMMENT ON COLUMN lighthouse_rates_flight_recorder.persons IS
    'Shop parameter: minimum guest count requested (Lighthouse persons param).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.meal_type IS
    'Shop parameter: meal filter (0 = any meal; 1-4 = specific meal plan).';

ALTER TABLE lighthouse_roomtype_rates_flight_recorder ADD COLUMN IF NOT EXISTS persons NUMBER;
UPDATE lighthouse_roomtype_rates_flight_recorder SET persons = 2 WHERE persons IS NULL;
ALTER TABLE lighthouse_roomtype_rates_flight_recorder ALTER COLUMN persons SET NOT NULL;

ALTER TABLE lighthouse_roomtype_rates_flight_recorder ADD COLUMN IF NOT EXISTS meal_type NUMBER;
UPDATE lighthouse_roomtype_rates_flight_recorder SET meal_type = 0 WHERE meal_type IS NULL;
ALTER TABLE lighthouse_roomtype_rates_flight_recorder ALTER COLUMN meal_type SET NOT NULL;

ALTER TABLE lighthouse_roomtype_rates_flight_recorder DROP PRIMARY KEY;
ALTER TABLE lighthouse_roomtype_rates_flight_recorder ADD PRIMARY KEY (
    snapshot_date, subscription_id, ota, hotel_id, arrival_date, los, persons, meal_type, room_type, rate_shop
);

COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.persons IS
    'Shop parameter: minimum guest count requested (Lighthouse persons param).';
COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.meal_type IS
    'Shop parameter: meal filter (0 = any meal; 1-4 = specific meal plan).';
