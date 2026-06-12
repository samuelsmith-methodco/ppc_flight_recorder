-- =============================================================================
-- Lighthouse Rates Flight Recorder — Daily snapshot tables for Snowflake
-- =============================================================================
-- Source: Lighthouse API v3 (api_connection/lighthouse_rate).
-- Each daily run of lighthouse_rates_flight_recorder.py appends one "layer"
-- of data keyed by snapshot_date, so rate evolution over the booking window
-- can be reconstructed (the API only exposes the most recent extract).
-- =============================================================================

-- =============================================================================
-- 0. lighthouse_hotels — Hotels API (/v3/hotels), client hotel dimension
-- One row per subscription; refreshed on every flight recorder run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS lighthouse_hotels (
    subscription_id NUMBER NOT NULL,
    hotel_id NUMBER NOT NULL,
    hotel_name VARCHAR(255),
    stars NUMBER,
    brand_code VARCHAR(64),
    subscribed_features VARCHAR(4000),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (subscription_id)
);

COMMENT ON TABLE lighthouse_hotels IS 'Lighthouse client hotels (Hotels API); subscription_id joins to the *_flight_recorder tables.';
COMMENT ON COLUMN lighthouse_hotels.hotel_id IS 'Unique ID of the physical client hotel.';
COMMENT ON COLUMN lighthouse_hotels.subscribed_features IS 'JSON of subscribed_features: API features and OTAs the subscription has access to.';

-- =============================================================================
-- 0b. lighthouse_hotel_competitors — competitors per compset per subscription
-- One row per (subscription, compset, competitor); replaced on every run so
-- compset changes in the Lighthouse dashboard never leave stale rows.
-- =============================================================================

CREATE TABLE IF NOT EXISTS lighthouse_hotel_competitors (
    subscription_id NUMBER NOT NULL,
    hotel_id NUMBER NOT NULL,
    compset_id NUMBER NOT NULL,
    compset_name VARCHAR(255),
    competitor_id NUMBER NOT NULL,
    competitor_name VARCHAR(255),
    competitor_stars NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (subscription_id, compset_id, competitor_id)
);

COMMENT ON TABLE lighthouse_hotel_competitors IS 'Lighthouse compset membership: which competitor hotels are in which compset, per subscription. competitor_id joins to hotel_id in the *_flight_recorder tables.';
COMMENT ON COLUMN lighthouse_hotel_competitors.hotel_id IS 'Client hotel ID of the subscription.';
COMMENT ON COLUMN lighthouse_hotel_competitors.compset_id IS 'Compset ID relative to the hotel (-1 = RMS, 1 = App Primary, 2 = App Secondary, ...).';

-- =============================================================================
-- 1. lighthouse_rates_flight_recorder — Rates API (/v3/rates)
-- One row per snapshot day per subscription per OTA per hotel (client +
-- competitors) per arrival date per LOS per rate_shop (lowest | best_flex).
-- =============================================================================

CREATE TABLE IF NOT EXISTS lighthouse_rates_flight_recorder (
    snapshot_date DATE NOT NULL,
    subscription_id NUMBER NOT NULL,
    client_hotel_id NUMBER,
    client_hotel_name VARCHAR(255),
    ota VARCHAR(64) NOT NULL,
    hotel_id NUMBER NOT NULL,
    hotel_name VARCHAR(255),
    is_client BOOLEAN,
    arrival_date DATE NOT NULL,
    los NUMBER NOT NULL,
    persons NUMBER NOT NULL DEFAULT 2,
    meal_type NUMBER NOT NULL DEFAULT 0,
    rate_shop VARCHAR(32) NOT NULL DEFAULT 'lowest',
    extract_datetime TIMESTAMP_NTZ,
    rate_value FLOAT,
    currency VARCHAR(10),
    shop_currency VARCHAR(10),
    cancellable BOOLEAN,
    best_flex BOOLEAN,
    vat_incl BOOLEAN,
    city_tax_incl BOOLEAN,
    other_taxes_incl BOOLEAN,
    room_name VARCHAR(500),
    room_type VARCHAR(64),
    max_persons NUMBER,
    meal_type_included NUMBER,
    message VARCHAR(128),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, subscription_id, ota, hotel_id, arrival_date, los, persons, meal_type, rate_shop)
);

COMMENT ON TABLE lighthouse_rates_flight_recorder IS 'Daily snapshots of Lighthouse Rates API (lowest and best_flex shops); one layer per snapshot_date for rate-evolution history.';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.persons IS 'Shop parameter: minimum guest count requested (Lighthouse persons param).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.meal_type IS 'Shop parameter: meal filter (0 = any meal; 1-4 = specific meal plan).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.rate_shop IS 'Rate shop: lowest (bar omitted) or best_flex (bar=true).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.snapshot_date IS 'Date the flight recorder run captured this rate (local run date).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.subscription_id IS 'Lighthouse subscription ID of the client hotel (from Hotels API).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.hotel_id IS 'Hotel that published the rate (client hotel or competitor).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.is_client IS 'TRUE when hotel_id is the client hotel of the subscription.';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.extract_datetime IS 'UTC timestamp when Lighthouse shopped the rate.';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.rate_value IS 'Rate value; 0 when message is set (e.g. rates.soldout).';
COMMENT ON COLUMN lighthouse_rates_flight_recorder.message IS 'Error message when no rate: general.missing, rates.soldout, etc.';

-- =============================================================================
-- 2. lighthouse_roomtype_rates_flight_recorder — Lowest Rates per Roomtype API (/v3/roomtyperates)
-- Same as lighthouse_rates_flight_recorder but one row per roomtype.
-- =============================================================================

CREATE TABLE IF NOT EXISTS lighthouse_roomtype_rates_flight_recorder (
    snapshot_date DATE NOT NULL,
    subscription_id NUMBER NOT NULL,
    client_hotel_id NUMBER,
    client_hotel_name VARCHAR(255),
    ota VARCHAR(64) NOT NULL,
    hotel_id NUMBER NOT NULL,
    hotel_name VARCHAR(255),
    is_client BOOLEAN,
    arrival_date DATE NOT NULL,
    los NUMBER NOT NULL,
    persons NUMBER NOT NULL DEFAULT 2,
    meal_type NUMBER NOT NULL DEFAULT 0,
    room_type VARCHAR(64) NOT NULL DEFAULT '',
    rate_shop VARCHAR(32) NOT NULL DEFAULT 'lowest',
    extract_datetime TIMESTAMP_NTZ,
    rate_value FLOAT,
    currency VARCHAR(10),
    shop_currency VARCHAR(10),
    cancellable BOOLEAN,
    best_flex BOOLEAN,
    vat_incl BOOLEAN,
    city_tax_incl BOOLEAN,
    other_taxes_incl BOOLEAN,
    room_name VARCHAR(500),
    max_persons NUMBER,
    meal_type_included NUMBER,
    message VARCHAR(128),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, subscription_id, ota, hotel_id, arrival_date, los, persons, meal_type, room_type, rate_shop)
);

COMMENT ON TABLE lighthouse_roomtype_rates_flight_recorder IS 'Daily snapshots of Lighthouse Roomtype Rates API (lowest and best_flex shops); one layer per snapshot_date.';
COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.persons IS 'Shop parameter: minimum guest count requested (Lighthouse persons param).';
COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.meal_type IS 'Shop parameter: meal filter (0 = any meal; 1-4 = specific meal plan).';
COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.rate_shop IS 'Rate shop: lowest (bar omitted) or best_flex (bar=true).';
COMMENT ON COLUMN lighthouse_roomtype_rates_flight_recorder.room_type IS 'Roomtype of the rate (standard, suite, premium, ...); empty when unavailable.';

-- =============================================================================
-- 3. lighthouse_parity_flight_recorder — Parity API (/v3/parities)
-- Parity rates flattened: one row per snapshot day per subscription per
-- arrival date per OTA/channel. hotel_id is always the client hotel.
-- =============================================================================

CREATE TABLE IF NOT EXISTS lighthouse_parity_flight_recorder (
    snapshot_date DATE NOT NULL,
    subscription_id NUMBER NOT NULL,
    hotel_id NUMBER NOT NULL,
    hotel_name VARCHAR(255),
    arrival_date DATE NOT NULL,
    los NUMBER NOT NULL,
    parity_currency VARCHAR(10),
    ota VARCHAR(64) NOT NULL,
    channel VARCHAR(64) NOT NULL DEFAULT '',
    is_baserate BOOLEAN,
    position_to_baserate NUMBER,
    extract_datetime TIMESTAMP_NTZ,
    rate_value FLOAT,
    currency VARCHAR(10),
    shop_currency VARCHAR(10),
    cancellable BOOLEAN,
    best_flex BOOLEAN,
    vat_incl BOOLEAN,
    city_tax_incl BOOLEAN,
    other_taxes_incl BOOLEAN,
    room_name VARCHAR(500),
    room_type VARCHAR(64),
    max_persons NUMBER,
    meal_type_included NUMBER,
    message VARCHAR(128),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_date, subscription_id, hotel_id, arrival_date, los, ota, channel)
);

COMMENT ON TABLE lighthouse_parity_flight_recorder IS 'Daily snapshots of Lighthouse Parity API; client hotel rates per OTA/channel per arrival date, one layer per snapshot_date.';
COMMENT ON COLUMN lighthouse_parity_flight_recorder.is_baserate IS 'TRUE when this rate is the reference (baserate, typically branddotcom) within the parity.';
COMMENT ON COLUMN lighthouse_parity_flight_recorder.position_to_baserate IS '-2..2: how the rate compares to the baserate (see Lighthouse Parity API docs).';
COMMENT ON COLUMN lighthouse_parity_flight_recorder.channel IS 'Publishing channel; equals ota unless the ota is a metasearch site.';
