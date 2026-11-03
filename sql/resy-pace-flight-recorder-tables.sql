-- =============================================================================
-- Resy Pace Flight Recorder — Snowflake tables
-- =============================================================================
-- Source: Resy OS analytics reports (Reservations + covers) plus Toast Standard
-- API for turn minutes. Column shapes follow method-resy-pace Supabase so
-- Leonardo can switch off RESY_SUPABASE_* later with a small remap.
--
-- Written by sync_resy.py (MERGE on natural keys). DDL is auto-applied.
-- Keep in sync with: leonardo/backend/snowflake/sql/resy-pace-flight-recorder-tables.sql
-- Existing DBs with a short table_assignment: migrate-resy-pace-ledger-table-assignment.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- resy_venue — pace venue dimension (ids 1–6 match the existing pace project)
-- resy_venue_id joins Snowflake toast_restaurant.resy_id
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_venue (
    id NUMBER NOT NULL,
    resy_venue_id VARCHAR(32),
    resy_slug VARCHAR(128),
    name VARCHAR(255),
    timezone VARCHAR(64),
    active BOOLEAN DEFAULT TRUE,
    toast_restaurant_guid VARCHAR(64),
    toast_include_rc VARCHAR(4000),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id)
);

COMMENT ON TABLE resy_venue IS 'Resy Pace venues. id is the pace venue id; resy_venue_id matches toast_restaurant.resy_id.';

-- -----------------------------------------------------------------------------
-- resy_reservation_snapshot — covers on the books / realized day
-- PK (venue_id, target_date, snapshot_date)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_reservation_snapshot (
    venue_id NUMBER NOT NULL,
    target_date DATE NOT NULL,
    snapshot_date DATE NOT NULL,
    snapshot_ts TIMESTAMP_NTZ,
    days_out NUMBER,
    covers_on_books NUMBER,
    reservation_count NUMBER,
    party_size_avg FLOAT,
    cancelled_covers_cum NUMBER,
    walkin_covers NUMBER,
    covers_seated NUMBER,
    no_show_covers NUMBER,
    is_eod BOOLEAN,
    is_realized BOOLEAN,
    source VARCHAR(64),
    notes VARCHAR(255),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (venue_id, target_date, snapshot_date)
);

COMMENT ON TABLE resy_reservation_snapshot IS 'Resy booking-pace snapshots. Future rows use snapshot_date=run day; realized rows use snapshot_date=target_date.';

-- -----------------------------------------------------------------------------
-- resy_reservation_ledger — one row per reservation per service date
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_reservation_ledger (
    venue_id NUMBER NOT NULL,
    reservation_id NUMBER NOT NULL,
    target_date DATE NOT NULL,
    party_size NUMBER,
    service VARCHAR(64),
    res_time VARCHAR(16),
    table_assignment VARCHAR(1000),
    visit_tags VARCHAR(4000),
    visit_note VARCHAR(4000),
    last_seen TIMESTAMP_NTZ,
    last_status VARCHAR(64),
    cancelled BOOLEAN,
    cancelled_at TIMESTAMP_NTZ,
    vip BOOLEAN,
    guest_total_visits NUMBER,
    guest_key VARCHAR(64),
    first_seen TIMESTAMP_NTZ,
    lead_days NUMBER,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (venue_id, reservation_id, target_date)
);

COMMENT ON TABLE resy_reservation_ledger IS 'Per-reservation Resy ledger. first_seen is preserved across polls. Celebrations/allergies use visit_tags and visit_note.';

-- -----------------------------------------------------------------------------
-- resy_reservation_status_event — first observation of each status
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_reservation_status_event (
    venue_id NUMBER NOT NULL,
    reservation_id NUMBER NOT NULL,
    target_date DATE NOT NULL,
    status VARCHAR(64) NOT NULL,
    observed_at TIMESTAMP_NTZ,
    prev_seen_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (venue_id, reservation_id, target_date, status)
);

COMMENT ON TABLE resy_reservation_status_event IS 'First time a reservation is seen in a status. Later polls that still see it are ignored.';

-- -----------------------------------------------------------------------------
-- resy_guest — hashed guest key plus profile fields from the reservations report
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_guest (
    guest_key VARCHAR(64) NOT NULL,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(64),
    vip BOOLEAN,
    total_visits NUMBER,
    lifetime_res NUMBER,
    lifetime_walkins NUMBER,
    lifetime_cancels NUMBER,
    lifetime_noshows NUMBER,
    last_visit DATE,
    guest_tags VARCHAR(4000),
    visit_tags VARCHAR(4000),
    allergy_tags VARCHAR(4000),
    special_requests VARCHAR(4000),
    guest_notes VARCHAR(4000),
    bio VARCHAR(4000),
    last_seen TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (guest_key)
);

COMMENT ON TABLE resy_guest IS 'Guest profile from Resy reservations report. guest_key is a salted SHA-256 prefix.';

-- -----------------------------------------------------------------------------
-- resy_daily_covers_actual — covers report (reserved / walk-in / no-show)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_daily_covers_actual (
    venue_id NUMBER NOT NULL,
    service_date DATE NOT NULL,
    service_type NUMBER NOT NULL,
    total_covers NUMBER,
    reserved_covers NUMBER,
    walkin_covers NUMBER,
    waitlist_covers NUMBER,
    no_show_covers NUMBER,
    source VARCHAR(64),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (venue_id, service_date, service_type)
);

COMMENT ON TABLE resy_daily_covers_actual IS 'Authoritative daily covers from the Resy covers report, including walk-ins and no-shows.';

-- -----------------------------------------------------------------------------
-- resy_toast_daily — Toast dine-in turns and net sales by venue / day / service
-- service_type 2 = dinner (open >= 16:00 ET), else 1
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resy_toast_daily (
    venue_id NUMBER NOT NULL,
    business_date DATE NOT NULL,
    service_type NUMBER NOT NULL,
    orders NUMBER,
    covers NUMBER,
    net_sales FLOAT,
    dine_net FLOAT,
    avg_check_per_cover FLOAT,
    clean_covers NUMBER,
    clean_sales FLOAT,
    avg_check_clean FLOAT,
    avg_turn_min FLOAT,
    median_turn_min FLOAT,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (venue_id, business_date, service_type)
);

COMMENT ON TABLE resy_toast_daily IS 'Toast Standard API daily turns. median_turn_min is the dinner/brunch turn Leonardo reads.';
