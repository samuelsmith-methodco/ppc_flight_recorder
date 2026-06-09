-- =============================================================================
-- IDeaS G3 RMS Data Feed Tables for Snowflake (Flight Recorder)
-- =============================================================================
-- Table prefix: ideas_flight_recorder_
-- Source: SFTPCloud daily property archives (*.tar.gz) containing PSV files.
-- Spec: IDeaS Data Feed Specification Overview V8.0 (Core Feed, Table 5).
--
-- PSV structure (all files):
--   Row 1: NO_OF_ROWS=<count>
--   Row 2: pipe-delimited headers
--   Row 3+: data rows (dates as DD-Mon-YYYY; timestamps in US Central)
--
-- Individual file naming:
--   <PropertyCode>_<Category>_<FilePrepareDate>_<FilePrepareTime>_<CutoffDate>_<Frequency>.psv
-- Example: 0014_RoomType_20260605_0007_20260522_Daily.psv
--
-- Daily snapshot model:
--   Each property archive delivered after BDE is one extract identified by
--   file_prepare_date + file_prepare_time (from the PSV/archive filename).
--   INSERT all rows from each daily file; PK includes the extract keys so
--   Jun-5 and Jun-6 loads for the same occupancy_date are both kept.
--
--   occupancy_date     = stay night the metric is FOR
--   file_prepare_date  = snapshot / as-of date (report delivery date)
-- =============================================================================

-- =============================================================================
-- 0. ideas_flight_recorder_property — property lookup from Informational feed
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_property (
    property_code       VARCHAR(16) NOT NULL,
    property_name       VARCHAR(255),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code)
);

COMMENT ON TABLE ideas_flight_recorder_property IS 'IDeaS properties; property_code is the IDeaS property code (e.g. 0014 from account 1111-0014).';
COMMENT ON COLUMN ideas_flight_recorder_property.property_code IS 'IDeaS property code from Informational.Property Code.';

-- =============================================================================
-- 1. ideas_flight_recorder_informational_daily — Core / Informational (Table 5)
-- One row per property per daily extract.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_informational_daily (
    property_code                           VARCHAR(16) NOT NULL,
    file_prepare_date                       DATE NOT NULL,
    file_prepare_time                       VARCHAR(4) NOT NULL,
    cutoff_date                             DATE,
    feed_frequency                          VARCHAR(16) DEFAULT 'Daily',
    property_name                           VARCHAR(255),
    system_date                             TIMESTAMP_NTZ,
    system_mode                             VARCHAR(128),
    build_type                              VARCHAR(128),
    transaction_system_date                 TIMESTAMP_NTZ,
    transaction_data_population_date        TIMESTAMP_NTZ,
    forecast_date                           TIMESTAMP_NTZ,
    control_date                            TIMESTAMP_NTZ,
    last_rate_shopping_extract_date         TIMESTAMP_NTZ,
    unqualified_processed_date              TIMESTAMP_NTZ,
    function_space_system_date              TIMESTAMP_NTZ,
    last_reputation_extract_date            TIMESTAMP_NTZ,
    decision_delivery_mode_date             TIMESTAMP_NTZ,
    scheduled_decision_delivery_mode_date   TIMESTAMP_NTZ,
    history_extract_date                    TIMESTAMP_NTZ,
    unprocessed_extract_count               INTEGER,
    processed_extract_count                 INTEGER,
    missing_daily_extract_count             INTEGER,
    unprocessed_rate_shopping_extract_count INTEGER,
    configuration_file_load_date            TIMESTAMP_NTZ,
    out_of_order                            VARCHAR(16),
    rate_shopping_vendor                    VARCHAR(128),
    demand360_last_data_population_date     TIMESTAMP_NTZ,
    booked                                  VARCHAR(16),
    last_ldb_update                         VARCHAR(64),
    bde_forecast_window                     INTEGER,
    bde_decision_window                     INTEGER,
    idp_forecast_window                     INTEGER,
    idp_decision_window                     INTEGER,
    group_pricing_extended_window           INTEGER,
    variable_decision_window                INTEGER,
    source_filename                         VARCHAR(512),
    loaded_at                               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time)
);

COMMENT ON TABLE ideas_flight_recorder_informational_daily IS 'IDeaS Core Informational file: hotel/system status snapshot per daily extract.';

-- =============================================================================
-- 2. ideas_flight_recorder_room_type_daily — Core / RoomType (Table 5)
-- One row per extract × property × occupancy date × room class × room type.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_room_type_daily (
    property_code                                           VARCHAR(16) NOT NULL,
    file_prepare_date                                       DATE NOT NULL,
    file_prepare_time                                       VARCHAR(4) NOT NULL,
    occupancy_date                                          DATE NOT NULL,
    room_class_code                                         VARCHAR(64) NOT NULL,
    room_type_code                                          VARCHAR(64) NOT NULL,
    comparison_date_last_year                               DATE,
    room_type_description                                   VARCHAR(255),
    capacity_this_year                                      INTEGER,
    capacity_comparison_date_last_year_actual               INTEGER,
    occupancy_on_books_this_year                            INTEGER,
    occupancy_on_books_comparison_date_last_year            INTEGER,
    arrivals_this_year                                      INTEGER,
    arrivals_comparison_date_last_year                      INTEGER,
    departures_this_year                                    INTEGER,
    departures_comparison_date_last_year                    INTEGER,
    rooms_na_ooo_this_year                                  INTEGER,
    rooms_na_ooo_comparison_date_last_year                  INTEGER,
    rooms_na_other_this_year                                INTEGER,
    rooms_na_other_comparison_date_last_year                INTEGER,
    cancelled_this_year                                     INTEGER,
    cancelled_comparison_date_last_year                     INTEGER,
    no_show_this_year                                       INTEGER,
    no_show_comparison_date_last_year                       INTEGER,
    booked_room_revenue_this_year                           NUMBER(18, 4),
    booked_room_revenue_comparison_date_last_year           NUMBER(18, 4),
    forecast_occupancy_this_year                            NUMBER(18, 4),
    forecast_occupancy_comparison_date_last_year            NUMBER(18, 4),
    forecast_room_revenue_this_year                         NUMBER(18, 4),
    forecast_room_revenue_comparison_date_last_year         NUMBER(18, 4),
    decisions_overbooking_this_year                         INTEGER,
    decisions_overbooking_comparison_date_last_year         INTEGER,
    decisions_lrv_this_year                                 NUMBER(18, 4),
    decisions_lrv_comparison_date_last_year                 NUMBER(18, 4),
    decisions_bar_los1                                      NUMBER(18, 4),
    decisions_bar_los2                                      NUMBER(18, 4),
    decisions_bar_los3                                      NUMBER(18, 4),
    decisions_bar_los4                                      NUMBER(18, 4),
    decisions_bar_los5                                      NUMBER(18, 4),
    decisions_bar_los6                                      NUMBER(18, 4),
    decisions_bar_los7                                      NUMBER(18, 4),
    optimal_bar_los1                                        NUMBER(18, 4),
    occupancy_on_books_same_time_last_year                  INTEGER,
    booked_room_revenue_same_time_last_year                 NUMBER(18, 4),
    comparison_date_two_years_ago                           DATE,
    occupancy_on_books_same_time_two_years_ago              INTEGER,
    booked_room_revenue_same_time_two_years_ago             NUMBER(18, 4),
    occupancy_on_books_comparison_date_two_years_ago        INTEGER,
    booked_room_revenue_comparison_date_two_years_ago       NUMBER(18, 4),
    cutoff_date                                             DATE,
    feed_frequency                                          VARCHAR(16) DEFAULT 'Daily',
    source_filename                                         VARCHAR(512),
    loaded_at                                               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, room_class_code, room_type_code)
);

COMMENT ON TABLE ideas_flight_recorder_room_type_daily IS 'IDeaS Core RoomType file: daily snapshot of inventory, forecast, and BAR decisions by room type.';
COMMENT ON COLUMN ideas_flight_recorder_room_type_daily.file_prepare_date IS 'Snapshot date: date the daily extract was generated (report delivery date).';
COMMENT ON COLUMN ideas_flight_recorder_room_type_daily.occupancy_date IS 'Stay night the metrics apply to (not the snapshot date).';

-- =============================================================================
-- 3. ideas_flight_recorder_room_class_daily — Core / RoomClass (Table 5)
-- One row per property × occupancy date × room class.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_room_class_daily (
    property_code                                               VARCHAR(16) NOT NULL,
    file_prepare_date                                           DATE NOT NULL,
    file_prepare_time                                           VARCHAR(4) NOT NULL,
    occupancy_date                                              DATE NOT NULL,
    room_class_code                                             VARCHAR(64) NOT NULL,
    comparison_date_last_year                                   DATE,
    remaining_demand_system_unconstrained_this_year             NUMBER(18, 4),
    remaining_demand_system_unconstrained_comparison_ly         NUMBER(18, 4),
    remaining_demand_user_this_year                             NUMBER(18, 4),
    remaining_demand_user_comparison_ly                         NUMBER(18, 4),
    remaining_demand_system_los1                                NUMBER(18, 4),
    remaining_demand_system_los2                                NUMBER(18, 4),
    remaining_demand_system_los3                                NUMBER(18, 4),
    remaining_demand_system_los4                                NUMBER(18, 4),
    remaining_demand_system_los5                                NUMBER(18, 4),
    remaining_demand_system_los6                                NUMBER(18, 4),
    remaining_demand_system_los7                                NUMBER(18, 4),
    remaining_demand_user_los1                                  NUMBER(18, 4),
    remaining_demand_user_los2                                  NUMBER(18, 4),
    remaining_demand_user_los3                                  NUMBER(18, 4),
    remaining_demand_user_los4                                  NUMBER(18, 4),
    remaining_demand_user_los5                                  NUMBER(18, 4),
    remaining_demand_user_los6                                  NUMBER(18, 4),
    remaining_demand_user_los7                                  NUMBER(18, 4),
    cutoff_date                                                 DATE,
    feed_frequency                                              VARCHAR(16) DEFAULT 'Daily',
    source_filename                                             VARCHAR(512),
    loaded_at                                                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, room_class_code)
);

COMMENT ON TABLE ideas_flight_recorder_room_class_daily IS 'IDeaS Core RoomClass file: remaining unconstrained demand by room class and LOS bucket.';

-- =============================================================================
-- 4. ideas_flight_recorder_room_class_configuration — Core / RoomClassConfiguration (Table 5)
-- One row per property × room class × room type (configuration snapshot).
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_room_class_configuration (
    property_code               VARCHAR(16) NOT NULL,
    file_prepare_date           DATE NOT NULL,
    file_prepare_time           VARCHAR(4) NOT NULL,
    room_class_code             VARCHAR(64) NOT NULL,
    room_type_code              VARCHAR(64) NOT NULL,
    room_class_name             VARCHAR(255),
    room_class_description      VARCHAR(255),
    master_class                VARCHAR(16),
    room_type_name              VARCHAR(255),
    room_type_description       VARCHAR(255),
    room_type_capacity          INTEGER,
    discontinued_room_type      BOOLEAN,
    cutoff_date                 DATE,
    feed_frequency              VARCHAR(16) DEFAULT 'Daily',
    source_filename             VARCHAR(512),
    loaded_at                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, room_class_code, room_type_code)
);

COMMENT ON TABLE ideas_flight_recorder_room_class_configuration IS 'IDeaS Core RoomClassConfiguration file: room class/type hierarchy and capacity.';

-- =============================================================================
-- 5. ideas_flight_recorder_market_segment_daily — Core / MarketSegment (Table 5)
-- One row per property × occupancy date × forecast group × business view × segment.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_market_segment_daily (
    property_code                                           VARCHAR(16) NOT NULL,
    file_prepare_date                                       DATE NOT NULL,
    file_prepare_time                                       VARCHAR(4) NOT NULL,
    occupancy_date                                          DATE NOT NULL,
    forecast_group_code                                     VARCHAR(128) NOT NULL,
    business_view_name                                      VARCHAR(128) NOT NULL,
    market_segment_code                                     VARCHAR(128) NOT NULL,
    comparison_date_last_year                               DATE,
    occupancy_on_books_this_year                            INTEGER,
    occupancy_on_books_comparison_date_last_year            INTEGER,
    arrivals_this_year                                      INTEGER,
    arrivals_comparison_date_last_year                      INTEGER,
    departures_this_year                                    INTEGER,
    departures_comparison_date_last_year                    INTEGER,
    cancelled_this_year                                     INTEGER,
    cancelled_comparison_date_last_year                     INTEGER,
    no_show_this_year                                       INTEGER,
    no_show_comparison_date_last_year                       INTEGER,
    booked_room_revenue_this_year                           NUMBER(18, 4),
    booked_room_revenue_comparison_date_last_year           NUMBER(18, 4),
    forecast_occupancy_this_year                            NUMBER(18, 4),
    forecast_occupancy_comparison_date_last_year            NUMBER(18, 4),
    forecast_room_revenue_this_year                         NUMBER(18, 4),
    forecast_room_revenue_comparison_date_last_year           NUMBER(18, 4),
    occupancy_on_books_same_time_last_year                  INTEGER,
    booked_room_revenue_same_time_last_year                 NUMBER(18, 4),
    comparison_date_two_years_ago                           DATE,
    occupancy_on_books_same_time_two_years_ago              INTEGER,
    booked_room_revenue_same_time_two_years_ago             NUMBER(18, 4),
    occupancy_on_books_comparison_date_two_years_ago        INTEGER,
    booked_room_revenue_comparison_date_two_years_ago       NUMBER(18, 4),
    cutoff_date                                             DATE,
    feed_frequency                                          VARCHAR(16) DEFAULT 'Daily',
    source_filename                                         VARCHAR(512),
    loaded_at                                               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, forecast_group_code, business_view_name, market_segment_code)
);

COMMENT ON TABLE ideas_flight_recorder_market_segment_daily IS 'IDeaS Core MarketSegment file: on-books and forecast metrics by market segment.';

-- =============================================================================
-- 6. ideas_flight_recorder_market_segment_configuration — Core / MarketSegmentConfig (Table 5)
-- One row per property × forecast group × business view × market segment.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_market_segment_configuration (
    property_code                   VARCHAR(16) NOT NULL,
    file_prepare_date               DATE NOT NULL,
    file_prepare_time               VARCHAR(4) NOT NULL,
    forecast_group_code             VARCHAR(128) NOT NULL,
    business_view_name              VARCHAR(128) NOT NULL,
    market_segment_code             VARCHAR(128) NOT NULL,
    business_view_description       VARCHAR(255),
    forecast_group_name             VARCHAR(255),
    forecast_group_description      VARCHAR(255),
    market_segment_name             VARCHAR(255),
    market_segment_description      VARCHAR(255),
    business_type                   VARCHAR(64),
    contract                        VARCHAR(64),
    booking                         VARCHAR(64),
    selling                         VARCHAR(64),
    forecast_type                   VARCHAR(64),
    control                         VARCHAR(64),
    linked                          VARCHAR(16),
    priced_by_bar                   VARCHAR(16),
    base_product                    VARCHAR(128),
    cutoff_date                     DATE,
    feed_frequency                  VARCHAR(16) DEFAULT 'Daily',
    source_filename                 VARCHAR(512),
    loaded_at                       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, forecast_group_code, business_view_name, market_segment_code)
);

COMMENT ON TABLE ideas_flight_recorder_market_segment_configuration IS 'IDeaS Core MarketSegmentConfig file: segment hierarchy and yield configuration.';

-- =============================================================================
-- 7. ideas_flight_recorder_forecast_group_wash_remaining_demand_daily
--    Core / ForecastGroup_Wash_RemainingDemand (Table 5)
-- One row per property × occupancy date × forecast group.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_forecast_group_wash_remaining_demand_daily (
    property_code                                               VARCHAR(16) NOT NULL,
    file_prepare_date                                           DATE NOT NULL,
    file_prepare_time                                           VARCHAR(4) NOT NULL,
    occupancy_date                                              DATE NOT NULL,
    forecast_group_code                                         VARCHAR(128) NOT NULL,
    comparison_date_last_year                                   DATE,
    wash_pct_system_this_year                                   NUMBER(18, 4),
    wash_pct_system_comparison_ly                               NUMBER(18, 4),
    wash_pct_user_override_this_year                            NUMBER(18, 4),
    wash_pct_user_override_comparison_ly                        NUMBER(18, 4),
    expiration_date                                             DATE,
    remaining_demand_system_unconstrained_total_this_year       NUMBER(18, 4),
    remaining_demand_system_unconstrained_total_comparison_ly   NUMBER(18, 4),
    remaining_demand_user_total_this_year                       NUMBER(18, 4),
    remaining_demand_user_total_comparison_ly                   NUMBER(18, 4),
    remaining_demand_system_los1                                NUMBER(18, 4),
    remaining_demand_system_los2                                NUMBER(18, 4),
    remaining_demand_system_los3                                NUMBER(18, 4),
    remaining_demand_system_los4                                NUMBER(18, 4),
    remaining_demand_system_los5                                NUMBER(18, 4),
    remaining_demand_system_los6                                NUMBER(18, 4),
    remaining_demand_system_los7                                NUMBER(18, 4),
    remaining_demand_user_los1                                  NUMBER(18, 4),
    remaining_demand_user_los2                                  NUMBER(18, 4),
    remaining_demand_user_los3                                  NUMBER(18, 4),
    remaining_demand_user_los4                                  NUMBER(18, 4),
    remaining_demand_user_los5                                  NUMBER(18, 4),
    remaining_demand_user_los6                                  NUMBER(18, 4),
    remaining_demand_user_los7                                  NUMBER(18, 4),
    cutoff_date                                                 DATE,
    feed_frequency                                              VARCHAR(16) DEFAULT 'Daily',
    source_filename                                             VARCHAR(512),
    loaded_at                                                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, forecast_group_code)
);

COMMENT ON TABLE ideas_flight_recorder_forecast_group_wash_remaining_demand_daily IS 'IDeaS Core ForecastGroup_Wash_RemainingDemand file: group wash % and remaining demand by LOS.';

-- =============================================================================
-- 8. ideas_flight_recorder_hotel_level_daily — Core / HotelLevel (Table 5; spec typo: HoteLevel)
-- One row per property × occupancy date.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_hotel_level_daily (
    property_code                   VARCHAR(16) NOT NULL,
    file_prepare_date               DATE NOT NULL,
    file_prepare_time               VARCHAR(4) NOT NULL,
    occupancy_date                  DATE NOT NULL,
    comparison_date_last_year       DATE,
    hotel_overbooking               INTEGER,
    special_event_name_this_year    VARCHAR(512),
    special_event_name_last_year    VARCHAR(512),
    budgeted_rooms_sold             INTEGER,
    budgeted_room_revenue           NUMBER(18, 4),
    property_wash_pct               NUMBER(18, 4),
    cutoff_date                     DATE,
    feed_frequency                  VARCHAR(16) DEFAULT 'Daily',
    source_filename                 VARCHAR(512),
    loaded_at                       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date)
);

COMMENT ON TABLE ideas_flight_recorder_hotel_level_daily IS 'IDeaS Core HotelLevel file: daily snapshot of property-level overbooking, events, budget, and wash %.';

-- =============================================================================
-- 9. ideas_flight_recorder_pricing_daily — Core / Pricing (Agile) (Table 5)
-- One row per property × occupancy date × rate product × room class × room type.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_pricing_daily (
    property_code           VARCHAR(16) NOT NULL,
    file_prepare_date       DATE NOT NULL,
    file_prepare_time       VARCHAR(4) NOT NULL,
    occupancy_date          DATE NOT NULL,
    rate_product_name       VARCHAR(128) NOT NULL,
    room_class_code         VARCHAR(64) NOT NULL,
    room_type_code          VARCHAR(64) NOT NULL,
    price                   NUMBER(18, 4),
    cutoff_date             DATE,
    feed_frequency          VARCHAR(16) DEFAULT 'Daily',
    source_filename         VARCHAR(512),
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, rate_product_name, room_class_code, room_type_code)
);

COMMENT ON TABLE ideas_flight_recorder_pricing_daily IS 'IDeaS Core Pricing (Agile) file: BAR/product price decisions by room type and occupancy date.';

-- =============================================================================
-- 10. ideas_flight_recorder_forecast_arrivals_departures_daily
--     Core / ForecastArrivalsDepartures (Table 5, V7.3)
-- One row per property × occupancy date.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_forecast_arrivals_departures_daily (
    property_code               VARCHAR(16) NOT NULL,
    file_prepare_date           DATE NOT NULL,
    file_prepare_time           VARCHAR(4) NOT NULL,
    occupancy_date              DATE NOT NULL,
    on_books_arrivals           INTEGER,
    on_books_departures         INTEGER,
    on_books_stay_thrus         INTEGER,
    forecast_arrivals           INTEGER,
    forecast_departures         INTEGER,
    forecast_stay_thrus         INTEGER,
    on_books_adults             INTEGER,
    on_books_children           INTEGER,
    forecast_adults             INTEGER,
    forecast_children           INTEGER,
    cutoff_date                 DATE,
    feed_frequency              VARCHAR(16) DEFAULT 'Daily',
    source_filename             VARCHAR(512),
    loaded_at                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date)
);

COMMENT ON TABLE ideas_flight_recorder_forecast_arrivals_departures_daily IS 'IDeaS Core ForecastArrivalsDepartures file: daily snapshot of arrivals, departures, and stay-through.';

-- =============================================================================
-- 11. ideas_flight_recorder_channel_forecast_daily — ChannelForecast (delivered in feed; not in V8 Table 5)
-- One row per property × occupancy date × channel × source.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_channel_forecast_daily (
    property_code           VARCHAR(16) NOT NULL,
    file_prepare_date       DATE NOT NULL,
    file_prepare_time       VARCHAR(4) NOT NULL,
    occupancy_date          DATE NOT NULL,
    channel                 VARCHAR(128) NOT NULL DEFAULT '',
    source                  VARCHAR(128) NOT NULL,
    on_books                INTEGER,
    on_books_revenue        NUMBER(18, 4),
    net_on_books_revenue    NUMBER(18, 4),
    cost                    NUMBER(18, 4),
    cost_forecast           NUMBER(18, 4),
    average_cost_forecast   NUMBER(18, 4),
    occupancy_forecast      NUMBER(18, 4),
    revenue_forecast        NUMBER(18, 4),
    net_revenue_forecast    NUMBER(18, 4),
    adr_forecast            NUMBER(18, 4),
    net_adr_forecast        NUMBER(18, 4),
    revpar_forecast         NUMBER(18, 4),
    net_revpar_forecast     NUMBER(18, 4),
    cutoff_date             DATE,
    feed_frequency          VARCHAR(16) DEFAULT 'Daily',
    source_filename         VARCHAR(512),
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, channel, source)
);

COMMENT ON TABLE ideas_flight_recorder_channel_forecast_daily IS 'IDeaS ChannelForecast file: channel/source on-books and forecast metrics (present in property daily archive).';

-- =============================================================================
-- 12. ideas_flight_recorder_saved_group_pricing_evaluations — SavedGroupPricingEvaluations
-- One row per property × extract × group evaluation × arrival date × room type.
-- Delivered weekly in the daily property archive (may be empty).
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_saved_group_pricing_evaluations (
    property_code                   VARCHAR(16) NOT NULL,
    file_prepare_date               DATE NOT NULL,
    file_prepare_time               VARCHAR(4) NOT NULL,
    unique_group_id                 VARCHAR(128) NOT NULL,
    evaluated_on                    TIMESTAMP_NTZ NOT NULL,
    arrival_date                    DATE NOT NULL,
    room_type_code                  VARCHAR(64) NOT NULL DEFAULT '',
    evaluation_method               VARCHAR(128) NOT NULL DEFAULT '',
    group_name                      VARCHAR(512),
    salesperson                     VARCHAR(255),
    materialization                 VARCHAR(64),
    multiproperty_evaluation        VARCHAR(64),
    preferred_date                  DATE,
    market_segment_code             VARCHAR(64) DEFAULT '',
    number_of_nights                INTEGER,
    number_of_rooms                 INTEGER,
    recommended_rate                NUMBER(18, 4),
    adjusted_rate                   NUMBER(18, 4),
    break_even_rate                 NUMBER(18, 4),
    average_mar                     NUMBER(18, 4),
    displaced_rooms                 NUMBER(18, 4),
    incremental_rooms               NUMBER(18, 4),
    gross_revenue_rooms             NUMBER(18, 4),
    cost_rooms                      NUMBER(18, 4),
    net_revenue_rooms               NUMBER(18, 4),
    gross_profit_rooms              NUMBER(18, 4),
    displaced_revenue_rooms         NUMBER(18, 4),
    displaced_profit_rooms          NUMBER(18, 4),
    cost_of_walk                    NUMBER(18, 4),
    net_profit_rooms                NUMBER(18, 4),
    net_profit_pct_rooms            NUMBER(18, 6),
    gross_revenue_ancillary         NUMBER(18, 4),
    net_revenue_ancillary           NUMBER(18, 4),
    gross_profit_ancillary          NUMBER(18, 4),
    displaced_revenue_ancillary     NUMBER(18, 4),
    displaced_profit_ancillary      NUMBER(18, 4),
    net_profit_ancillary            NUMBER(18, 4),
    net_profit_pct_ancillary        NUMBER(18, 6),
    gross_revenue_cnb               NUMBER(18, 4),
    total_cost_cnb                  NUMBER(18, 4),
    net_revenue_cnb                 NUMBER(18, 4),
    gross_profit_cnb                NUMBER(18, 4),
    net_profit_cnb                  NUMBER(18, 4),
    net_profit_pct_cnb              NUMBER(18, 6),
    gross_revenue_total             NUMBER(18, 4),
    cost_total                      NUMBER(18, 4),
    net_revenue_total               NUMBER(18, 4),
    gross_profit_total              NUMBER(18, 4),
    displaced_revenue_total         NUMBER(18, 4),
    displaced_profit_total          NUMBER(18, 4),
    net_profit_total                NUMBER(18, 4),
    net_profit_pct                  NUMBER(18, 6),
    adjust_gross_revenue_total      NUMBER(18, 4),
    adjust_gross_profit_total       NUMBER(18, 4),
    adjust_net_profit_total         NUMBER(18, 4),
    adjust_net_profit_pct_total     NUMBER(18, 6),
    rate_contracted                 VARCHAR(64),
    notes                           VARCHAR(4000),
    booking_id                      VARCHAR(128) DEFAULT '',
    cutoff_date                     DATE,
    feed_frequency                  VARCHAR(16) DEFAULT 'Weekly',
    source_filename                 VARCHAR(512),
    loaded_at                       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (
        property_code,
        file_prepare_date,
        file_prepare_time,
        unique_group_id,
        evaluated_on,
        arrival_date,
        room_type_code,
        evaluation_method
    )
);

COMMENT ON TABLE ideas_flight_recorder_saved_group_pricing_evaluations IS 'IDeaS SavedGroupPricingEvaluations file: saved group pricing evaluation outcomes (weekly feed in daily archive).';

-- =============================================================================
-- 13. ideas_flight_recorder_benefit_measurement_monthly — BenefitMeasurement
-- One row per property × extract × calendar month (RMS benefit vs simulated/actual).
-- Delivered monthly in the daily property archive.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_benefit_measurement_monthly (
    property_code                           VARCHAR(16) NOT NULL,
    file_prepare_date                       DATE NOT NULL,
    file_prepare_time                       VARCHAR(4) NOT NULL,
    measurement_month                       DATE NOT NULL,
    occupancy_estimated_benefit             NUMBER(18, 4),
    revenue_estimated_benefit               NUMBER(18, 4),
    adr_estimated_benefit                   NUMBER(18, 4),
    revpar_estimated_benefit                NUMBER(18, 4),
    occupancy_simulated                     NUMBER(18, 4),
    occupancy_actual                        NUMBER(18, 4),
    revenue_simulated                       NUMBER(18, 4),
    revenue_actual                          NUMBER(18, 4),
    adr_simulated                           NUMBER(18, 4),
    adr_actual                              NUMBER(18, 4),
    revpar_simulated                        NUMBER(18, 4),
    revpar_actual                           NUMBER(18, 4),
    occupancy_pct_gain                      NUMBER(18, 6),
    revenue_pct_gain                        NUMBER(18, 6),
    adr_pct_gain                            NUMBER(18, 6),
    revpar_pct_gain                         NUMBER(18, 6),
    ancillary_revenue_estimated_benefit     NUMBER(18, 4),
    ancillary_revenue_simulated             NUMBER(18, 4),
    ancillary_revenue_actual                NUMBER(18, 4),
    ancillary_revenue_pct_gain              NUMBER(18, 6),
    ancillary_profit_estimated_benefit      NUMBER(18, 4),
    ancillary_profit_simulated              NUMBER(18, 4),
    ancillary_profit_actual                 NUMBER(18, 4),
    ancillary_profit_pct_gain               NUMBER(18, 6),
    profit_estimated_benefit                NUMBER(18, 4),
    profit_simulated                        NUMBER(18, 4),
    profit_actual                           NUMBER(18, 4),
    profit_pct_gain                         NUMBER(18, 6),
    propor_estimated_benefit                NUMBER(18, 4),
    propor_simulated                        NUMBER(18, 4),
    propor_actual                           NUMBER(18, 4),
    propor_pct_gain                         NUMBER(18, 6),
    propar_estimated_benefit                NUMBER(18, 4),
    propar_simulated                        NUMBER(18, 4),
    propar_actual                           NUMBER(18, 4),
    propar_pct_gain                         NUMBER(18, 6),
    cutoff_date                             DATE,
    feed_frequency                          VARCHAR(16) DEFAULT 'Monthly',
    source_filename                         VARCHAR(512),
    loaded_at                               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, measurement_month)
);

COMMENT ON TABLE ideas_flight_recorder_benefit_measurement_monthly IS 'IDeaS BenefitMeasurement file: monthly RMS benefit measurement (estimated vs simulated vs actual).';

-- =============================================================================
-- 14. ideas_flight_recorder_ldb_projections_weekly — LDBProjections
-- One row per property × extract × occupancy date × market segment.
-- Delivered weekly in the daily property archive.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ideas_flight_recorder_ldb_projections_weekly (
    property_code           VARCHAR(16) NOT NULL,
    file_prepare_date       DATE NOT NULL,
    file_prepare_time       VARCHAR(4) NOT NULL,
    occupancy_date          DATE NOT NULL,
    market_segment_code     VARCHAR(128) NOT NULL DEFAULT '',
    projected_rooms         INTEGER,
    projected_revenue       NUMBER(18, 4),
    cutoff_date             DATE,
    feed_frequency          VARCHAR(16) DEFAULT 'Weekly',
    source_filename         VARCHAR(512),
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (property_code, file_prepare_date, file_prepare_time, occupancy_date, market_segment_code)
);

COMMENT ON TABLE ideas_flight_recorder_ldb_projections_weekly IS 'IDeaS LDBProjections file: projected rooms and revenue by market segment (weekly feed in daily archive).';
