-- One-time: widen resy_reservation_ledger.table_assignment for multi-table lists
-- (e.g. "21,22,23,...,61"). Fresh installs using resy-pace-flight-recorder-tables.sql
-- already have VARCHAR(1000). Safe to re-run if Snowflake allows the ALTER.

ALTER TABLE resy_reservation_ledger
  ALTER COLUMN table_assignment SET DATA TYPE VARCHAR(1000);
