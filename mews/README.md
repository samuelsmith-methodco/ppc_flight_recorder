# Mews module (PMS Flight Recorder)

- **`client.py`** — `POST` to Connector API with Cursor pagination (`fetch_all`).
- **`normalize.py`** — flatten API objects to snake_case columns aligned with Snowflake DDL.
- **`diff_utils.py`** — value equality for diff rows (same spirit as `sync.py`).
- **`csv_samples/`** — optional sample CSVs used by `scripts/generate_pms_mews_sql.py`.
- **`generated_schema.json`** — per-entity typed column list (regenerate after changing samples).

Snowflake loads (`mews_storage.py`) delete the snapshot/diff partition once, then upload with **`write_pandas` in row batches** (`MEWS_SNOWFLAKE_WRITE_BATCH_ROWS`, default 5000).

Regenerate SQL + schema:

```bash
cd ppc_flight_recorder
python scripts/generate_pms_mews_sql.py
```
