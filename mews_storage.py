"""
PMS Mews Flight Recorder — Snowflake snapshot + diff (lowercase quoted DDL).

snowflake.connector.pandas_tools imports pandas **and** pyarrow; without pyarrow the
connector treats pandas as missing (MissingDependencyError). Install: pip install pyarrow
(or pip install -r requirements.txt).
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from config import MEWS_SNOWFLAKE_WRITE_BATCH_ROWS, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
from mews.diff_utils import diff_values_equal, format_diff_value
from snowflake.connector.errors import ProgrammingError

from snowflake_connection import execute, execute_query, get_connection

logger = logging.getLogger(__name__)

# write_pandas / Parquet path: Snowflake rejects strings longer than max VARCHAR in table DDL (65535).
SNOWFLAKE_MAX_VARCHAR_CHARS = 65535

SCHEMA_PATH = Path(__file__).resolve().parent / "mews" / "generated_schema.json"
_SCHEMA_CACHE: Optional[Dict[str, List[str]]] = None


def load_generated_schema() -> Dict[str, List[str]]:
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is None:
        if not SCHEMA_PATH.exists():
            raise FileNotFoundError(
                f"Missing {SCHEMA_PATH}; run: python scripts/generate_pms_mews_sql.py"
            )
        with SCHEMA_PATH.open("r", encoding="utf-8") as f:
            _SCHEMA_CACHE = json.load(f)
    return _SCHEMA_CACHE


def _daily_table(logical: str) -> str:
    return f"pms_mews_{logical}_daily"


def _diff_table(logical: str) -> str:
    return f"pms_mews_{logical}_diff_daily"


def _sql_table_fq(logical: str, *, diff: bool = False) -> str:
    """Unquoted table reference for SQL. DDL uses `CREATE TABLE pms_mews_...` (unquoted) so Snowflake
    stores the name in UPPERCASE; quoted lowercase `"pms_mews_..."` does NOT match and fails with 002003."""
    t = _diff_table(logical) if diff else _daily_table(logical)
    if SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA:
        return f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{t}"
    return t


def _pandas_table_name(logical: str, *, diff: bool = False) -> str:
    """Target table for write_pandas with quote_identifiers=True: uppercase matches unquoted DDL."""
    return (_diff_table(logical) if diff else _daily_table(logical)).upper()


def _execute_with_ddl_hint(conn, sql: str, params: Dict[str, Any]) -> None:
    try:
        execute(conn, sql, params)
    except ProgrammingError as e:
        msg = str(e)
        if "002003" in msg or "does not exist" in msg.lower():
            raise RuntimeError(
                f"{msg}\n"
                "Hint: run `sql/pms-mews-flight-recorder-tables.sql` in Snowflake for database/schema "
                f"from .env ({SNOWFLAKE_DATABASE or '(session)'}.{SNOWFLAKE_SCHEMA or '(session)'}). "
                "Table names in DDL are unquoted (stored uppercase); use matching database/schema."
            ) from e
        raise


def delete_snapshot_rows(
    conn,
    logical: str,
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> None:
    fq = _sql_table_fq(logical, diff=False)
    if enterprise_id is not None:
        logger.info(
            '[progress] Snowflake: DELETE FROM %s WHERE snapshot_date=%s enterprise_id=%s …',
            fq, snapshot_date, enterprise_id,
        )
        _execute_with_ddl_hint(
            conn,
            f'DELETE FROM {fq} WHERE "snapshot_date" = %(d)s AND "enterprise_id" = %(e)s',
            {"d": snapshot_date, "e": enterprise_id},
        )
    else:
        logger.info(
            '[progress] Snowflake: DELETE FROM %s WHERE snapshot_date=%s …',
            fq, snapshot_date,
        )
        _execute_with_ddl_hint(
            conn,
            f'DELETE FROM {fq} WHERE "snapshot_date" = %(d)s',
            {"d": snapshot_date},
        )


def delete_diff_rows(
    conn,
    logical: str,
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> None:
    fq = _sql_table_fq(logical, diff=True)
    if enterprise_id is not None:
        logger.info(
            '[progress] Snowflake: DELETE diff %s snapshot_date=%s enterprise_id=%s …',
            fq, snapshot_date, enterprise_id,
        )
        _execute_with_ddl_hint(
            conn,
            f'DELETE FROM {fq} WHERE "snapshot_date" = %(d)s AND "enterprise_id" = %(e)s',
            {"d": snapshot_date, "e": enterprise_id},
        )
    else:
        logger.info(
            '[progress] Snowflake: DELETE diff %s snapshot_date=%s …',
            fq, snapshot_date,
        )
        _execute_with_ddl_hint(
            conn,
            f'DELETE FROM {fq} WHERE "snapshot_date" = %(d)s',
            {"d": snapshot_date},
        )


def _scalar_for_snowflake_parquet(v: Any) -> Any:
    """write_pandas uses Parquet; nested dicts (e.g. empty dict from json_normalize) become
    Arrow structs and PyArrow cannot write structs with no fields. JSON-encode dict/list/tuple/set."""
    if isinstance(v, dict) or isinstance(v, list):
        return json.dumps(v, ensure_ascii=False, default=str)
    if isinstance(v, (tuple, set)):
        return json.dumps(list(v), ensure_ascii=False, default=str)
    return v


def _truncate_string_for_snowflake(v: Any) -> Any:
    """Snowflake VARCHAR(65535) and write_pandas fail if any cell exceeds the limit."""
    if isinstance(v, str) and len(v) > SNOWFLAKE_MAX_VARCHAR_CHARS:
        return v[:SNOWFLAKE_MAX_VARCHAR_CHARS]
    return v


def _cell_for_snowflake_parquet(v: Any) -> Any:
    """JSON-encode nested values, then cap string length for Snowflake."""
    return _truncate_string_for_snowflake(_scalar_for_snowflake_parquet(v))


def _sanitize_dataframe_for_write_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure no object columns hold raw dict/list (Parquet / Snowflake pandas path)."""
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].map(_cell_for_snowflake_parquet)
        elif pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].map(_truncate_string_for_snowflake)
    return out


def _dataframe_for_daily(logical: str, rows: List[Dict[str, Any]]) -> pd.DataFrame:
    schema_cols = load_generated_schema().get(logical, [])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in schema_cols:
        if c not in df.columns:
            df[c] = None
    has_eid = "enterprise_id" in df.columns
    meta = ["snapshot_date"] + (["enterprise_id"] if has_eid else []) + ["entity_id"]
    rest = [c for c in schema_cols if c in df.columns]
    ordered = meta + rest + ["record_json"]
    for col in ordered:
        if col not in df.columns:
            df[col] = None
    return df[ordered]


def _write_pandas_batches(
    conn,
    df: pd.DataFrame,
    table_name: str,
) -> int:
    """Upload DataFrame in row batches (multiple write_pandas calls after a single DELETE)."""
    try:
        from snowflake.connector.pandas_tools import write_pandas
    except ImportError:
        raise RuntimeError("snowflake-connector-python with pandas_tools required")

    n = len(df)
    if n == 0:
        return 0
    batch = MEWS_SNOWFLAKE_WRITE_BATCH_ROWS
    if batch <= 0:
        batch = n
    batch = min(batch, n)
    db = SNOWFLAKE_DATABASE or None
    sch = SNOWFLAKE_SCHEMA or None
    total = 0
    num_batches = (n + batch - 1) // batch
    bi = 0
    for start in range(0, n, batch):
        bi += 1
        part = _sanitize_dataframe_for_write_pandas(df.iloc[start : start + batch])
        part = part.reset_index(drop=True)
        success, num_sf_chunks, nrows, _ = write_pandas(
            conn,
            part,
            table_name,
            database=db,
            schema=sch,
            quote_identifiers=True,
            auto_create_table=False,
            # Keep internal parquet chunks aligned with our row batch to reduce memory spikes
            chunk_size=min(batch, len(part)),
        )
        if not success:
            raise RuntimeError(
                f"write_pandas failed for {table_name} (batch {bi}/{num_batches})"
            )
        total += int(nrows)
        logger.info(
            "[progress] Snowflake: %s write_pandas batch %s/%s rows=%s (connector_chunks=%s)",
            table_name,
            bi,
            num_batches,
            nrows,
            num_sf_chunks,
        )
    logger.info(
        "mews: wrote %s rows to %s in %s batch(es) (batch_size<=%s)",
        total,
        table_name,
        num_batches,
        batch,
    )
    return total


def begin_streaming_snapshot(
    conn,
    logical: str,
    snapshot_date: date,
    enterprise_ids: List[str],
) -> None:
    """DELETE existing snapshot rows for every enterprise once before append-only uploads."""
    for eid in enterprise_ids:
        delete_snapshot_rows(conn, logical, snapshot_date, eid)


def append_daily_snapshot_rows(
    conn,
    logical: str,
    rows: List[Dict[str, Any]],
    snapshot_date: date,
    enterprise_id: str,
) -> int:
    """Append snapshot rows without DELETE (use after begin_streaming_snapshot)."""
    if not rows:
        return 0
    df = _dataframe_for_daily(logical, rows)
    if df.empty:
        return 0
    return _write_pandas_batches(conn, df, _pandas_table_name(logical, diff=False))


def write_daily_snapshot(
    conn,
    logical: str,
    rows: List[Dict[str, Any]],
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> int:
    if not rows:
        return 0

    delete_snapshot_rows(conn, logical, snapshot_date, enterprise_id)
    df = _dataframe_for_daily(logical, rows)
    if df.empty:
        return 0

    return _write_pandas_batches(conn, df, _pandas_table_name(logical, diff=False))


def load_previous_daily(
    conn,
    logical: str,
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> pd.DataFrame:
    prev = snapshot_date - timedelta(days=1)
    fq = _sql_table_fq(logical, diff=False)
    if enterprise_id is not None:
        q = f'SELECT * FROM {fq} WHERE "snapshot_date" = %(d)s AND "enterprise_id" = %(e)s'
        df = execute_query(conn, q, {"d": prev, "e": enterprise_id})
    else:
        q = f'SELECT * FROM {fq} WHERE "snapshot_date" = %(d)s'
        df = execute_query(conn, q, {"d": prev})
    if df is not None and not df.empty:
        df.columns = [str(c).lower() for c in df.columns]
    return df


def load_latest_daily_on_or_before(
    conn,
    logical: str,
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> pd.DataFrame:
    """As-of lookup: latest snapshot on or before ``snapshot_date`` (skips gaps)."""
    fq = _sql_table_fq(logical, diff=False)
    if enterprise_id is not None:
        q = f"""
            SELECT * FROM {fq}
            WHERE "enterprise_id" = %(e)s
              AND "snapshot_date" = (
                  SELECT MAX("snapshot_date") FROM {fq}
                  WHERE "enterprise_id" = %(e)s AND "snapshot_date" <= %(d)s
              )
        """
        df = execute_query(conn, q, {"d": snapshot_date, "e": enterprise_id})
    else:
        q = f"""
            SELECT * FROM {fq}
            WHERE "snapshot_date" = (
                SELECT MAX("snapshot_date") FROM {fq}
                WHERE "snapshot_date" <= %(d)s
            )
        """
        df = execute_query(conn, q, {"d": snapshot_date})
    if df is not None and not df.empty:
        df.columns = [str(c).lower() for c in df.columns]
    return df


def compute_diff_rows(
    logical: str,
    current_rows: List[Dict[str, Any]],
    prev_df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """Field-level diffs + entity added/removed."""
    schema_cols = load_generated_schema().get(logical, [])
    # Skip partition/meta and volatile timestamps (updated_utc changes every sync and is not a business-field diff).
    meta_skip = {
        "snapshot_date",
        "enterprise_id",
        "created_at",
        "record_json",
        "updated_utc",
        "enterprise_updated_utc",
    }

    cur_by_id: Dict[str, Dict[str, Any]] = {}
    for r in current_rows:
        eid = str(r.get("entity_id", ""))
        if eid:
            cur_by_id[eid] = r

    prev_by_id: Dict[str, Dict[str, Any]] = {}
    if prev_df is not None and not prev_df.empty and "entity_id" in prev_df.columns:
        for _, row in prev_df.iterrows():
            eid = str(row.get("entity_id", ""))
            if eid:
                prev_by_id[eid] = row.to_dict()

    cur_ids: Set[str] = set(cur_by_id.keys())
    prev_ids: Set[str] = set(prev_by_id.keys())

    out: List[Dict[str, Any]] = []
    for eid in sorted(cur_ids - prev_ids):
        out.append(
            {
                "entity_id": eid,
                "changed_metric_name": "entity_added",
                "old_value": None,
                "new_value": eid,
            }
        )
    for eid in sorted(prev_ids - cur_ids):
        out.append(
            {
                "entity_id": eid,
                "changed_metric_name": "entity_removed",
                "old_value": eid,
                "new_value": None,
            }
        )

    def _clean(v: Any) -> Any:
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    for eid in sorted(cur_ids & prev_ids):
        cur = cur_by_id[eid]
        prev = prev_by_id[eid]
        for col in schema_cols:
            if col in meta_skip:
                continue
            cv = _clean(cur.get(col))
            pv = _clean(prev.get(col)) if col in prev else None
            if diff_values_equal(pv, cv):
                continue
            # Snowflake/API types often differ (e.g. bool vs "false" vs 0) while stored diff strings match — skip noise.
            fv_old = format_diff_value(pv)
            fv_new = format_diff_value(cv)
            if fv_old == fv_new:
                continue
            out.append(
                {
                    "entity_id": eid,
                    "changed_metric_name": col,
                    "old_value": fv_old,
                    "new_value": fv_new,
                }
            )
    return out


def write_diff_daily(
    conn,
    logical: str,
    diff_rows: List[Dict[str, Any]],
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> int:
    if not diff_rows:
        return 0

    delete_diff_rows(conn, logical, snapshot_date, enterprise_id)
    enriched = []
    for r in diff_rows:
        row: Dict[str, Any] = {
            "snapshot_date": snapshot_date,
            "entity_id": r["entity_id"],
            "changed_metric_name": r["changed_metric_name"],
            "old_value": r.get("old_value"),
            "new_value": r.get("new_value"),
        }
        if enterprise_id is not None:
            row["enterprise_id"] = enterprise_id
        enriched.append(row)
    df = pd.DataFrame(enriched)
    col_order = ["snapshot_date"] + (["enterprise_id"] if enterprise_id is not None else []) + [
        "entity_id", "changed_metric_name", "old_value", "new_value",
    ]
    df = df[[c for c in col_order if c in df.columns]]
    return _write_pandas_batches(conn, df, _pandas_table_name(logical, diff=True))


def run_diff_for_logical(
    conn,
    logical: str,
    current_rows: List[Dict[str, Any]],
    snapshot_date: date,
    enterprise_id: Optional[str],
) -> int:
    prev_df = load_previous_daily(conn, logical, snapshot_date, enterprise_id)
    if prev_df is None or prev_df.empty:
        logger.info("mews diff: no previous snapshot for %s — skipping diff", logical)
        return 0
    diff_rows = compute_diff_rows(logical, current_rows, prev_df)
    logger.info(
        "[progress] Snowflake: %s_diff_daily — %s change row(s) to write",
        logical,
        len(diff_rows),
    )
    return write_diff_daily(conn, logical, diff_rows, snapshot_date, enterprise_id)
