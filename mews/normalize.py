"""Flatten Mews JSON records to snake_case column names (match generated CSV / DDL)."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

import pandas as pd


def header_to_snake(h: str) -> str:
    h = (h or "").strip()
    if not h:
        return "col_empty"
    s = h.replace(".", "_").replace(" ", "_")
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "col_empty"
    if s[0].isdigit():
        s = "c_" + s
    return s


def _looks_like_rates_get_pricing(record: Dict[str, Any]) -> bool:
    """rates/getPricing: top-level arrays/objects — avoid json_normalize exploding nested arrays."""
    return "Currency" in record and (
        "TimeUnitStartsUtc" in record or "BaseAmountPrices" in record
    )


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """json_normalize with dot separator, then snake_case keys."""
    if not isinstance(record, dict):
        return {}
    if _looks_like_rates_get_pricing(record):
        return {header_to_snake(k): v for k, v in record.items()}
    rows = pd.json_normalize(record, sep=".").to_dict(orient="records")
    if not rows:
        return {}
    return {header_to_snake(k): v for k, v in rows[0].items()}


def enterprise_id_for_mews_record(
    record: Dict[str, Any],
    services_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Optional[str]:
    """Resolve Snowflake partition key from Mews JSON (multi-enterprise Connector responses)."""
    if not isinstance(record, dict):
        return None
    eid = record.get("EnterpriseId")
    if eid is not None and str(eid).strip():
        return str(eid)
    ent = record.get("Enterprise")
    if isinstance(ent, dict) and ent.get("Id"):
        return str(ent["Id"])
    if services_by_id:
        sid = record.get("ServiceId")
        if sid is not None:
            svc = services_by_id.get(str(sid))
            if isinstance(svc, dict) and svc.get("EnterpriseId"):
                return str(svc["EnterpriseId"])
    return None


def entity_id_for(record: Dict[str, Any], singleton: bool = False) -> str:
    if singleton:
        return "singleton"
    rid = record.get("Id") or record.get("id")
    if rid is not None and str(rid).strip():
        return str(rid)
    ident = record.get("Identifier")
    if ident is not None and str(ident).strip():
        return str(ident)
    return "unknown"


def rows_for_snapshot(
    records: list,
    schema_cols: list,
    snapshot_date,
    enterprise_id: Optional[str],
    singleton: bool = False,
    enterprise_id_from_record: bool = False,
    services_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
    fallback_enterprise_id: Optional[str] = None,
) -> list:
    """Build list of dicts aligned to Snowflake daily table columns."""
    out = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        flat = flatten_record(rec)
        eid = entity_id_for(rec, singleton=singleton)
        row: Dict[str, Any] = {
            "snapshot_date": snapshot_date,
            "entity_id": eid,
            "record_json": rec,
        }
        eid_row: Optional[str] = enterprise_id
        if enterprise_id_from_record:
            rid = enterprise_id_for_mews_record(rec, services_by_id)
            eid_row = rid if rid is not None else fallback_enterprise_id
        if eid_row is not None:
            row["enterprise_id"] = eid_row
        for c in schema_cols:
            row[c] = flat.get(c)
        out.append(row)
    return out


def singleton_row(
    record: Dict[str, Any],
    schema_cols: list,
    snapshot_date,
    enterprise_id: Optional[str],
    enterprise_id_from_record: bool = False,
    services_by_id: Optional[Dict[str, Dict[str, Any]]] = None,
    fallback_enterprise_id: Optional[str] = None,
) -> list:
    """Single configuration-style object."""
    flat = flatten_record(record) if record else {}
    eid_row: Optional[str] = enterprise_id
    if enterprise_id_from_record:
        rid = enterprise_id_for_mews_record(record, services_by_id)
        eid_row = rid if rid is not None else fallback_enterprise_id
    row: Dict[str, Any] = {
        "snapshot_date": snapshot_date,
        "entity_id": "singleton",
        "record_json": record,
    }
    if eid_row is not None:
        row["enterprise_id"] = eid_row
    for c in schema_cols:
        row[c] = flat.get(c)
    return [row]
