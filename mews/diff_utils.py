"""Diff equality helpers (same spirit as ppc_flight_recorder/sync.py)."""

from __future__ import annotations

import math
from typing import Any, Optional


def diff_value_empty(v: Any) -> bool:
    if v is None:
        return True
    try:
        if isinstance(v, float) and math.isnan(v):
            return True
    except Exception:
        pass
    try:
        f = float(v)
        if math.isnan(f):
            return True
    except (TypeError, ValueError):
        pass
    return False


def diff_values_equal(ov: Any, nv: Any) -> bool:
    if diff_value_empty(ov) and diff_value_empty(nv):
        return True
    if diff_value_empty(ov) or diff_value_empty(nv):
        return False
    try:
        o_f, n_f = float(ov), float(nv)
        if math.isnan(o_f) and math.isnan(n_f):
            return True
        if math.isnan(o_f) or math.isnan(n_f):
            return False
        return math.isclose(o_f, n_f, rel_tol=1e-9, abs_tol=1e-12) or o_f == n_f
    except (TypeError, ValueError):
        pass
    try:
        if ov is not None and nv is not None:
            os, ns = str(ov).strip(), str(nv).strip()
            if os == ns:
                return True
    except Exception:
        pass
    return ov == nv


def format_diff_value(v: Any, max_len: int = 65535) -> Optional[str]:
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    try:
        f = float(v)
        if math.isnan(f):
            return None
        if isinstance(v, bool):
            return str(v).lower()
        if isinstance(v, int) or (isinstance(v, float) and v == int(v)):
            s = str(int(f))
        else:
            s = str(v)
    except (TypeError, ValueError):
        s = str(v)
    return s[:max_len] if len(s) > max_len else s
