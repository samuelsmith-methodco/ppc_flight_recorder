"""Row shaping for Resy reservations, ledger, guests, and covers."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
from typing import Any

CANCELLED_RE = re.compile(r"cancel", re.I)
NOSHOW_RE = re.compile(r"no.?show", re.I)
WALKIN_RE = re.compile(r"walk.?in", re.I)
DEFAULT_GUEST_SALT = "methodco-resy-pace-v1"


def _norm(s: Any) -> str:
    return "" if s is None else str(s).strip().lower()


def guest_key(email: Any, phone: Any, name: Any, salt: str = DEFAULT_GUEST_SALT) -> str | None:
    e, p, n = _norm(email), _norm(phone), _norm(name)
    basis = e or (f"p:{p}" if p else "") or (f"n:{n}" if n else "")
    if not basis:
        return None
    return hashlib.sha256(f"{salt}|{basis}".encode()).hexdigest()[:32]


def parse_date(s: Any) -> str | None:
    if not s:
        return None
    m = re.search(r"\d{4}-\d{2}-\d{2}", str(s))
    return m.group(0) if m else None


def parse_time(s: Any) -> str | None:
    if not s:
        return None
    text = str(s).strip()
    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*([ap]m)?$", text, re.I)
    if not m:
        return None
    h = int(m.group(1))
    minute = m.group(2)
    sec = m.group(3) or "00"
    ap = m.group(4).lower() if m.group(4) else None
    if ap:
        if ap == "pm" and h < 12:
            h += 12
        if ap == "am" and h == 12:
            h = 0
    if h > 23:
        return None
    return f"{h:02d}:{minute}:{sec}"


def _clip(s: Any, n: int = 4000) -> str | None:
    if s is None:
        return None
    text = str(s).replace("\x00", "").strip()
    if not text:
        return None
    return text[:n]


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def parse_reservation(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _int_or_none(raw.get("Reservation_id")),
        "ps": int(raw.get("Party_Size") or 0),
        "status": raw.get("status") or "",
        "svc": raw.get("service") or "",
        "time": raw.get("Time"),
        "table": (str(raw.get("table")).strip() if raw.get("table") not in (None, "") else None),
        "guest": raw.get("Guest"),
        "vip": bool(re.search(r"vip", str(raw.get("VIP_Tag") or ""), re.I)),
        "phone": raw.get("phone"),
        "email": raw.get("Email"),
        "totalVisits": _int_or_none(raw.get("Total_Visits")),
        "res": _int_or_none(raw.get("Res")),
        "wi": _int_or_none(raw.get("WI")),
        "cx": _int_or_none(raw.get("CX")),
        "ns": _int_or_none(raw.get("NS")),
        "lastVisit": raw.get("Last_Visit"),
        "guestTags": raw.get("Guest_Tags"),
        "visitTags": raw.get("Visit_Tags"),
        "allergyTags": raw.get("Allergy_Tags"),
        "specialReq": raw.get("Special_Requests"),
        "guestNotes": raw.get("Guest_Notes"),
        "bio": raw.get("bio"),
        "visitNote": raw.get("Visit_Note"),
    }


def pace_row(venue_id: int, target: date, snapshot_date: date, rows: list[dict[str, Any]], *, is_eod: bool, notes: str | None, now: datetime) -> dict[str, Any]:
    active = [r for r in rows if not CANCELLED_RE.search(str(r.get("status") or ""))]
    covers = sum(int(r.get("ps") or 0) for r in active)
    return {
        "venue_id": venue_id,
        "target_date": target.isoformat(),
        "snapshot_date": snapshot_date.isoformat(),
        "snapshot_ts": now.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
        "days_out": (target - snapshot_date).days,
        "covers_on_books": covers,
        "reservation_count": len(active),
        "party_size_avg": round(covers / len(active), 2) if active else None,
        "is_eod": is_eod,
        "is_realized": False,
        "source": "resy_reservations_report",
        "notes": notes,
    }


def realize_row(venue_id: int, target: date, rows: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    seated = walkin = noshow = cancelled = 0
    for r in rows:
        ps = int(r.get("ps") or 0)
        status = str(r.get("status") or "")
        if CANCELLED_RE.search(status):
            cancelled += ps
            continue
        if NOSHOW_RE.search(status):
            noshow += ps
        elif WALKIN_RE.search(status):
            walkin += ps
            seated += ps
        else:
            seated += ps
    return {
        "venue_id": venue_id,
        "target_date": target.isoformat(),
        "snapshot_date": target.isoformat(),
        "snapshot_ts": now.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" "),
        "days_out": 0,
        "covers_seated": seated,
        "walkin_covers": walkin,
        "no_show_covers": noshow,
        "cancelled_covers_cum": cancelled,
        "is_eod": True,
        "is_realized": True,
        "source": "resy_reservations_report",
    }


def collect_ledger(
    venue_id: int,
    target: date,
    rows: list[dict[str, Any]],
    *,
    now: datetime,
    salt: str,
    ledger: dict[str, dict[str, Any]],
    guests: dict[str, dict[str, Any]],
    seen: dict[str, set[int]],
    ok_dates: set[str],
    min_rows: int = 1,
) -> None:
    day = target.isoformat()
    if len(rows) < min_rows:
        return
    ok_dates.add(day)
    ids = seen.setdefault(day, set())
    now_s = now.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")
    for r in rows:
        rid = r.get("id")
        if not rid:
            continue
        ids.add(int(rid))
        gk = guest_key(r.get("email"), r.get("phone"), r.get("guest"), salt)
        ledger[f"{day}|{rid}"] = {
            "venue_id": venue_id,
            "reservation_id": int(rid),
            "target_date": day,
            "party_size": r.get("ps") or None,
            "service": _clip(r.get("svc"), 64),
            "res_time": parse_time(r.get("time")),
            "table_assignment": _clip(r.get("table"), 64),
            "visit_tags": _clip(r.get("visitTags")),
            "visit_note": _clip(r.get("visitNote")),
            "last_seen": now_s,
            "last_status": _clip(r.get("status"), 64),
            "cancelled": False,
            "cancelled_at": None,
            "vip": bool(r.get("vip")),
            "guest_total_visits": r.get("totalVisits"),
            "guest_key": gk,
        }
        if gk:
            guests[gk] = {
                "guest_key": gk,
                "name": _clip(r.get("guest"), 255),
                "email": _clip(r.get("email"), 255),
                "phone": _clip(r.get("phone"), 64),
                "vip": bool(r.get("vip")),
                "total_visits": r.get("totalVisits"),
                "lifetime_res": r.get("res"),
                "lifetime_walkins": r.get("wi"),
                "lifetime_cancels": r.get("cx"),
                "lifetime_noshows": r.get("ns"),
                "last_visit": parse_date(r.get("lastVisit")),
                "guest_tags": _clip(r.get("guestTags")),
                "visit_tags": _clip(r.get("visitTags")),
                "allergy_tags": _clip(r.get("allergyTags")),
                "special_requests": _clip(r.get("specialReq")),
                "guest_notes": _clip(r.get("guestNotes")),
                "bio": _clip(r.get("bio")),
                "last_seen": now_s,
            }


def finalize_ledger(
    ledger: dict[str, dict[str, Any]],
    prior: dict[tuple[str, int], dict[str, Any]],
    seen: dict[str, set[int]],
    ok_dates: set[str],
    venue_id: int,
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Attach first_seen, emit status events, and mark vanished reservations cancelled."""
    now_s = now.astimezone(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")
    events: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for row in ledger.values():
        key = (row["target_date"], int(row["reservation_id"]))
        prev = prior.get(key)
        first_seen = (prev or {}).get("first_seen") or now_s
        row = dict(row)
        row["first_seen"] = first_seen
        try:
            fs_day = str(first_seen)[:10]
            row["lead_days"] = (date.fromisoformat(row["target_date"]) - date.fromisoformat(fs_day)).days
            if row["lead_days"] < 0:
                row["lead_days"] = None
        except ValueError:
            row["lead_days"] = None
        was = (prev or {}).get("last_status")
        if row.get("last_status") and row["last_status"] != was:
            events.append(
                {
                    "venue_id": venue_id,
                    "reservation_id": row["reservation_id"],
                    "target_date": row["target_date"],
                    "status": row["last_status"],
                    "observed_at": now_s,
                    "prev_seen_at": (prev or {}).get("last_seen"),
                }
            )
        rows.append(row)

    for (target_date, rid), prev in prior.items():
        if target_date not in ok_dates:
            continue
        if prev.get("cancelled"):
            continue
        if rid in seen.get(target_date, set()):
            continue
        rows.append(
            {
                "venue_id": venue_id,
                "reservation_id": rid,
                "target_date": target_date,
                "party_size": prev.get("party_size"),
                "service": prev.get("service"),
                "res_time": prev.get("res_time"),
                "table_assignment": prev.get("table_assignment"),
                "visit_tags": prev.get("visit_tags"),
                "visit_note": prev.get("visit_note"),
                "last_seen": now_s,
                "last_status": prev.get("last_status"),
                "cancelled": True,
                "cancelled_at": now_s,
                "vip": prev.get("vip"),
                "guest_total_visits": prev.get("guest_total_visits"),
                "guest_key": prev.get("guest_key"),
                "first_seen": prev.get("first_seen") or now_s,
                "lead_days": prev.get("lead_days"),
            }
        )
    return rows, events


def covers_rows(venue_id: int, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for d in raw_rows:
        if not d.get("date"):
            continue
        ns_raw = str(d.get("no_show_covers_parties") or "0").split("/")[0].strip()
        try:
            ns = int(ns_raw or 0)
        except ValueError:
            ns = 0
        out.append(
            {
                "venue_id": venue_id,
                "service_date": str(d["date"])[:10],
                "service_type": int(d.get("service") or 0),
                "total_covers": int(d.get("total_covers") or 0),
                "reserved_covers": int(d.get("reserved_covers") or 0),
                "walkin_covers": int(d.get("walkin_covers") or 0),
                "waitlist_covers": int(d.get("waitlist_covers") or 0),
                "no_show_covers": ns,
                "source": "resy_covers_report",
            }
        )
    return out
