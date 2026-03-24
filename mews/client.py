"""Mews Connector API HTTP client with Cursor pagination."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests

from config import (
    MEWS_ACCESS_TOKEN,
    MEWS_BASE_URL,
    MEWS_CLIENT_NAME,
    MEWS_CLIENT_TOKEN,
    MEWS_PAGE_SIZE,
    MEWS_REQUEST_CONNECT_TIMEOUT_SEC,
    MEWS_REQUEST_RETRIES,
    MEWS_REQUEST_TIMEOUT_SEC,
    MEWS_RETRY_BACKOFF_MAX_SEC,
    MEWS_RETRY_BACKOFF_SEC,
)

logger = logging.getLogger(__name__)

# Log width matches sync_mews visual separators.
_LOG_VIS_W = 78


def _vis_line(char: str) -> None:
    logger.info(char * _LOG_VIS_W)


class MewsApiPermissionError(RuntimeError):
    """HTTP 401/403 — token not allowed for this operation (skip and continue sync)."""


def merge_rows_by_id(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate Mews entities by Id (same row can appear at slice boundaries)."""
    by_id: Dict[str, Dict[str, Any]] = {}
    no_id: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        rid = r.get("Id")
        if rid is not None:
            by_id[str(rid)] = r
        else:
            no_id.append(r)
    return list(by_id.values()) + no_id


class MewsClient:
    def __init__(self) -> None:
        self.base_url = MEWS_BASE_URL.rstrip("/")
        self.timeout = (MEWS_REQUEST_CONNECT_TIMEOUT_SEC, MEWS_REQUEST_TIMEOUT_SEC)

    def base_payload(self) -> Dict[str, Any]:
        return {
            "ClientToken": MEWS_CLIENT_TOKEN,
            "AccessToken": MEWS_ACCESS_TOKEN,
            "Client": MEWS_CLIENT_NAME,
        }

    def _sleep_backoff(self, attempt: int) -> None:
        base = MEWS_RETRY_BACKOFF_SEC * (2**attempt)
        cap = min(base, MEWS_RETRY_BACKOFF_MAX_SEC)
        jitter = random.uniform(0, min(1.0, cap * 0.1))
        time.sleep(cap + jitter)

    def _post_once(self, url: str, body: Dict[str, Any]) -> requests.Response:
        return requests.post(url, json=body, timeout=self.timeout)

    def post(self, endpoint: str, body: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(MEWS_REQUEST_RETRIES + 1):
            try:
                r = self._post_once(url, body)
                if r.status_code in (429, 502, 503, 504) and attempt < MEWS_REQUEST_RETRIES:
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if ra and ra.isdigit() else None
                    logger.warning(
                        "Mews %s HTTP %s (attempt %s/%s)%s",
                        endpoint,
                        r.status_code,
                        attempt + 1,
                        MEWS_REQUEST_RETRIES + 1,
                        f" — retry in {wait}s" if wait is not None else "",
                    )
                    if wait is not None:
                        time.sleep(min(wait, MEWS_RETRY_BACKOFF_MAX_SEC))
                    else:
                        self._sleep_backoff(attempt)
                    continue
                if r.status_code in (401, 403):
                    raise MewsApiPermissionError(
                        f"Mews {endpoint} HTTP {r.status_code}: {(r.text or '')[:500]}"
                    )
                if r.status_code != 200:
                    raise RuntimeError(
                        f"Mews {endpoint} HTTP {r.status_code}: {(r.text or '')[:500]}"
                    )
                return r.json() or {}
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                if attempt >= MEWS_REQUEST_RETRIES:
                    raise
                logger.warning(
                    "Mews %s %s (attempt %s/%s) — retrying",
                    endpoint,
                    type(e).__name__,
                    attempt + 1,
                    MEWS_REQUEST_RETRIES + 1,
                )
                self._sleep_backoff(attempt)
        raise RuntimeError("Mews post: internal retry loop exited unexpectedly")

    def post_traced(self, endpoint: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """Single POST with visual + / - and [progress] lines (e.g. configuration/get)."""
        _vis_line("+")
        logger.info("[progress] Mews API  %s  POST (single request)", endpoint)
        try:
            out = self.post(endpoint, body)
        except Exception:
            logger.exception("[progress] Mews API  %s  request failed", endpoint)
            _vis_line("-")
            raise
        logger.info("[progress] Mews API  %s  OK", endpoint)
        _vis_line("-")
        return out

    def post_optional(self, endpoint: str, body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Return None on non-200 (caller logs skip). Retries same as post for transient errors."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(MEWS_REQUEST_RETRIES + 1):
            try:
                r = self._post_once(url, body)
                if r.status_code in (429, 502, 503, 504) and attempt < MEWS_REQUEST_RETRIES:
                    self._sleep_backoff(attempt)
                    continue
                if r.status_code != 200:
                    logger.warning(
                        "Mews %s HTTP %s: %s", endpoint, r.status_code, (r.text or "")[:300]
                    )
                    return None
                return r.json() or {}
            except (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as e:
                if attempt >= MEWS_REQUEST_RETRIES:
                    logger.warning("Mews %s failed after retries: %s", endpoint, e)
                    return None
                logger.warning(
                    "Mews %s %s (attempt %s/%s) — retrying",
                    endpoint,
                    type(e).__name__,
                    attempt + 1,
                    MEWS_REQUEST_RETRIES + 1,
                )
                self._sleep_backoff(attempt)
        return None

    def post_optional_traced(self, endpoint: str, body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """post_optional with the same [progress] + visual framing as post_traced."""
        _vis_line("+")
        logger.info("[progress] Mews API  %s  POST optional", endpoint)
        out = self.post_optional(endpoint, body)
        if out is not None:
            logger.info("[progress] Mews API  %s  OK (body returned)", endpoint)
        else:
            logger.info("[progress] Mews API  %s  no data / HTTP not 200", endpoint)
        _vis_line("-")
        return out

    def fetch_all(
        self,
        endpoint: str,
        list_key: str,
        body: Dict[str, Any],
        sleep_sec: float = 0.05,
        on_page: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> List[Dict[str, Any]]:
        """Paginate using Limitation.Cursor until exhausted.

        If ``on_page`` is set, each page is passed to the callback and **not** accumulated in
        memory (returns ``[]``). Use for very large lists (e.g. orderitems) to flush to storage per page.
        """
        out: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        base_body = {k: v for k, v in body.items() if k != "Limitation"}
        page = 0
        total_streamed = 0
        _vis_line("+")
        logger.info(
            "[progress] Mews API  %s  paginated fetch  list_key=%s  page_size=%s%s",
            endpoint,
            list_key,
            MEWS_PAGE_SIZE,
            "  (streaming on_page — no in-memory list)" if on_page else "",
        )
        try:
            while True:
                page += 1
                lim: Dict[str, Any] = {"Count": MEWS_PAGE_SIZE}
                if cursor:
                    lim["Cursor"] = cursor
                payload = {**base_body, "Limitation": lim}
                try:
                    data = self.post(endpoint, payload)
                except MewsApiPermissionError as e:
                    n_so_far = total_streamed if on_page else len(out)
                    if n_so_far == 0:
                        logger.warning(
                            "[progress] Mews API  %s  skipped (no permission): %s",
                            endpoint,
                            (str(e)[:300]),
                        )
                    else:
                        logger.warning(
                            "[progress] Mews API  %s  permission denied mid-pagination — keeping %s rows: %s",
                            endpoint,
                            n_so_far,
                            (str(e)[:200]),
                        )
                    break
                chunk = data.get(list_key)
                added = 0
                if isinstance(chunk, list):
                    dicts = [x for x in chunk if isinstance(x, dict)]
                    added = len(dicts)
                    if on_page:
                        on_page(dicts)
                        total_streamed += added
                    else:
                        out.extend(dicts)
                logger.info(
                    "[progress] Mews API  %s  page %s  +%s rows  (total %s)%s",
                    endpoint,
                    page,
                    added,
                    total_streamed if on_page else len(out),
                    "  ... more pages" if data.get("Cursor") else "  last page",
                )
                next_cur = data.get("Cursor")
                if not next_cur:
                    break
                cursor = next_cur
                if sleep_sec:
                    time.sleep(sleep_sec)
            logger.info(
                "[progress] Mews API  %s  completed  %s row(s)",
                endpoint,
                total_streamed if on_page else len(out),
            )
        except Exception:
            logger.exception(
                "[progress] Mews API  %s  paginated fetch failed (partial rows=%s)",
                endpoint,
                total_streamed if on_page else len(out),
            )
            raise
        finally:
            _vis_line("-")
        return [] if on_page else out

    def fetch_all_time_slices(
        self,
        endpoint: str,
        list_key: str,
        body_template: Dict[str, Any],
        time_key: str,
        slices: List[Dict[str, str]],
        on_page: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
    ) -> List[Dict[str, Any]]:
        """Same as fetch_all but run one paginated fetch per UTC slice; merge + dedupe by Id.

        When ``on_page`` is set, each page is passed to the callback and nothing is merged in
        memory (returns ``[]``). Use for huge endpoints + Snowflake append-after-each-page.
        """
        if not slices:
            return self.fetch_all(endpoint, list_key, body_template, on_page=on_page)

        if len(slices) == 1:
            sl = slices[0]
            _vis_line("=")
            logger.info(
                "[progress] Mews API  %s  single %s window  %s → %s  (paginating; large responses can take several minutes)",
                endpoint,
                time_key,
                sl.get("StartUtc"),
                sl.get("EndUtc"),
            )
            _vis_line("=")
            body = {**body_template, time_key: sl}
            merged_one = self.fetch_all(endpoint, list_key, body, on_page=on_page)
            logger.info(
                "[progress] Mews API  %s  single-window fetch done  %s",
                endpoint,
                f"{len(merged_one)} row(s)" if not on_page else "streamed (on_page)",
            )
            return merged_one

        _vis_line("=")
        logger.info(
            "[progress] Mews API  %s  %s %s slice(s)  (%s)",
            endpoint,
            len(slices),
            time_key,
            "streaming on_page — no merged list" if on_page else "merge + dedupe by Id after all slices",
        )
        _vis_line("=")
        merged: List[Dict[str, Any]] = []
        for i, sl in enumerate(slices):
            _vis_line("~")
            logger.info(
                "[progress] Mews API  %s  slice %s/%s  %s  %s → %s",
                endpoint,
                i + 1,
                len(slices),
                time_key,
                sl.get("StartUtc"),
                sl.get("EndUtc"),
            )
            _vis_line("~")
            body = {**body_template, time_key: sl}
            part = self.fetch_all(endpoint, list_key, body, on_page=on_page)
            logger.info(
                "[progress] Mews API  %s  slice %s/%s  fetched %s row(s)  (pre-merge running total %s)",
                endpoint,
                i + 1,
                len(slices),
                len(part),
                len(merged) + len(part),
            )
            if not on_page:
                merged.extend(part)
        if on_page:
            _vis_line("=")
            logger.info(
                "[progress] Mews API  %s  all slices streamed (no in-memory merge)",
                endpoint,
            )
            _vis_line("=")
            return []
        merged = merge_rows_by_id(merged)
        _vis_line("=")
        logger.info(
            "[progress] Mews API  %s  all slices merged/deduped  %s row(s)",
            endpoint,
            len(merged),
        )
        _vis_line("=")
        return merged
