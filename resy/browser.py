"""Headless Chromium fallback when the analytics token cannot call Resy reports."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_KEEP = ("authorization", "content-type", "accept")


def capture_report_headers(email: str, password: str, resy_slug: str) -> dict[str, str]:
    """Log into Resy OS and copy the headers the analytics page sends."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright is not installed. pip install playwright && python -m playwright install chromium"
        ) from exc

    captured: dict[str, str] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def on_request(req) -> None:
            if "analytics/report/core/" not in req.url or req.method != "POST":
                return
            picked: dict[str, str] = {}
            for key, value in req.headers.items():
                low = key.lower()
                if low in _KEEP or low.startswith("x-resy") or low.startswith("x-origin"):
                    picked[key] = value
            if picked:
                captured.clear()
                captured.update(picked)

        page.on("request", on_request)
        page.goto("https://os.resy.com/", wait_until="domcontentloaded", timeout=60000)
        email_box = page.locator('input[type="email"], input[name="email"]')
        password_box = page.locator('input[type="password"]')
        if email_box.count() and password_box.count():
            email_box.first.fill(email)
            password_box.first.fill(password)
            submit = page.locator('button[type="submit"]')
            if submit.count():
                submit.first.click()
            else:
                password_box.first.press("Enter")
            page.wait_for_timeout(4000)
        page.goto(
            f"https://os.resy.com/portal/{resy_slug}/analytics/Reservations",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        page.wait_for_timeout(8000)
        browser.close()

    if not captured:
        raise RuntimeError(f"could not capture Resy report headers for {resy_slug}")
    logger.info("Captured Resy browser headers for %s (%s keys)", resy_slug, len(captured))
    return captured
