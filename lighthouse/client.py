"""
Lighthouse API v3 client (https://api.mylighthouse.com/v3) — self-contained.

Endpoints used by the flight recorder:
  Hotels API                  GET /v3/hotels
  Lowest Rates API            GET /v3/rates
  Lowest Rates per Roomtype   GET /v3/roomtyperates
  Parity API                  GET /v3/parities

Rate limits (Lighthouse fair-usage policy):
  - max 20 requests per API per subscription over a 24-hour period
  - max 120 requests/minute across all APIs
"""

import requests

from config import LIGHTHOUSE_RATE_API_BASE_URL, LIGHTHOUSE_RATE_API_TOKEN

PER_PAGE = 100  # max allowed by the API
REQUEST_TIMEOUT = 120  # seconds


def log(step, msg):
    """Print a step label and message."""
    print(f"[{step}] {msg}")


def _request(path, params=None):
    """Perform an authenticated GET request against the Lighthouse API and return parsed JSON."""
    url = f"{LIGHTHOUSE_RATE_API_BASE_URL}/{path.lstrip('/')}"
    headers = {"X-Oi-Authorization": LIGHTHOUSE_RATE_API_TOKEN, "Accept-Encoding": "gzip"}
    resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
    if resp.status_code == 429:
        raise RuntimeError(f"Lighthouse API rate limited (429) for {url}: {resp.text[:300]}")
    if resp.status_code != 200:
        raise RuntimeError(f"Lighthouse API error {resp.status_code} for {url}: {resp.text[:500]}")
    return resp.json()


def _params(**kwargs):
    """Build a query params dict, dropping None values and joining lists with commas."""
    params = {}
    for k, v in kwargs.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            v = ",".join(str(x) for x in v)
        elif isinstance(v, bool):
            v = "true" if v else "false"
        params[k] = v
    return params


def fetch_hotels():
    """
    Fetch all client hotels (with competitors, compsets and subscribed features)
    from GET /v3/hotels, following pagination. Returns a list of hotel dicts.
    """
    hotels = []
    page = 1
    total_pages = None

    while True:
        log("hotels", f"Fetching page {page}" + (f"/{total_pages}" if total_pages else ""))
        data = _request("hotels", params={"page": page, "per_page": PER_PAGE})

        page_hotels = data.get("hotels", [])
        hotels.extend(page_hotels)

        meta = data.get("meta", {})
        total_pages = meta.get("total_pages", 1)
        total_results = meta.get("total_results", len(hotels))
        log("hotels", f"Page {page}/{total_pages}: got {len(page_hotels)} hotels ({len(hotels)}/{total_results} total)")

        if page >= total_pages:
            break
        page += 1

    return hotels


def fetch_rates(subscription_id, ota=None, los=None, bar=None, persons=None, meal_type=None,
                room_type=None, from_date=None, change_days=None, shop_length=None,
                compset_ids=None, currency=None):
    """
    Fetch lowest rates for the client hotel plus its compset from GET /v3/rates.
    Returns a list of rate dicts.

    subscription_id: required subscription ID (from the Hotels API).
    ota:             OTA id, e.g. 'bookingdotcom' (default), 'expedia', 'branddotcom'.
    los:             length of stay (1-45, default 1).
    bar:             True to return only Best Flex rates.
    persons:         minimum persons the rate accommodates (default 2).
    meal_type:       0-5 (0 = filter not applied).
    room_type:       e.g. 'standard', 'suite', ...
    from_date:       'YYYY-MM-DD' arrival date to start from (past or future).
    change_days:     list of day offsets for rate changes, e.g. [7, 14] (max 3 values).
    shop_length:     number of days to return (max 365).
    compset_ids:     list of compset IDs, e.g. [-1, 1] (default 1).
    currency:        3-letter currency code (default hotel currency).
    """
    params = _params(
        subscriptionId=subscription_id, ota=ota, los=los, bar=bar, persons=persons,
        mealType=meal_type, roomType=room_type, fromDate=from_date, changeDays=change_days,
        shopLength=shop_length, compsetIds=compset_ids, currency=currency,
    )
    log("rates", f"Fetching rates for subscription {subscription_id} (params: {params})")
    data = _request("rates", params=params)
    rates = data.get("rates", [])
    log("rates", f"Got {len(rates)} rates")
    return rates


def fetch_roomtype_rates(subscription_id, ota=None, los=None, bar=None, persons=None,
                         meal_type=None, from_date=None, shop_length=None,
                         compset_ids=None, currency=None):
    """
    Fetch lowest rates per roomtype for the client hotel plus its compset
    from GET /v3/roomtyperates. Returns a list of rate dicts. Params as fetch_rates().
    """
    params = _params(
        subscriptionId=subscription_id, ota=ota, los=los, bar=bar, persons=persons,
        mealType=meal_type, fromDate=from_date, shopLength=shop_length,
        compsetIds=compset_ids, currency=currency,
    )
    log("roomtyperates", f"Fetching roomtype rates for subscription {subscription_id}")
    data = _request("roomtyperates", params=params)
    rates = data.get("rates", [])
    log("roomtyperates", f"Got {len(rates)} rates")
    return rates


def fetch_parities(subscription_id, los=None, bar=None, persons=None, meal_type=None,
                   room_type=None, from_date=None, shop_length=None, currency=None):
    """
    Fetch the most recent parity rates per arrival date from GET /v3/parities.
    Returns a list of parity dicts (each containing a list of rates per OTA/channel).

    los:        length of stay (1-7, default 1).
    from_date:  'YYYY-MM-DD', must be >= the hotel's current local date.
    Other params as fetch_rates().
    """
    params = _params(
        subscriptionId=subscription_id, los=los, bar=bar, persons=persons, mealType=meal_type,
        roomType=room_type, fromDate=from_date, shopLength=shop_length, currency=currency,
    )
    log("parities", f"Fetching parities for subscription {subscription_id}")
    data = _request("parities", params=params)
    parities = data.get("parities", [])
    log("parities", f"Got {len(parities)} parity records")
    return parities
