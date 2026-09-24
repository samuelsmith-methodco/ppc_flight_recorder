"""Unit checks for Lighthouse GET /v3/demand flattening (no API / Snowflake)."""

from lighthouse.recorder import flatten_demand


def test_flatten_demand_reads_common_keys():
    row = flatten_demand(
        {
            "arrival_date": "2026-09-12",
            "demand": 0.62,
            "market_otb_index": 1.04,
            "extractDateTime": "2026-08-20T12:00:00Z",
        },
        "2026-08-20",
        164110,
        233265,
        "ROOST Detroit",
    )
    assert row is not None
    assert row["arrival_date"] == "2026-09-12"
    assert row["demand_value"] == 0.62
    assert row["market_otb_index"] == 1.04
    assert row["hotel_id"] == 233265
    assert "property_id" not in row


def test_flatten_demand_skips_missing_arrival():
    assert flatten_demand({}, "2026-08-20", 1, 2, "x") is None


def test_flatten_demand_skips_bad_arrival():
    assert flatten_demand({"arrivalDate": "not-a-date"}, "2026-08-20", 1, 2, "x") is None


if __name__ == "__main__":
    test_flatten_demand_reads_common_keys()
    test_flatten_demand_skips_missing_arrival()
    test_flatten_demand_skips_bad_arrival()
    print("ok")
