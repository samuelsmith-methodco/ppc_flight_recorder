"""Pace venue ids (1–6) and Toast restaurant map. Ids match method-resy-pace `venue`."""

from __future__ import annotations

# include_rc isolates a concept inside a shared Toast restaurant (see resy-pace toast.mjs).
SEED_VENUES: list[dict] = [
    {
        "id": 1,
        "resy_venue_id": "738",
        "resy_slug": "pha/wm-mulherins-sons",
        "name": "Wm. Mulherin's Sons",
        "timezone": "America/New_York",
        "toast_restaurant_guid": "c70fe34e-3f99-4258-8e8a-428c863714ef",
        "toast_include_rc": None,
    },
    {
        "id": 2,
        "resy_venue_id": "73128",
        "resy_slug": "det/le-supreme",
        "name": "Le Suprême",
        "timezone": "America/Detroit",
        "toast_restaurant_guid": "99d1583c-6e31-43dc-89f8-8d2e19ac147b",
        "toast_include_rc": [
            "0694abd9-61d7-45cb-a691-5b0153ebf628",
            "28c78ef0-c32e-499e-8023-95f6a298e81a",
            "52cb42e0-4077-467b-994f-b95817b95d0a",
            "a58f0c1a-f473-419a-bd2a-bb57565b32f2",
            "d45c6b1a-ca2d-4188-ab59-02056f9828fd",
        ],
    },
    {
        "id": 3,
        "resy_venue_id": "78298",
        "resy_slug": "det/hiroki-san",
        "name": "HIROKI-SAN Detroit",
        "timezone": "America/Detroit",
        "toast_restaurant_guid": "1480f7c0-7b54-4c74-bec9-862f8e0efc0e",
        "toast_include_rc": [
            "3530d64b-7942-4789-b629-8334074aa6ed",
            "83cb7120-f9d4-4ebf-9696-b0f4a570b44d",
            "a452d1ce-b2e9-4c0b-a141-11df1b6c9425",
            "ab80114b-6361-40c4-8596-ca2a4b368745",
        ],
    },
    {
        "id": 4,
        "resy_venue_id": "74950",
        "resy_slug": "chs/lowland",
        "name": "Lowland",
        "timezone": "America/New_York",
        "toast_restaurant_guid": "8a5cb8ab-308d-482f-b498-c6bc2a16c96c",
        "toast_include_rc": None,
    },
    {
        "id": 5,
        "resy_venue_id": "74852",
        "resy_slug": "det/kampers",
        "name": "Kamper's",
        "timezone": "America/Detroit",
        "toast_restaurant_guid": "0541cfe4-f874-46a9-91ed-0380408a6f2a",
        "toast_include_rc": None,
    },
    {
        "id": 6,
        "resy_venue_id": "92072",
        "resy_slug": "chs/rosemary-rose",
        "name": "Rosemary & Rose",
        "timezone": "America/New_York",
        "toast_restaurant_guid": "757c51f9-ae4a-4dd2-9609-e231f21df72a",
        "toast_include_rc": None,
    },
]


def seed_by_id() -> dict[int, dict]:
    return {int(v["id"]): v for v in SEED_VENUES}
