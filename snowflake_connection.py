"""
PPC Flight Recorder – Snowflake connection (standalone).
Matches backend key-pair auth: AUTH_METHOD=KEYPAIR, SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH, insecure_mode.
"""

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import snowflake.connector

from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_AUTH_METHOD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_PRIVATE_KEY,
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
    SNOWFLAKE_PRIVATE_KEY_PATH,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)

logger = logging.getLogger(__name__)


def _get_connection_params() -> Dict[str, Any]:
    # Same base as backend; longer timeouts for slow proxy/VPN (insecure_mode avoids OCSP issues)
    params = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "login_timeout": 60,
        "network_timeout": 120,
        "insecure_mode": True,
    }
    if SNOWFLAKE_ROLE:
        params["role"] = SNOWFLAKE_ROLE

    if SNOWFLAKE_AUTH_METHOD == "KEYPAIR":
        from cryptography.hazmat.primitives import serialization

        if SNOWFLAKE_PRIVATE_KEY:
            private_key_str = SNOWFLAKE_PRIVATE_KEY.strip()
            if "\\n" in private_key_str:
                private_key_str = private_key_str.replace("\\n", "\n")
            if r"\n" in private_key_str and "\n" not in private_key_str:
                private_key_str = private_key_str.replace(r"\n", "\n")
            private_key_str = private_key_str.strip()
            if not private_key_str.startswith("-----BEGIN"):
                raise ValueError("SNOWFLAKE_PRIVATE_KEY must start with -----BEGIN PRIVATE KEY----- or similar")
            if "-----END" not in private_key_str:
                raise ValueError("SNOWFLAKE_PRIVATE_KEY must contain -----END ... -----")
            private_key_data = private_key_str.encode("utf-8")
        elif SNOWFLAKE_PRIVATE_KEY_PATH:
            key_path = Path(SNOWFLAKE_PRIVATE_KEY_PATH)
            with key_path.open("rb") as f:
                private_key_data = f.read()
        else:
            raise ValueError(
                "KEYPAIR auth requires SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH in .env"
            )

        passphrase = None
        if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
            passphrase = SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode("utf-8")

        p_key = serialization.load_pem_private_key(
            private_key_data,
            password=passphrase,
        )
        private_key_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        params["private_key"] = private_key_bytes
    else:
        params["password"] = SNOWFLAKE_PASSWORD

    return params


@contextmanager
def get_connection():
    """Yield a Snowflake connection. Commits on exit, rolls back on exception."""
    conn = None
    try:
        conn = snowflake.connector.connect(**_get_connection_params())
        yield conn
        conn.commit()
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def execute_query(conn: Any, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Run a SELECT and return a DataFrame."""
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cur.close()


def execute(conn: Any, query: str, params: Optional[Dict[str, Any]] = None) -> None:
    """Run a non-SELECT (INSERT/UPDATE/DELETE/MERGE)."""
    cur = conn.cursor()
    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
    finally:
        cur.close()


def execute_many(conn: Any, query: str, params_list: List[Dict[str, Any]]) -> None:
    """Run a query multiple times with different params."""
    cur = conn.cursor()
    try:
        cur.executemany(query, params_list)
    finally:
        cur.close()
