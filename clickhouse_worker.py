from dataclasses import dataclass
import os
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError
import requests
from json import dumps
import textwrap
import numbers
import pandas as pd
import logging



MAX_SAFE_JS_INT = 2**53 - 1


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class ClickHouseQueryError(Exception):
    """User-facing ClickHouse error with safe message."""
    pass

class ClickHouseConnectionError(RuntimeError):
    """Raised when the application cannot connect to ClickHouse."""


def _json_safe_cell(v):
    if isinstance(v, bool):
        return v
    # большие int → строкой, чтобы JS не терял точность
    # if isinstance(v, int) and (v > MAX_SAFE_JS_INT or v < -MAX_SAFE_JS_INT):
    if isinstance(v, numbers.Integral):
        iv = int(v)
        if iv > MAX_SAFE_JS_INT or iv < -MAX_SAFE_JS_INT:
            return str(iv)
        return iv
    return v


def _get_client():
    """
    Create client lazily (per request call).
    clickhouse_connect internally pools HTTP connections.
    """
    try:
        client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8443)),
            username=os.environ.get("CLICKHOUSE_USERNAME"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            secure=True,
            verify=False,
        )
        client.ping()
    except Exception as exc:
        logger.error(f"Failed to connect to ClickHouse: {exc}")
        raise ClickHouseConnectionError(f"Failed to connect to ClickHouse: {exc}") from exc

    return client


def _sanitize_sql(sql: str) -> str:
    sql = (sql or "").strip().strip(";")
    if not sql:
        raise ClickHouseQueryError("SQL is empty.")
    # (грубо: если есть ';' внутри — считаем несколькими statements)
    if ";" in sql:
        raise ClickHouseQueryError("Only one SQL statement is allowed (remove extra ';').")
    return textwrap.dedent(sql).strip()


def execute_sql(sql: str, *, max_rows: int = 2000) -> pd.DataFrame:
    """
    Execute SQL and return JSON-serializable structure:
    {
      "columns": ["col1", ...],
      "rows": [[...], ...],
      "row_count": int,
      "truncated": bool,
      "elapsed_ms": int | None
    }
    """
    sql = _sanitize_sql(sql)
    client = _get_client()
    result = client.query(sql)

    try:
        columns: list[str] = list(result.column_names or [])
        if result.first_row:
            for i, cell in enumerate(result.first_row):
                logger.info("cell info: col=%s value=%s type=%s", columns[i], cell, type(cell))
        else:
            print("Clickhouse returned no rows")
            logger.info("ClickHouse returned no rows")

        df: pd.DataFrame = pd.DataFrame(result.result_rows, columns=result.column_names)
        return df

    except ClickHouseError as e:
        raise ClickHouseQueryError(str(e)) from e
    except ValueError as e:
        raise ClickHouseQueryError(f"Invalid response: {e}") from e
    except Exception as e:
        raise ClickHouseQueryError(f"Unexpected error: {e}") from e

