from dataclasses import dataclass
import os
import re
import string
from typing import Any
import random
import requests
from json import dumps
import textwrap
import numbers
import pandas as pd
import datetime
import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError
import logging



MAX_SAFE_JS_INT = 2**53 - 1


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



class ClickHouseQueryError(Exception):
    """User-facing ClickHouse error with safe message."""
    pass

class ClickHouseConnectionError(RuntimeError):
    """Raised when the application cannot connect to ClickHouse."""


def generate_random_id(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
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


def get_query(query_name: str, params: dict = {}) -> str:
        sql_req: str = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params) if bool(params) else sql_req

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


def execute_sql_modify(sql: str) -> None:
    """
    Execute SQL that modifies data (INSERT/CREATE/DROP etc.) without returning results.
    """
    sql = _sanitize_sql(sql)
    client = _get_client()
    try:
        client.command(sql)
    except ClickHouseError as e:
        raise ClickHouseQueryError(str(e)) from e
    except ValueError as e:
        raise ClickHouseQueryError(f"Invalid response: {e}") from e
    except Exception as e:
        raise ClickHouseQueryError(f"Unexpected error: {e}") from e
    finally:
        client.close()

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
            logger.info("ClickHouse returned no rows")

        df: pd.DataFrame = pd.DataFrame(result.result_rows, columns=result.column_names)
        return df

    except ClickHouseError as e:
        raise ClickHouseQueryError(str(e)) from e
    except ValueError as e:
        raise ClickHouseQueryError(f"Invalid response: {e}") from e
    except Exception as e:
        raise ClickHouseQueryError(f"Unexpected error: {e}") from e
    finally:
        client.close()


def get_experiment(id) -> dict:
        query = get_query("get_ug_exp_info", params=dict({"id": id}))
        query_result = execute_sql(query)
        df = query_result
        clients_pattern = r'(\w+)'
        df["clients_list"] = df.clients.apply(lambda x: re.findall(clients_pattern, x))
        exp_info: dict = {
            "id": df.id[0],
            "date_start": df.date_start[0],
            "date_end": df.date_end[0],
            "variations": df.variations[0],
            "experiment_event_start": df.experiment_event_start[0],
            "configuration": df.configuration[0],
            'clients_list': df.clients_list[0],
            'clients_options': df.clients_options[0]
        }
        logger.info("exp_info: %s", exp_info)
        return exp_info


def create_experiment_users_table(exp_info: dict) -> str:
    session_id = generate_random_id(32)
    exp_id = exp_info["id"]
    exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
    table_name = f'exp_users_{exp_id}_{session_id}'
    
    query_part_1 = get_query(
        "create_table_template", 
        params=dict({
            "table_name": table_name, 
            "partition": "toYYYYMM(toDate(exp_start_dt))", 
            "sorting": "exp_start_dt"
        })
    )
    where_filter: str = "1"
    if exp_info["experiment_event_start"] == "App Experiment Start":
        where_filter = f"event = 'App Experiment Start' and item_id = {exp_id}"
    elif exp_info["experiment_event_start"] != "":
        where_filter = f"event = '{exp_info['experiment_event_start']}'"
    if "UG_WEB" in exp_info["clients_list"]:
        query_part_2 = get_query(
            "exp_raw_data_web", 
            params={
                "exp_id": exp_id, 
                "where_sql": where_filter, 
                "having_sql": 1,
                "date_filter": exp_start_dt.strftime("%Y-%m-%d")
            }
        )
    query = query_part_1 + "\n" + query_part_2
    # log query for debugging
    logger.info("Creating experiment users table with query:\n%s", query)
    execute_sql_modify(query)
    
    exp_end_dt = datetime.datetime.now(datetime.timezone.utc)
    if exp_info["date_end"] > exp_info["date_start"]:
        exp_end_dt = datetime.datetime.fromtimestamp(exp_info["date_end"], datetime.timezone.utc)
    days_cnt = (exp_end_dt.date() - exp_start_dt.date()).days
    for day in range(days_cnt):
        logger.info("calculating query for day %s", day)
        current_day = exp_start_dt + datetime.timedelta(days=day+1)
        query_part_1 = f"insert into sandbox.ug_monetization_sloperator_{table_name}"
        if "UG_WEB" in exp_info["clients_list"]:
            query_part_2 = get_query(
            "exp_raw_data_web", 
            params={
                "exp_id": exp_id, 
                "where_sql": where_filter, 
                "having_sql": 1,
                "date_filter": current_day.strftime("%Y-%m-%d")
            }
        )
        query = query_part_1 + "\n" + query_part_2
        logger.info("inserting experiment users table with query:\n%s", query)
        execute_sql_modify(query)

    return f"sandbox.ug_monetization_sloperator_{table_name}"

def create_experiments_subscription_table(exp_info: dict) -> str:
    session_id = generate_random_id(32)
    table_name = f'exp_subscription_{exp_info["id"]}_{session_id}'
    query_part_1 = get_query("create_table_template", params=dict({"table_name": table_name, "partition": "toYYYYMM(toDate(subscribed_dt))", "sorting": "subscribed_dt"}))
    query_part_2 = get_query("subscriptions_by_sub_date", params={"date_start": exp_info["date_start"], "date_end": exp_info["date_end"]})
    query = query_part_1 + "\n" + query_part_2
    execute_sql_modify(query)

    return f"sandbox.ug_monetization_sloperator_{table_name}"

def drop_table(table_name: str) -> None:
    query = f"drop table if exists {table_name} on cluster ug_core"
    execute_sql_modify(query)
    return None

def get_monetization_metrics(exp_info: dict, exp_users_table: str, subscription_table: str) -> pd.DataFrame:
    query = get_query("monetization_metrics", params={"exp_users_table": exp_users_table, "subscription_table": subscription_table})
    logger.info("total query:\n%s", query)
    df = execute_sql(query)
    return df
