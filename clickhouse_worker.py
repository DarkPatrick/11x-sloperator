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
import ast
import pandas as pd
import numpy as np
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


def drop_exp_partitions(exp_id: int, client_name: str, segment: str, cluster: str = "ug_core"):
    database = "sandbox"
    table = "ug_monetization_sloperator_ug_exp_results"

    partitions_sql = f"""
    SELECT DISTINCT
        partition
    FROM clusterAllReplicas('{cluster}', system.parts)
    WHERE database = '{database}'
      AND table = '{table}'
      AND active
      AND partition LIKE '%, {exp_id}, {client_name}, {segment})'
    ORDER BY partition
    """

    client = _get_client()
    try:
        partitions = client.query(partitions_sql).result_rows

        if not partitions:
            print(f"No active partitions found for exp_id={exp_id} and client={client_name}, and segment={segment}, skipping drop")
            return

        for (partition,) in partitions:
            # partition: '(202603,7178)'
            year_month, partition_exp_id = (
                partition
                .strip("()")
                .split(",")
            )

            drop_sql = f"""
            ALTER TABLE {database}.{table}
            ON CLUSTER {cluster}
            DROP PARTITION ({year_month}, {partition_exp_id}, {client_name}, {segment})
            """

            print(f"Drop partition: ({year_month}, {partition_exp_id}, {client_name}, {segment})")
            
            client.command(drop_sql)
    except ClickHouseError as e:
        raise ClickHouseQueryError(str(e)) from e
    except ValueError as e:
        raise ClickHouseQueryError(f"Invalid response: {e}") from e
    except Exception as e:
        raise ClickHouseQueryError(f"Unexpected error: {e}") from e
    finally:
        client.close()


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


def prepare_df_for_clickhouse(df):
    df = df.copy()

    string_columns = [
        'dt',
        'metric',
        'variation_pair',
        'numerator',
        'denominator',
        'variance',
        'distribution',
        'percentage',
        'client',
    ]

    int_columns = [
        'control_variation',
        'test_variation',
        'exp_id',
    ]

    float_columns = [
        'mean_0',
        'mean_1',
        'mean_diff',
        'ci_low',
        'ci_high',
        'pvalue',
        'lift',
    ]

    for col in string_columns:
        df[col] = df[col].replace({np.nan: ''}).fillna('').astype(str)

    for col in int_columns:
        df[col] = df[col].replace({np.nan: 0}).fillna(0).astype('int64')

    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')

    return df


def insert_dataframe(table_name: str, df: pd.DataFrame) -> None:
    client = _get_client()
    try:
        for col in ['dt', 'metric', 'variation_pair', 'numerator', 'denominator', 'variance', 'distribution', 'percentage', 'client']:
            bad = df[df[col].map(lambda x: not isinstance(x, str) and pd.notna(x))]
            print(col, len(bad), bad[col].head().tolist())
        client.insert_df(table_name, df)
    except ClickHouseError as e:
        raise ClickHouseQueryError(str(e)) from e
    except ValueError as e:
        raise ClickHouseQueryError(f"Invalid response: {e}") from e
    except Exception as e:
        raise ClickHouseQueryError(f"Unexpected error: {e}") from e
    finally:
        client.close()


def insert_df_by_chunks(table_name: str, df: pd.DataFrame, chunk_size=1000):
    df = prepare_df_for_clickhouse(df)
    total = len(df)

    for start in range(0, total, chunk_size):
        end = min(start + chunk_size, total)
        chunk = df.iloc[start:end].copy()

        print(f'Insert rows {start} - {end} / {total}')

        insert_dataframe(table_name, chunk)


def parse_configuration_project(row) -> str:
    text = str(row)

    # --- 1. PROJECT ---
    project = ''

    # ищем после project:
    match_project = re.search(r'project:\s*"?([^",\s]+)"?', text)
    if match_project:
        project = match_project.group(1)
    else:
        # fallback — ищем любую ссылку
        match_url = re.search(r'https?://[^\s,"]+', text)
        if match_url:
            project = match_url.group(0)

    # убираем якорь (#...)
    if project:
        project = project.split('#')[0]

    return project


def parse_configuration_segments(row) -> dict:
    DEFAULT_SEGMENTS = {'Total': {'pro_rights': 'All'}}
    text = str(row)
    if not text or 'segments:' not in text:
        return DEFAULT_SEGMENTS

    start_match = re.search(r'segments:\s*', text)
    if not start_match:
        return DEFAULT_SEGMENTS

    start = start_match.end()

    # ищем первую открывающую скобку словаря
    first_brace = text.find('{', start)
    if first_brace == -1:
        return DEFAULT_SEGMENTS

    depth = 0

    for i in range(first_brace, len(text)):
        if text[i] == '{':
            depth += 1
        elif text[i] == '}':
            depth -= 1

            if depth == 0:
                dict_str = text[first_brace:i + 1]

                try:
                    return ast.literal_eval(dict_str)
                except Exception:
                    return DEFAULT_SEGMENTS

    return DEFAULT_SEGMENTS


def get_ugm_exps_list() -> list[int]:
    query = get_query("get_ug_monetization_exps_ids_to_calc")
    df = execute_sql(query)
    return df['id'].tolist()


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
        project = parse_configuration_project(exp_info["configuration"])
        segments = parse_configuration_segments(exp_info["configuration"])
        exp_info["project"] = project
        exp_info["segments"] = segments
        
        logger.info("exp_info: %s", exp_info)
        return exp_info


def create_experiment_users_table(exp_info: dict, client: str) -> str:
    session_id = generate_random_id(32)
    exp_id = exp_info["id"]
    exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
    table_name = f'exp_users_{exp_id}_{session_id}'
    
    query_part_1 = get_query(
        "create_table_template", 
        params=dict({
            "table_name": table_name, 
            "schema": "",
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
                "date_filter": exp_start_dt.strftime("%Y-%m-%d"),
                "client": client
            }
        )
    else:
        query_part_2 = get_query(
            "exp_raw_data_app", 
            params={
                "exp_id": exp_id, 
                "where_sql": where_filter, 
                "having_sql": 1,
                "date_filter": exp_start_dt.strftime("%Y-%m-%d"),
                "client": client
            }
        )
    query = query_part_1 + "\n as \n" + query_part_2
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
                "exp_raw_data_web_insert", 
                params={
                    "exp_id": exp_id, 
                    "where_sql": where_filter, 
                    "having_sql": 1,
                    "date_filter": current_day.strftime("%Y-%m-%d"),
                    "exp_users_table": f"sandbox.ug_monetization_sloperator_{table_name}",
                    "client": client
                }
            )
        else:
            query_part_2 = get_query(
                "exp_raw_data_app_insert", 
                params={
                    "exp_id": exp_id, 
                    "where_sql": where_filter, 
                    "having_sql": 1,
                    "date_filter": current_day.strftime("%Y-%m-%d"),
                    "exp_users_table": f"sandbox.ug_monetization_sloperator_{table_name}",
                    "client": client
                }
            )
        query = query_part_1 + "\n" + query_part_2
        logger.info("inserting experiment users table with query:\n%s", query)
        execute_sql_modify(query)

    return f"sandbox.ug_monetization_sloperator_{table_name}"

def create_experiments_subscription_table(exp_info: dict) -> str:
    session_id = generate_random_id(32)
    table_name = f'exp_subscription_{exp_info["id"]}_{session_id}'
    query_part_1 = get_query("create_table_template", params=dict({"table_name": table_name, "schema": "", "partition": "toYYYYMM(toDate(subscribed_dt))", "sorting": "subscribed_dt"}))
    query_part_2 = get_query("subscriptions_by_sub_date", params={"date_start": exp_info["date_start"], "date_end": exp_info["date_end"]})
    query = query_part_1 + "\n as \n" + query_part_2
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


def pandas_to_clickhouse_types(df) -> str:
    mapping = {
        'int64': 'Int64',
        'float64': 'Float64',
        'object': 'String',
        'datetime64[ns]': 'DateTime'
    }
    cols = []
    for col, dtype in df.dtypes.items():
        ch_type = mapping.get(str(dtype), 'String')
        cols.append(f"`{col}` {ch_type}")
    return ",\n".join(cols)


def create_exp_results_table(df: pd.DataFrame) -> None:
    schema = pandas_to_clickhouse_types(df)
    query = get_query("create_table_template", params=dict({"table_name": "ug_exp_results", "schema": f"({schema})", "partition": "toYYYYMM(toDate(dt)), exp_id, client, segment", "sorting": "dt"}))
    logger.info("Creating experiment results table with query:\n%s", query)
    execute_sql_modify(query)
    # insert_dataframe("ug_monetization_sloperator_ug_exp_results", df)
    insert_df_by_chunks("sandbox.ug_monetization_sloperator_ug_exp_results", df)


def update_exp_results_table(df: pd.DataFrame) -> None:
    # insert_dataframe(df, "ug_monetization_sloperator_ug_exp_results")
    insert_df_by_chunks("sandbox.ug_monetization_sloperator_ug_exp_results", df)
