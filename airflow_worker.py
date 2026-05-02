import os
import requests
from datetime import datetime, timezone

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
USERNAME = os.environ.get("AIRFLOW_USERNAME", "admin")
PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")

DAG_ID = "dbt_financial_report_layer"


def get_token():
    url = f"{AIRFLOW_URL}/auth/token"

    response = requests.post(
        url,
        json={
            "username": USERNAME,
            "password": PASSWORD
        },
        timeout=30
    )
    response.raise_for_status()
    return response.json()["access_token"]

def get_dag_runs(token, state=None, limit=10):
    url = f"{AIRFLOW_URL}/api/v2/dags/{DAG_ID}/dagRuns"

    params = {
        "limit": limit,
        "order_by": "-start_date"
    }
    if state:
        params["state"] = state

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()["dag_runs"]


def get_status(token):
    running = get_dag_runs(token, state="running", limit=1)
    if running:
        run = running[0]
        return {
            "is_running": True,
            "run_id": run["dag_run_id"],
            "start_date": run["start_date"]
        }

    success = get_dag_runs(token, state="success", limit=1)
    return {
        "is_running": False,
        "last_success": success[0] if success else None
    }


def trigger_dag(token):
    url = f"{AIRFLOW_URL}/api/v2/dags/{DAG_ID}/dagRuns"

    payload = {
        "dag_run_id": f"manual__{datetime.now(timezone.utc).isoformat()}",
        "conf": {}
    }

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json=payload,
        timeout=30
    )
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    token = get_token()

    status = get_status(token)
    print(status)

    if not status["is_running"]:
        pass
        # result = trigger_dag(token)
        # print("Triggered:", result["dag_run_id"])
    else:
        print("DAG is already running")