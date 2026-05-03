#!/usr/bin/env bash
set -euo pipefail

VPN_STATE_PATH="${VPN_STATE_PATH:-/tmp/vpn_state.json}"
HEARTBEAT_START_CHECK_INTERVAL_SEC="${HEARTBEAT_START_CHECK_INTERVAL_SEC:-10}"

is_vpn_ok() {
  python -c '
import json
import os
import sys

path = os.environ.get("VPN_STATE_PATH", "/tmp/vpn_state.json")
try:
    with open(path, "r", encoding="utf-8") as f:
        state = json.load(f)
except Exception:
    sys.exit(1)

sys.exit(0 if state.get("connected") is True else 1)
'
}

is_clickhouse_ok() {
  python -c '
from clickhouse_worker import _get_client

client = _get_client()
try:
    client.command("SELECT 42")
finally:
    client.close()
'
}

start_maintenance_when_ready() {
  echo "[entrypoint] waiting for VPN and ClickHouse before background maintenance"

  while true; do
    if is_vpn_ok && is_clickhouse_ok; then
      echo "[entrypoint] VPN and ClickHouse OK, processing missed messages"
      python /app/missed_messages_worker.py || echo "[entrypoint] missed messages worker failed"

      echo "[entrypoint] starting heartbeat worker"
      python /app/heartbeat_worker.py &
      return
    fi

    echo "[entrypoint] background maintenance is waiting: VPN or ClickHouse is not ready"
    sleep "$HEARTBEAT_START_CHECK_INTERVAL_SEC"
  done
}

echo "[entrypoint] starting vpn supervisor"
python /app/vpn_supervisor.py &

echo "[entrypoint] starting clickhouse supervisor"
python /app/clickhouse_supervisor.py &

start_maintenance_when_ready &

echo "[entrypoint] starting slack bot"
exec python /app/main.py
