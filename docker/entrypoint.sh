#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] starting vpn supervisor"
python /app/vpn_supervisor.py &

echo "[entrypoint] starting clickhouse supervisor"
python /app/clickhouse_supervisor.py &

echo "[entrypoint] starting slack bot"
exec python /app/main.py
