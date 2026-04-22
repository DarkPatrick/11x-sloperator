#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] container started"

python /app/vpn_supervisor.py &
exec python /app/main.py
