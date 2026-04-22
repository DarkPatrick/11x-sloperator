#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${VPN_CONFIG_PATH:-/app/vpn/hz config 2fa.ovpn}"
LOG_PATH="${VPN_LOG_PATH:-/tmp/openvpn.log}"

echo "[vpn] starting openvpn with config: $CONFIG_PATH"
echo "[vpn] checking config file exists"

ls -l /app/vpn || true
ls -l "$CONFIG_PATH" || true
echo "[vpn] auth-user-pass lines:"
grep -n "auth-user-pass" "$CONFIG_PATH" || true
echo "[vpn] pass file:"
cat /app/vpn/pass.txt || true

rm -f "$LOG_PATH"

openvpn \
  --config "$CONFIG_PATH" \
  --log "$LOG_PATH" \
  --verb 4 \
  --daemon

echo "[vpn] openvpn started in background"
