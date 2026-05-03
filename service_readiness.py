import json
import logging
import os
import time
from typing import Optional

from clickhouse_worker import _get_client

logger = logging.getLogger(__name__)


VPN_STATE_PATH = os.environ.get("VPN_STATE_PATH", "/tmp/vpn_state.json")
SERVICE_READY_CHECK_INTERVAL_SEC = int(os.environ.get("SERVICE_READY_CHECK_INTERVAL_SEC", "10"))


def read_vpn_state() -> Optional[dict]:
    try:
        with open(VPN_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        logger.warning("Failed to decode VPN state file: %s", exc)
        return None


def is_vpn_ok() -> bool:
    state = read_vpn_state()
    return bool(state and state.get("connected") is True)


def is_clickhouse_ok() -> bool:
    client = _get_client()
    try:
        client.command("SELECT 42")
        return True
    finally:
        try:
            client.close()
        except Exception:
            pass


def are_vpn_and_clickhouse_ok() -> bool:
    if not is_vpn_ok():
        return False

    try:
        return is_clickhouse_ok()
    except Exception:
        logger.exception("ClickHouse readiness check failed")
        return False


def wait_for_vpn_and_clickhouse() -> None:
    while True:
        if are_vpn_and_clickhouse_ok():
            logger.info("VPN and ClickHouse are ready")
            return

        logger.info("Waiting for VPN and ClickHouse")
        time.sleep(SERVICE_READY_CHECK_INTERVAL_SEC)
