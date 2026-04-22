import os
import time
import json
import socket
import logging
from typing import Optional

from slack_sdk import WebClient
from clickhouse_worker import _get_client
from slack_worker import SlackWorker



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_NOTIFY_USER_ID = os.environ["SLACK_NOTIFY_USER_ID"]

VPN_MGMT_SOCK = os.environ.get("VPN_MGMT_SOCK", "/tmp/openvpn-mgmt.sock")
CHECK_INTERVAL_SEC = int(os.environ.get("CLICKHOUSE_CHECK_INTERVAL_SEC", "600"))
VPN_STATE_PATH = os.environ.get("VPN_STATE_PATH", "/tmp/vpn_state.json")


client = WebClient(token=SLACK_BOT_TOKEN)
slack = SlackWorker()


# def get_channel_id() -> str:
#     resp = client.conversations_open(users=SLACK_NOTIFY_USER_ID)
#     return resp["channel"]["id"]


# CHANNEL_ID = get_channel_id()


# def send_message(text: str, thread_ts: Optional[str] = None) -> str:
#     resp = client.chat_postMessage(
#         channel=CHANNEL_ID,
#         text=text,
#         thread_ts=thread_ts,
#         unfurl_links=False,
#         unfurl_media=False,
#     )
#     return resp.get("ts", "")

def read_vpn_state() -> Optional[dict]:
    try:
        with open(VPN_STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        logger.warning("Failed to decode VPN state file: %s", e)
        return None


def read_until_end(sock: socket.socket, timeout: float = 3.0) -> str:
    sock.settimeout(timeout)
    chunks = []

    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break

        text = chunk.decode("utf-8", errors="replace")
        chunks.append(text)

        # для management interface многие команды завершаются END
        if "\nEND" in text or text.endswith("\nEND\r\n") or text.endswith("\nEND\n"):
            break

    return "".join(chunks)


def is_vpn_up() -> bool:
    if not os.path.exists(VPN_MGMT_SOCK):
        return False

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    try:
        sock.connect(VPN_MGMT_SOCK)

        # banner
        try:
            read_until_end(sock, timeout=0.5)
        except Exception:
            pass

        sock.sendall(b"state\n")
        data = read_until_end(sock, timeout=3.0)

    except Exception as exc:
        logger.warning("Failed to query OpenVPN management socket: %s", exc)
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass

    last_state = None

    for line in data.splitlines():
        line = line.strip()
        if line.startswith(">STATE:"):
            # формат обычно: >STATE:timestamp,CONNECTED,SUCCESS,...
            parts = line.split(",")
            if len(parts) >= 2:
                last_state = parts[1]

    return last_state == "CONNECTED"


def main():
    thread_ts: Optional[str] = None
    last_error: Optional[str] = None
    last_cycle_id: Optional[int] = None
    last_skip_reason: Optional[str] = None
    is_clickhouse_first_ok = False

    while True:
        try:
            vpn_state = read_vpn_state()

            if not vpn_state:
                logger.info("VPN state file does not exist yet, skipping")
                time.sleep(60)
                continue

            connected = vpn_state.get("connected", False)
            ever_connected = vpn_state.get("ever_connected", False)
            cycle_id = vpn_state.get("cycle_id")
            status = vpn_state.get("status", "unknown")

            logger.info(
                "VPN state: connected=%s ever_connected=%s cycle_id=%s status=%s",
                connected, ever_connected, cycle_id, status
            )

            # До первого успешного подключения ничего не делаем
            if not ever_connected:
                logger.info("VPN has never connected yet, skipping ClickHouse check")
                time.sleep(60)
                continue

            # VPN сейчас не подключён — проверки пропускаем
            if not connected:
                skip_reason = f"vpn_not_connected:{cycle_id}:{status}"

                if skip_reason != last_skip_reason:
                    thread_ts = slack.start_thread_in_dm("🚫 ClickHouse проверка пропущена: VPN сейчас не подключён.")
                    last_skip_reason = skip_reason

                last_error = None
                time.sleep(600)
                continue

            # VPN снова подключён, сбрасываем skip-маркер
            last_skip_reason = None

            # Если это новый цикл подключения после разрыва, начинаем новый тред
            if cycle_id != last_cycle_id:
                thread_ts = None
                last_error = None
                last_cycle_id = cycle_id

            ch = _get_client()
            try:
                ch.command("SELECT 42")
                logger.info("ClickHouse OK")

                # если раньше была ошибка — можно закрыть thread новым сообщением
                if last_error is not None:
                    # send_message("🟢 ClickHouse снова доступен", thread_ts)
                    if thread_ts is not None:
                        slack.reply_in_thread("🟢 ClickHouse снова доступен", thread_ts=thread_ts)
                    else:
                        thread_ts = slack.start_thread_in_dm("🟢 ClickHouse снова доступен")
                    thread_ts = None  # следующий цикл создаст новый thread

                last_error = None

            finally:
                try:
                    ch.close()
                except Exception:
                    pass
            if not is_clickhouse_first_ok:
                slack.start_thread_in_dm("✅ ClickHouse доступен. Наблюдаю за состоянием VPN и ClickHouse.")
            is_clickhouse_first_ok = True
            logger.info("ClickHouse OK")

            if thread_ts is not None and last_error is not None:
                # send_message("🟢 ClickHouse снова доступен.", thread_ts)
                slack.reply_in_thread("🟢 ClickHouse снова доступен.", thread_ts=thread_ts)
                thread_ts = None

            last_error = None

        except Exception as e:
            error_text = f"❌ Ошибка ClickHouse: {e}"
            logger.exception(error_text)

            if error_text != last_error:
                if thread_ts is None:
                    # thread_ts = send_message("🚨 Проблема с ClickHouse")
                    thread_ts = slack.start_thread_in_dm("🚨 Проблема с ClickHouse")
                # send_message(error_text, thread_ts)
                slack.reply_in_thread(error_text, thread_ts=thread_ts)
                last_error = error_text

        if is_clickhouse_first_ok:
            time.sleep(CHECK_INTERVAL_SEC)
        else:
            time.sleep(60)

if __name__ == "__main__":
    main()