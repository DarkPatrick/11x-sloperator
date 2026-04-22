import os
import re
import sys
import time
import json
import socket
import signal
import subprocess
from pathlib import Path
from typing import Optional

from slack_sdk import WebClient

from clickhouse_worker import execute_sql
from slack_worker import SlackWorker


VPN_STATE_PATH = os.environ.get("VPN_STATE_PATH", "/tmp/vpn_state.json")

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_NOTIFY_USER_ID = os.environ["SLACK_NOTIFY_USER_ID"]

VPN_CONFIG_PATH = os.environ.get("VPN_CONFIG_PATH", "/app/vpn/hz config 2fa.ovpn")
VPN_LOG_PATH = os.environ.get("VPN_LOG_PATH", "/tmp/openvpn.log")
VPN_MGMT_SOCK = os.environ.get("VPN_MGMT_SOCK", "/tmp/openvpn-mgmt.sock")

RESTART_DELAY = int(os.environ.get("VPN_RESTART_DELAY", "5"))
SQL_ON_CONNECT = os.environ.get("SQL_ON_CONNECT", "0") == "1"

URL_RE = re.compile(r'https://[^\s")]+')

client = WebClient(token=SLACK_BOT_TOKEN)
slack = SlackWorker()



def write_vpn_state(
    *,
    connected: bool,
    ever_connected: bool,
    cycle_id: int,
    status: str,
) -> None:
    data = {
        "connected": connected,
        "ever_connected": ever_connected,
        "cycle_id": cycle_id,
        "status": status,
        "updated_at": time.time(),
    }

    tmp_path = f"{VPN_STATE_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp_path, VPN_STATE_PATH)

# def send_dm(text: str, thread_ts: str | None = None) -> str:
#     dm = client.conversations_open(users=SLACK_NOTIFY_USER_ID)
#     channel = dm.get("channel")
#     if not channel:
#         raise ValueError("Failed to get channel ID from conversation response")
#     channel_id = channel.get("id")
#     resp = client.chat_postMessage(channel=channel_id, text=text, thread_ts=thread_ts, unfurl_links=False, unfurl_media=False)
#     return resp.get("ts", "")

def wait_for_socket(path: str, timeout: int = 30) -> None:
    start = time.time()
    while not os.path.exists(path):
        if time.time() - start > timeout:
            raise TimeoutError(f"management socket did not appear: {path}")
        time.sleep(0.2)

def connect_mgmt(path: str) -> socket.socket:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(path)
    s.settimeout(1.0)
    return s

def mgmt_send(sock: socket.socket, cmd: str) -> None:
    sock.sendall((cmd + "\n").encode("utf-8"))

def start_openvpn() -> subprocess.Popen:
    for p in [VPN_LOG_PATH, VPN_MGMT_SOCK]:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    # stdout/stderr в консоль контейнера
    proc = subprocess.Popen(
        ["openvpn", "--config", VPN_CONFIG_PATH, "--verb", "4"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    return proc

def maybe_send_url(line: str, thread_ts: str) -> bool:
    sent_url = False
    if not sent_url:
        m = URL_RE.search(line)
        if m:
            url = m.group(0)
            match = re.search(r'https://\S+', line)
            if match:
                url = match.group(0)
                url = url.rstrip('").]\'')
            # send_dm(
            #     f"Нужно завершить VPN-авторизацию.\nОткрой ссылку:\n{url}",
            #     thread_ts
            # )
            slack.reply_in_thread(
                f"Нужно завершить VPN-авторизацию.\nОткрой ссылку:\n{url}",
                thread_ts=thread_ts
            )
            sent_url = True
    return sent_url

def run_sql_after_connect(thread_ts: str) -> None:
    try:
        result = execute_sql("SELECT 42")
        # send_dm(f"VPN подключён. ClickHouse доступен.\nРезультат теста: {result}", thread_ts)
        slack.reply_in_thread(f"VPN подключён. ClickHouse доступен.\nРезультат теста: {result}", thread_ts=thread_ts)
    except Exception as e:
        slack.reply_in_thread(f"VPN поднялся, но запрос в ClickHouse не выполнился:\n{e}", thread_ts=thread_ts)

def supervise() -> None:
    cycle_id = 0
    ever_connected = False
    while True:
        sent_urls: set[str] = set()
        vpn_up = False
        cycle_id += 1

        write_vpn_state(
            connected=False,
            ever_connected=ever_connected,
            cycle_id=cycle_id,
            status="starting",
        )

        # thread_ts = send_dm("Запускаю VPN-подключение.")
        thread_ts = slack.start_thread_in_dm("Запускаю VPN-подключение.")

        proc = start_openvpn()

        try:
            wait_for_socket(VPN_MGMT_SOCK, timeout=30)
            sock = connect_mgmt(VPN_MGMT_SOCK)

            # включаем историю логов и live-updates
            mgmt_send(sock, "log on all")
            mgmt_send(sock, "state on")
            mgmt_send(sock, "echo on all")

            # отпускаем hold, если management-hold включён
            mgmt_send(sock, "hold release")

            while True:
                # если процесс умер — выходим в общий restart loop
                if proc.poll() is not None:
                    raise RuntimeError(f"OpenVPN process exited with code {proc.returncode}")

                try:
                    data = sock.recv(65536).decode("utf-8", errors="replace")
                except socket.timeout:
                    continue

                if not data:
                    raise RuntimeError("management socket closed")

                sent_url = False
                for raw_line in data.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue

                    print(f"[mgmt] {line}", flush=True)
                    if not vpn_up and not sent_url:
                        # maybe_send_url(line, sent_urls)
                        write_vpn_state(
                            connected=False,
                            ever_connected=ever_connected,
                            cycle_id=cycle_id,
                            status="waiting_auth",
                        )
                        sent_url = maybe_send_url(line, thread_ts=thread_ts)

                    if "Initialization Sequence Completed" in line:
                        if not vpn_up:
                            vpn_up = True
                            ever_connected = True
                            write_vpn_state(
                                connected=True,
                                ever_connected=ever_connected,
                                cycle_id=cycle_id,
                                status="connected",
                            )
                            # send_dm("VPN подключён.", thread_ts)
                            slack.reply_in_thread("VPN подключён.", thread_ts=thread_ts)
                            # if SQL_ON_CONNECT:
                            #     run_sql_after_connect(thread_ts=thread_ts)

                    # типичные состояния на разрыв/переподключение
                    if "RECONNECTING" in line or "TCP/UDP: Closing socket" in line:
                        if vpn_up:
                            # send_dm("VPN-соединение потеряно. Пытаюсь подключиться заново.", thread_ts=thread_ts)
                            slack.reply_in_thread("VPN-соединение потеряно. Пытаюсь подключиться заново.", thread_ts=thread_ts)
                        raise RuntimeError("VPN reconnect required")

                    if "AUTH_FAILED" in line:
                        # send_dm("VPN отверг аутентификацию. Запускаю новый цикл подключения.", thread_ts=thread_ts)
                        slack.reply_in_thread("VPN отверг аутентификацию. Запускаю новый цикл подключения.", thread_ts=thread_ts)
                        raise RuntimeError("VPN auth failed")

        except Exception as e:
            print(f"[supervisor] {e}", flush=True)
            write_vpn_state(
                connected=False,
                ever_connected=ever_connected,
                cycle_id=cycle_id,
                status="reconnecting",
            )
            try:
                proc.terminate()
                proc.wait(timeout=10)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

            # thread_ts = send_dm(f"VPN отключён или перезапускается.\nПричина: {e}", thread_ts=thread_ts)
            slack.reply_in_thread(f"VPN отключён или перезапускается.\nПричина: {e}", thread_ts=thread_ts)
            thread_ts = None
            time.sleep(RESTART_DELAY)
            continue

if __name__ == "__main__":
    supervise()
