# TODO: распределение подписок по продуктам

# на ubuntu
# docker compose up -d --build
# docker compose logs -f
# на mac
# docker compose down
# docker compose build --no-cache
# docker compose up

import os
import logging
import re
from pathlib import Path
import ssl
from time import time
import certifi
from slack_sdk import WebClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from concurrent.futures import ThreadPoolExecutor
from chatgpt_agent_worker import ChatGPTAgentWorker

from slack_worker import SlackWorker, SlackWorkerError
from clickhouse_worker import get_ugm_exps_list, get_ugp_exps_list, get_ugg_exps_list, clear_exp_temp_tables, get_mb_user
from stats import calculate_exp_info



EXP_RE = re.compile(r"^\s*exp\s*#\s*(\d+)\s*$", re.IGNORECASE)
VPN_RECONNECT_REQUEST_PATH = os.environ.get(
    "VPN_RECONNECT_REQUEST_PATH",
    "/tmp/vpn_reconnect_requested",
)

VPN_RECONNECT_RE = re.compile(r"^\s*vpn\s+reconnect\s*$", re.IGNORECASE)
SLACK_NOTIFY_USER_ID = os.environ.get("SLACK_NOTIFY_USER_ID", "")
MB_USER_RE = re.compile(r"^\s*mb\s+user\s+(\d+)\s*$", re.IGNORECASE)


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


app = App(token=os.environ.get("SLACK_BOT_TOKEN", ""))

slack = SlackWorker()
agent = ChatGPTAgentWorker(slack=slack)

executor = ThreadPoolExecutor(max_workers=2)


def request_vpn_reconnect() -> None:
    with open(VPN_RECONNECT_REQUEST_PATH, "w", encoding="utf-8") as f:
        f.write("1")


# @app.message(VPN_RECONNECT_RE)
def handle_vpn_reconnect_message(message, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("handle_vpn_reconnect_message called with text: %r from user %s", text, user)
    event = message

    request_vpn_reconnect()
    logger.info("VPN reconnect request file created")
    slack.send_event_reply(
        event,
        "Ок, запускаю новую попытку VPN-подключения."
    )
    logger.info("VPN reconnect reply sent")

# @app.message("clear_exp_temp_tables")
def handle_clear_exp_temp_tables_message(message, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("Incoming message from %s: %s", user, text)
    event = message

    slack.send_event_reply(
        event,
        "Ок, очищаю временные таблицы экспериментов."
    )
    clear_exp_temp_tables()
    slack.reply_in_thread(
        "Готово, все временные таблицы удалены.",
        thread_ts=event.get("ts")
    )    


# @app.message(re.compile(r"^\s*(ugm_exps|ugp_exps|ugg_exps)\s*$", re.IGNORECASE))
def handle_all_ug_exp_message(message, logger):
    text = message.get("text", "").strip().lower()
    user = message.get("user")
    logger.info("handle_all_ug_exp_message called with text: %r from user %s", text, user)
    event = message

    # --- выбор типа экспериментов ---
    if text == "ugm_exps":
        exp_ids = get_ugm_exps_list()
        exp_type = "монетизационные"
    elif text == "ugp_exps":
        exp_ids = get_ugp_exps_list()
        exp_type = "Product"
    elif text == "ugg_exps":
        exp_ids = get_ugg_exps_list()
        exp_type = "Growth"
    else:
        slack.send_event_reply(event, "Неизвестная команда")
        return

    # --- стартовое сообщение (создаёт thread) ---
    thread_ts = slack.send_event_reply(
        event,
        f"Считаю все {exp_type} эксперименты. Это может занять некоторое время..."
    )

    retry_exps = []
    failed_exps = []

    total = len(exp_ids)

    # --- первый проход ---
    for i, exp_id in enumerate(exp_ids, 1):
        try:
            calculate_exp_info(exp_id)

            slack.reply_in_thread(
                f"✅ Посчитал experiment #{exp_id} ({i}/{total})",
                thread_ts=thread_ts
            )

        except Exception as exc:
            logger.exception("Failed to calculate exp info for exp_id=%s", exp_id)
            retry_exps.append(exp_id)

            slack.reply_in_thread(
                f"⚠️ Ошибка при расчёте experiment #{exp_id}, добавил в retry ({i}/{total})",
                thread_ts=thread_ts
            )

    # --- retry ---
    if retry_exps:
        slack.reply_in_thread(
            f"Повторно считаю {len(retry_exps)} экспериментов...",
            thread_ts=thread_ts
        )

        for exp_id in retry_exps:
            try:
                calculate_exp_info(exp_id)

                slack.reply_in_thread(
                    f"✅ Retry успешен для experiment #{exp_id}",
                    thread_ts=thread_ts
                )

            except Exception:
                logger.exception("Retry failed for exp_id=%s", exp_id)
                failed_exps.append(exp_id)

    # --- финал ---
    if failed_exps:
        failed_list = ", ".join(map(str, failed_exps))

        slack.reply_in_thread(
            f"❌ Не удалось посчитать даже после retry:\n{failed_list}",
            thread_ts=thread_ts
        )
    else:
        slack.reply_in_thread(
            "✅ Все эксперименты успешно посчитаны",
            thread_ts=thread_ts
        )


# @app.message(re.compile(r"(?i)^\s*exp\s*#\s*\d+\s*$"))
def handle_exp_message(message, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("Incoming message from %s: %s", user, text)
    event = message

    match = EXP_RE.match(text)
    if not match:
        progress_ts = slack.send_event_reply(
            event,
            f"hmmm.... {text}"
        )
        return

    exp_id = int(match.group(1))

    # Сразу пишем в тот же тред, чтобы пользователь видел, что запрос принят
    progress_ts = slack.send_event_reply(
        event,
        f"Считаю experiment {exp_id}, это может занять некоторое время..."
    )

    try:
        tables_dit, tables_cum_dict, stat_results_cum_dict, summary = calculate_exp_info(exp_id)

        # formatted_table, truncated = slack.format_table_for_slack(table)

        result_text = (
            f"*Experiment #{exp_id}*\n\n"
            # f"*Таблица*\n{formatted_table}\n\n"
            f"*Итоги*\n{summary}"
        )

        # send table as csv file
        for client, table in tables_dit.items():
            table_cum = tables_cum_dict[client]
            stat_results = stat_results_cum_dict[client]
            slack.upload_csv_file(
                title=f"Experiment #{exp_id} - Таблица",
                filename=f"experiment_{exp_id}_table.csv",
                content=table.to_csv(index=False),
                thread_ts=progress_ts,
                channel_id=event["channel"],
            )
            slack.upload_csv_file(
                title=f"Experiment #{exp_id} - Кумулятивная таблица по дням",
                filename=f"experiment_{exp_id}_table_cum.csv",
                content=table_cum.to_csv(index=False),
                thread_ts=progress_ts,
                channel_id=event["channel"],
            )
            slack.upload_csv_file(
                title=f"Experiment #{exp_id} - Статистика по метрикам",
                filename=f"experiment_{exp_id}_table_cum.csv",
                content=stat_results.to_csv(index=False),
                thread_ts=progress_ts,
                channel_id=event["channel"],
            )
        # Если сообщение слишком длинное, сначала пробуем обновить короткой версией,
        # а полную таблицу можно выгрузить snippet'ом
        if len(result_text) <= 3900:
            slack.update_event_reply(event, progress_ts, result_text)
        else:
            short_text = (
                f"*Experiment #{exp_id}*\n"
                f"Таблица слишком большая для обычного сообщения.\n\n"
                f"*Итоги*\n{summary}"
            )
            slack.update_event_reply(event, progress_ts, short_text)

            # channel_id = event["channel"]
            # slack.upload_text_snippet(
            #     title=f"exp_{exp_id}_table",
            #     content=str(table),
            #     channel_id=channel_id,
            # )

    except Exception as exc:
        logger.exception("Failed to calculate exp info for exp_id=%s", exp_id)
        slack.update_event_reply(
            event,
            progress_ts,
            f"Не удалось посчитать experiment #{exp_id}.\nОшибка: `{exc}`"
        )


def is_dm(event: dict) -> bool:
    channel = event.get("channel", "")
    return channel.startswith("D")


def is_bot_mentioned(text: str, bot_user_id: str) -> bool:
    if not text:
        return False
    return f"<@{bot_user_id}>" in text


def process_agent_message(event: dict):
    progress_ts = None

    try:
        progress_ts = slack.send_event_reply(event, "Думаю...")
        answer = agent.build_answer_for_slack_event(event)
        slack.update_event_reply(event, progress_ts, answer)

    except Exception as exc:
        logger.exception("Failed to process ChatGPT agent request")
        error_text = f"Не смог ответить: `{exc}`"

        if progress_ts:
            slack.update_event_reply(event, progress_ts, error_text)
        else:
            slack.send_event_reply(event, error_text)


@app.event("message")
def handle_any_message(body, event, logger):
    subtype = event.get("subtype")
    if subtype:
        return

    slack.save_user_message(event)

    text = event.get("text", "").strip()
    if not text:
        return

    # Log raw text for debugging routing issues
    logger.info("Message received: user=%s text=%r", event.get("user"), text)

    if EXP_RE.match(text):
        logger.info("Matched EXP_RE pattern")
        handle_exp_message(event, logger)
        return
    
    if VPN_RECONNECT_RE.match(text):
        logger.info("Matched VPN_RECONNECT_RE pattern")
        handle_vpn_reconnect_message(event, logger)
        return
    
    if text.lower() == "clear_exp_temp_tables":
        logger.info("Matched clear_exp_temp_tables")
        handle_clear_exp_temp_tables_message(event, logger)
        return
    
    if re.match(r"^\s*(ugm_exps|ugp_exps|ugg_exps)\s*$", text, re.IGNORECASE):
        logger.info("Matched experiment list pattern")
        handle_all_ug_exp_message(event, logger)
        return

    if MB_USER_RE.match(text):
        logger.info("Matched MB_USER_RE pattern")
        match = MB_USER_RE.match(text)
        if not match:
            logger.error("MB_USER_RE matched but no group found, this should not happen")
            slack.send_event_reply(event, "Unexpected error parsing command")
            return
        mb_user_id = int(match.group(1))
        try:
            mb_user_info = get_mb_user(mb_user_id)
            answer = f"Metabase user info for ID {mb_user_id}:\n```{mb_user_info}```"
        except Exception as exc:
            logger.exception("Failed to get Metabase user info for ID %s", mb_user_id)
            answer = f"Failed to get Metabase user info for ID {mb_user_id}: `{exc}`"
        
        slack.send_event_reply(event, answer)
        return
    
    logger.info("No command pattern matched, checking if should process as agent message")
    
    if not is_dm(event):
        if not is_bot_mentioned(text, os.environ.get("SLACK_BOT_ID", "")):
            logger.info("Not a DM and bot not mentioned, ignoring")
            return

    # temp filter to work only for me
    if event.get("user") != SLACK_NOTIFY_USER_ID:
        logger.info("Message from different user, ignoring")
        return
    
    logger.info("Processing as agent message: %s", text)
    executor.submit(process_agent_message, event)


if __name__ == "__main__":
    print("starting slack socket mode...", flush=True)

    handler = SocketModeHandler(app, os.environ["SLACK_BOT_SOCKET_TOKEN_ID"])
    handler.start()
