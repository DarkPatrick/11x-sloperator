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
import certifi
from slack_sdk import WebClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from concurrent.futures import ThreadPoolExecutor
from chatgpt_agent_worker import ChatGPTAgentWorker

from slack_worker import SlackWorker, SlackWorkerError
from clickhouse_worker import get_ugm_exps_list
from stats import calculate_exp_info



EXP_RE = re.compile(r"^\s*exp\s*#\s*(\d+)\s*$", re.IGNORECASE)
VPN_RECONNECT_REQUEST_PATH = os.environ.get(
    "VPN_RECONNECT_REQUEST_PATH",
    "/tmp/vpn_reconnect_requested",
)

VPN_RECONNECT_RE = re.compile(r"^\s*vpn\s+reconnect\s*$", re.IGNORECASE)


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


app = App(token=os.environ.get("SLACK_BOT_TOKEN", ""))

slack = SlackWorker()
agent = ChatGPTAgentWorker(slack=slack)

executor = ThreadPoolExecutor(max_workers=2)


def request_vpn_reconnect() -> None:
    with open(VPN_RECONNECT_REQUEST_PATH, "w", encoding="utf-8") as f:
        f.write("1")


@app.message(VPN_RECONNECT_RE)
def handle_vpn_reconnect_message(message, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("Incoming message from %s: %s", user, text)
    event = message

    request_vpn_reconnect()
    slack.send_event_reply(
        event,
        "Ок, запускаю новую попытку VPN-подключения."
    )


@app.message("ugm_exps")
def handle_all_ugm_exp_message(message, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("Incoming message from %s: %s", user, text)
    event = message

    progress_ts = slack.send_event_reply(
        event,
        f"Считаю все монетизационные эксперименты: активные и закрытые в последний месяц. это может занять некоторое время..."
    )
    exp_ids = get_ugm_exps_list()
    i = 1
    max_i = len(exp_ids)
    for exp_id in exp_ids:
        try:
            calculate_exp_info(exp_id)
            result_text = (
                f"*Посчитал experiment #{exp_id}*\n\n"
                f"Посчитано {i}/{max_i}"
            )
            slack.update_event_reply(event, progress_ts, result_text)
        except Exception as exc:
            logger.exception("Failed to calculate exp info for exp_id=%s", exp_id)
            slack.update_event_reply(
                event,
                progress_ts,
                f"Не удалось посчитать experiment #{exp_id}.\nОшибка: `{exc}`"
            )
        i += 1


@app.message(re.compile(r"(?i)^\s*exp\s*#\s*\d+\s*$"))
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
    # Slack может присылать bot/system events.
    # Их лучше игнорировать, чтобы бот не отвечал сам себе.
    subtype = event.get("subtype")
    if subtype:
        return

    text = event.get("text", "").strip()
    if not text:
        return
    
    if not is_dm(event):
        if not is_bot_mentioned(text, os.environ.get("SLACK_BOT_ID", "")):
            return

    logger.info("Incoming user message: %s", text)

    executor.submit(process_agent_message, event)



if __name__ == "__main__":
    print("starting slack socket mode...", flush=True)
    handler = SocketModeHandler(app, os.environ["SLACK_BOT_SOCKET_TOKEN_ID"])
    handler.start()