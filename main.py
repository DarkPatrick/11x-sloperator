# на ubuntu
# docker compose up -d --build
# docker compose logs -f
# на mac
# docker compose down
# docker compose down
# docker compose build --no-cache

import os
import logging
import re
from pathlib import Path
import ssl
import certifi
from slack_sdk import WebClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from slack_worker import SlackWorker, SlackWorkerError
from stats import calculate_exp_info



EXP_RE = re.compile(r"^\s*exp\s*#\s*(\d+)\s*$", re.IGNORECASE)


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


app = App(token=os.environ.get("SLACK_BOT_TOKEN", ""))

slack = SlackWorker()

# @app.event("message")
# def debug_all_messages(body, logger):
#     logger.info("RAW MESSAGE EVENT: %s", body)

# @app.message(re.compile(r".+"))
# def handle_exp_message(message, logger):
#     text = message.get("text", "")
#     user = message.get("user")
#     logger.info("Incoming message from %s: %s", user, text)


# @app.message("")
# def handle_message(message, say, logger):
#     text = message.get("text", "")
#     user = message.get("user")
#     logger.info("Incoming message from %s: %s", user, text)
#     say(f"Получил: {text}")

@app.message(re.compile(r"(?i)^\s*exp\s*#\s*\d+\s*$"))
# @app.message("")
def handle_exp_message(message, say, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("Incoming message from %s: %s", user, text)
    # say(f"Получил: {text}")
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
        table, table_cum, stat_results, summary = calculate_exp_info(exp_id)

        formatted_table, truncated = slack.format_table_for_slack(table)

        result_text = (
            f"*Experiment #{exp_id}*\n\n"
            # f"*Таблица*\n{formatted_table}\n\n"
            f"*Итоги*\n{summary}"
        )

        # send table as csv file
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

            channel_id = event["channel"]
            slack.upload_text_snippet(
                title=f"exp_{exp_id}_table",
                content=str(table),
                channel_id=channel_id,
            )

    except Exception as exc:
        logger.exception("Failed to calculate exp info for exp_id=%s", exp_id)
        slack.update_event_reply(
            event,
            progress_ts,
            f"Не удалось посчитать experiment #{exp_id}.\nОшибка: `{exc}`"
        )



if __name__ == "__main__":
    print("starting slack socket mode...", flush=True)
    handler = SocketModeHandler(app, os.environ["SLACK_BOT_SOCKET_TOKEN_ID"])
    handler.start()