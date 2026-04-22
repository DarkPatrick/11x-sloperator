# на ubuntu
# docker compose up -d --build
# docker compose logs -f
# на mac
# docker compose down
# docker compose down
# docker compose build --no-cache

import os
from pathlib import Path
import ssl
import certifi
from slack_sdk import WebClient
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler



app = App(token=os.environ.get("SLACK_BOT_TOKEN", ""))


@app.message("")
def handle_message(message, say, logger):
    text = message.get("text", "")
    user = message.get("user")
    logger.info("Incoming message from %s: %s", user, text)
    say(f"Получил: {text}")


if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ.get("SLACK_BOT_SOCKET_TOKEN_ID", ""))
    handler.start()
