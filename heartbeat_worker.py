import os
import time
import logging

from conversation_store import ConversationStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HEARTBEAT_INTERVAL_SEC = int(os.environ.get("HEARTBEAT_INTERVAL_SEC", "600"))


def main() -> None:
    store = ConversationStore()
    store.init_tables()

    while True:
        try:
            store.write_heartbeat()
            logger.info("Heartbeat written")
        except Exception:
            logger.exception("Failed to write heartbeat")

        time.sleep(HEARTBEAT_INTERVAL_SEC)


if __name__ == "__main__":
    main()