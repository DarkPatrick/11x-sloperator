import logging
from concurrent.futures import ThreadPoolExecutor

from conversation_store import ConversationStore
from main import process_message_event
from service_readiness import wait_for_vpn_and_clickhouse


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


MAX_MISSED_MESSAGE_WORKERS = 2


def process_missed_message(event: dict) -> None:
    process_message_event(
        event,
        logger,
        save_message=False,
        process_agent_async=False,
    )


def process_missed_messages_once() -> None:
    store = ConversationStore()

    missed_messages = store.get_unanswered_messages_since_last_heartbeat()
    logger.info("Found %s missed unanswered messages", len(missed_messages))

    if not missed_messages:
        return

    with ThreadPoolExecutor(max_workers=MAX_MISSED_MESSAGE_WORKERS) as executor:
        futures = [
            executor.submit(process_missed_message, event)
            for event in missed_messages
        ]

        for future in futures:
            future.result()


def main() -> None:
    wait_for_vpn_and_clickhouse()
    process_missed_messages_once()


if __name__ == "__main__":
    main()
