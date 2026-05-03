import logging
from concurrent.futures import ThreadPoolExecutor

from chatgpt_agent_worker import ChatGPTAgentWorker
from conversation_store import ConversationStore
from service_readiness import wait_for_vpn_and_clickhouse
from slack_worker import SlackWorker


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


MAX_MISSED_MESSAGE_WORKERS = 2


def process_agent_message(slack: SlackWorker, agent: ChatGPTAgentWorker, event: dict) -> None:
    progress_ts = None

    try:
        progress_ts = slack.send_event_reply(event, "Думаю...")
        answer = agent.build_answer_for_slack_event(event)
        slack.update_event_reply(event, progress_ts, answer)

    except Exception as exc:
        logger.exception("Failed to process missed ChatGPT agent request")
        error_text = f"Не смог ответить: `{exc}`"

        if progress_ts:
            slack.update_event_reply(event, progress_ts, error_text)
        else:
            slack.send_event_reply(event, error_text)


def process_missed_messages_once() -> None:
    slack = SlackWorker()
    agent = ChatGPTAgentWorker(slack=slack)
    store = ConversationStore()

    missed_messages = store.get_unanswered_messages_since_last_heartbeat()
    logger.info("Found %s missed unanswered messages", len(missed_messages))

    if not missed_messages:
        return

    with ThreadPoolExecutor(max_workers=MAX_MISSED_MESSAGE_WORKERS) as executor:
        futures = [
            executor.submit(process_agent_message, slack, agent, event)
            for event in missed_messages
        ]

        for future in futures:
            future.result()


def main() -> None:
    wait_for_vpn_and_clickhouse()
    process_missed_messages_once()


if __name__ == "__main__":
    main()
