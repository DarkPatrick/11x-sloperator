import os
import time
import logging
from typing import Any, Optional

from openai import OpenAI

from slack_worker import SlackWorker

logger = logging.getLogger(__name__)


class ChatGPTAgentWorkerError(Exception):
    """Base ChatGPT agent worker error."""


class ChatGPTAgentWorker:
    def __init__(
        self,
        *,
        slack: SlackWorker,
        model: Optional[str] = None,
        max_history_messages: int = 120,
        max_thread_messages: int = 200,
    ) -> None:
        self.slack = slack
        self.client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        self.model = model or os.environ.get("OPENAI_MODEL", "gpt-5.5")
        self.max_history_messages = max_history_messages
        self.max_thread_messages = max_thread_messages

    def build_answer_for_slack_event(self, event: dict[str, Any]) -> str:
        """
        Main public method:
        - builds Slack context
        - calls OpenAI
        - returns text ready to send to Slack
        """
        context = self.build_context_from_slack_event(event)
        return self.ask_agent(context)

    def build_context_from_slack_event(self, event: dict[str, Any]) -> dict[str, Any]:
        channel_id = event.get("channel")
        if not channel_id:
            raise ChatGPTAgentWorkerError("Slack event does not contain channel")

        event_ts = event.get("ts")
        thread_ts = event.get("thread_ts")
        root_thread_ts = thread_ts or event_ts

        user_id = event.get("user")
        text = event.get("text", "")

        one_week_ago = time.time() - 7 * 24 * 60 * 60

        history = self.slack.get_conversation_history(
            channel_id=channel_id,
            oldest=one_week_ago,
            limit=self.max_history_messages,
        )

        thread_replies: list[dict[str, Any]] = []
        if thread_ts and root_thread_ts:
            thread_replies = self.slack.get_thread_replies(
                channel_id=channel_id,
                thread_ts=root_thread_ts,
                limit=self.max_thread_messages,
            )

        bot_identity = self.slack.get_bot_identity()
        bot_name = os.environ.get("SLACK_BOT_NAME") or bot_identity.get("bot_name") or "Slack bot"

        return {
            "current_message": {
                "channel_id": channel_id,
                "user_id": user_id,
                "user_name": self.slack.get_user_label(user_id),
                "text": text,
                "ts": event_ts,
                "thread_ts": thread_ts,
            },
            "bot": {
                "bot_user_id": bot_identity.get("bot_user_id", ""),
                "bot_name": bot_name,
                "team": bot_identity.get("team", ""),
                "team_id": bot_identity.get("team_id", ""),
            },
            "history_last_week": self._serialize_messages(history),
            "thread_messages": self._serialize_messages(thread_replies),
            "attachments": self._extract_attachment_placeholders(history, thread_replies),
        }

    def _serialize_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, str]]:
        """
        Convert raw Slack messages to compact model context.
        Slack returns newest-first for conversations.history, so we sort by ts.
        """
        result: list[dict[str, str]] = []

        sorted_messages = sorted(
            messages,
            key=lambda m: float(m.get("ts", "0") or 0),
        )

        for msg in sorted_messages:
            subtype = msg.get("subtype")
            if subtype in {"message_deleted", "message_changed"}:
                continue

            user_id = msg.get("user") or msg.get("bot_id") or "unknown"
            text = msg.get("text") or ""

            # Пока документы/картинки не грузим.
            # Но оставляем маркер, чтобы агент понимал, что они были.
            files = msg.get("files") or []
            if files:
                text += f"\n[В сообщении есть файлов: {len(files)}. Содержимое файлов пока не загружено.]"

            result.append(
                {
                    "ts": str(msg.get("ts", "")),
                    "user_id": str(user_id),
                    "user_name": self.slack.get_user_label(msg.get("user")),
                    "text": text,
                }
            )

        return result

    def _extract_attachment_placeholders(
        self,
        history: list[dict[str, Any]],
        thread_replies: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        """
        Future extension point.
        Later here we can download files/images from Slack,
        run OCR/image understanding, and add extracted content to context.
        """
        placeholders: list[dict[str, str]] = []

        for msg in [*history, *thread_replies]:
            for file_obj in msg.get("files") or []:
                placeholders.append(
                    {
                        "ts": str(msg.get("ts", "")),
                        "file_id": str(file_obj.get("id", "")),
                        "name": str(file_obj.get("name", "")),
                        "mimetype": str(file_obj.get("mimetype", "")),
                        "note": "File/image content is not loaded yet.",
                    }
                )

        return placeholders

    def ask_agent(self, context: dict[str, Any]) -> str:
        system_prompt = self._build_system_prompt(context)

        user_prompt = (
            "Ниже Slack-контекст. Ответь на последнее сообщение пользователя.\n\n"
            f"{self._format_context_for_model(context)}"
        )

        response = self.client.responses.create(
            model=self.model,
            instructions=system_prompt,
            input=user_prompt,
        )

        text = getattr(response, "output_text", None)
        if not text:
            # fallback на случай изменения формата SDK/ответа
            text = str(response)

        return text.strip()

    def _build_system_prompt(self, context: dict[str, Any]) -> str:
        bot = context["bot"]

        return f"""
Ты агент, встроенный в Slack-бота.

Твоё имя в Slack: {bot.get("bot_name")}.
Твой Slack bot user id: {bot.get("bot_user_id")}.

Ты отвечаешь пользователю в Slack. Твой ответ будет переслан в Slack как обычное сообщение.
если пользователь написал по-русски, отвечай по-русски, если по-английски - по-английски.

Стиль:
- отвечай коротко и по делу;
- не будь сухим и слишком официальным;
- деловой стиль не нужен;
- можно писать нормально, по-человечески;
- не используй слишком длинные вступления;
- форматируй под Slack: короткие абзацы, списки, `код`, ```блоки кода```;
- если не хватает данных, задай короткий уточняющий вопрос;
- не выдумывай факты, которых нет в контексте;
- если контекст из треда противоречит истории канала, приоритет у треда.
- если это не личная беседа, и ты не уверен что пользователь обращается именно к тебе, ничего не отвечай.
""".strip()

    def _format_context_for_model(self, context: dict[str, Any]) -> str:
        current = context["current_message"]
        bot = context["bot"]

        parts: list[str] = []

        parts.append("## Slack bot info")
        parts.append(f"bot_name: {bot.get('bot_name')}")
        parts.append(f"bot_user_id: {bot.get('bot_user_id')}")
        parts.append(f"team: {bot.get('team')}")
        parts.append("")

        parts.append("## Current message")
        parts.append(f"channel_id: {current.get('channel_id')}")
        parts.append(f"user_id: {current.get('user_id')}")
        parts.append(f"user_name: {current.get('user_name')}")
        parts.append(f"ts: {current.get('ts')}")
        parts.append(f"thread_ts: {current.get('thread_ts')}")
        parts.append(f"text: {current.get('text')}")
        parts.append("")

        parts.append("## Conversation history: last 7 days")
        if context["history_last_week"]:
            for msg in context["history_last_week"]:
                parts.append(
                    f"[{msg['ts']}] {msg['user_name']} ({msg['user_id']}): {msg['text']}"
                )
        else:
            parts.append("No recent history.")
        parts.append("")

        parts.append("## Full thread context")
        if context["thread_messages"]:
            for msg in context["thread_messages"]:
                parts.append(
                    f"[{msg['ts']}] {msg['user_name']} ({msg['user_id']}): {msg['text']}"
                )
        else:
            parts.append("Current message is not inside a thread, or no thread replies found.")
        parts.append("")

        parts.append("## Attachments / images placeholders")
        if context["attachments"]:
            for item in context["attachments"]:
                parts.append(
                    f"- ts={item['ts']} file={item['name']} mimetype={item['mimetype']} note={item['note']}"
                )
        else:
            parts.append("No loaded attachments. File/image loading is not implemented yet.")

        return "\n".join(parts)