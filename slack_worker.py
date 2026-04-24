import os
import logging
from typing import Optional, Any
from io import StringIO
import pandas as pd

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError



logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SlackWorkerError(Exception):
    """Base Slack worker error."""


class SlackChannelError(SlackWorkerError):
    """Raised when Slack channel cannot be resolved."""


class SlackWorker:
    def __init__(
        self,
        bot_token: Optional[str] = None,
        default_user_id: Optional[str] = None,
    ) -> None:
        self.bot_token = bot_token or os.environ["SLACK_BOT_TOKEN"]
        self.default_user_id = default_user_id or os.environ.get("SLACK_NOTIFY_USER_ID")

        self.client = WebClient(token=self.bot_token)
        self._dm_channel_cache: dict[str, str] = {}

    def get_dm_channel_id(self, user_id: Optional[str] = None, use_cache: bool = True) -> str:
        """
        Resolve DM channel id for a user.
        Caches channel id in memory by default.
        """
        target_user_id = user_id or self.default_user_id
        if not target_user_id:
            raise SlackChannelError("Slack user id is not provided")

        if use_cache and target_user_id in self._dm_channel_cache:
            return self._dm_channel_cache[target_user_id]

        try:
            resp = self.client.conversations_open(users=target_user_id)
        except SlackApiError as exc:
            logger.exception("Failed to open DM with user %s", target_user_id)
            raise SlackChannelError(f"Failed to open DM with user {target_user_id}: {exc}") from exc

        channel = resp.get("channel")
        if not channel:
            raise SlackChannelError("Failed to get channel object from conversations_open response")

        channel_id = channel.get("id")
        if not channel_id:
            raise SlackChannelError("Failed to get channel id from conversations_open response")

        if use_cache:
            self._dm_channel_cache[target_user_id] = channel_id

        return channel_id

    def send_message(
        self,
        *,
        channel_id: str,
        text: str,
        thread_ts: Optional[str] = None,
        unfurl_links: bool = False,
        unfurl_media: bool = False,
    ) -> str:
        """
        Send message to any Slack channel and return ts.
        """
        try:
            resp = self.client.chat_postMessage(
                channel=channel_id,
                text=text,
                thread_ts=thread_ts,
                unfurl_links=unfurl_links,
                unfurl_media=unfurl_media,
            )
        except SlackApiError as exc:
            logger.exception("Failed to send message to channel %s", channel_id)
            raise SlackWorkerError(f"Failed to send Slack message: {exc}") from exc

        ts = resp.get("ts")
        if not ts:
            raise SlackWorkerError("Slack response does not contain ts")

        return ts

    def send_dm(
        self,
        text: str,
        thread_ts: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> str:
        """
        Send DM to a user and return message ts.
        """
        channel_id = self.get_dm_channel_id(user_id=user_id)
        return self.send_message(
            channel_id=channel_id,
            text=text,
            thread_ts=thread_ts,
            unfurl_links=False,
            unfurl_media=False,
        )

    def start_thread_in_dm(self, text: str, user_id: Optional[str] = None) -> str:
        """
        Create a new root message in DM and return its ts.
        """
        return self.send_dm(text=text, thread_ts=None, user_id=user_id)

    def reply_in_thread(
        self,
        text: str,
        thread_ts: str,
        user_id: Optional[str] = None,
    ) -> str:
        """
        Reply in an existing DM thread.
        """
        return self.send_dm(text=text, thread_ts=thread_ts, user_id=user_id)

    def send_to_channel(
        self,
        channel_id: str,
        text: str,
        thread_ts: Optional[str] = None,
    ) -> str:
        """
        Send message to public/private channel or group DM by channel id.
        """
        return self.send_message(
            channel_id=channel_id,
            text=text,
            thread_ts=thread_ts,
            unfurl_links=False,
            unfurl_media=False,
        )

    def update_message(
        self,
        *,
        channel_id: str,
        ts: str,
        text: str,
    ) -> str:
        """
        Update existing Slack message. Returns ts.
        """
        try:
            resp = self.client.chat_update(
                channel=channel_id,
                ts=ts,
                text=text,
            )
        except SlackApiError as exc:
            logger.exception("Failed to update message %s in %s", ts, channel_id)
            raise SlackWorkerError(f"Failed to update Slack message: {exc}") from exc

        updated_ts = resp.get("ts")
        if not updated_ts:
            raise SlackWorkerError("Slack update response does not contain ts")

        return updated_ts

    def add_reaction(
        self,
        *,
        channel_id: str,
        ts: str,
        emoji_name: str,
    ) -> None:
        """
        Add emoji reaction, example emoji_name='white_check_mark'
        """
        try:
            self.client.reactions_add(
                channel=channel_id,
                timestamp=ts,
                name=emoji_name,
            )
        except SlackApiError as exc:
            logger.exception("Failed to add reaction %s to %s", emoji_name, ts)
            raise SlackWorkerError(f"Failed to add reaction: {exc}") from exc

    def upload_text_snippet(
        self,
        *,
        title: str,
        content: str,
        channel_id: Optional[str] = None,
    ) -> Any:
        """
        Upload text snippet. Returns raw Slack response.
        """
        try:
            return self.client.files_upload_v2(
                channel=channel_id,
                title=title,
                content=content,
                filename=f"{title}.txt",
            )
        except SlackApiError as exc:
            logger.exception("Failed to upload snippet %s", title)
            raise SlackWorkerError(f"Failed to upload snippet: {exc}") from exc

    def test_auth(self) -> dict[str, Any]:
        """
        Useful for diagnostics.
        """
        try:
            resp = self.client.auth_test()
            return resp.data  # type: ignore
        except SlackApiError as exc:
            logger.exception("Slack auth_test failed")
            raise SlackWorkerError(f"Slack auth_test failed: {exc}") from exc
    
    def get_thread_ts(self, event: dict[str, Any]) -> str:
        """
        Return thread_ts for replying in the same thread.
        If message is not in a thread, returns root message ts.
        """
        thread_ts = event.get("thread_ts")
        if thread_ts:
            return thread_ts

        ts = event.get("ts")
        if not ts:
            raise SlackWorkerError("Event does not contain ts or thread_ts")

        return ts

    def send_event_reply(self, event: dict[str, Any], text: str) -> str:
        """
        Reply in the same conversation and same thread as incoming event.
        Works for DM, channel, group DM.
        """
        channel_id = event.get("channel")
        if not channel_id:
            raise SlackWorkerError("Event does not contain channel")

        thread_ts = self.get_thread_ts(event)
        return self.send_message(
            channel_id=channel_id,
            text=text,
            thread_ts=thread_ts,
            unfurl_links=False,
            unfurl_media=False,
        )

    def send_event_root_reply(self, event: dict[str, Any], text: str) -> str:
        """
        Send reply in the same conversation but NOT in thread.
        Usually not what you need for exp# handler, but useful sometimes.
        """
        channel_id = event.get("channel")
        if not channel_id:
            raise SlackWorkerError("Event does not contain channel")

        return self.send_message(
            channel_id=channel_id,
            text=text,
            thread_ts=None,
            unfurl_links=False,
            unfurl_media=False,
        )

    def update_event_reply(self, event: dict[str, Any], ts: str, text: str) -> str:
        """
        Update a previously sent message in the same channel as event.
        """
        channel_id = event.get("channel")
        if not channel_id:
            raise SlackWorkerError("Event does not contain channel")

        return self.update_message(channel_id=channel_id, ts=ts, text=text)

    def format_table_for_slack(
        self,
        table: Any,
        max_len: int = 3500,
    ) -> tuple[str, bool]:
        """
        Format DataFrame / list[dict] / plain object as Slack-friendly text.
        Returns (formatted_text, is_truncated).
        """
        rendered = ""

        if pd is not None and isinstance(table, pd.DataFrame):
            if table.empty:
                rendered = "Пустая таблица"
            else:
                rendered = table.to_string(index=False)
        elif isinstance(table, list):
            if not table:
                rendered = "Пустая таблица"
            else:
                # list[dict] -> simple aligned text
                if all(isinstance(row, dict) for row in table):
                    headers = list({k for row in table for k in row.keys()})
                    rows = [[str(row.get(h, "")) for h in headers] for row in table]
                    widths = [
                        max(len(str(h)), *(len(r[i]) for r in rows))
                        for i, h in enumerate(headers)
                    ]
                    header_line = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
                    sep_line = "-+-".join("-" * widths[i] for i in range(len(headers)))
                    row_lines = [
                        " | ".join(r[i].ljust(widths[i]) for i in range(len(headers)))
                        for r in rows
                    ]
                    rendered = "\n".join([header_line, sep_line, *row_lines])
                else:
                    rendered = "\n".join(str(x) for x in table)
        else:
            rendered = str(table)

        truncated = False
        if len(rendered) > max_len:
            rendered = rendered[:max_len] + "\n... <truncated>"
            truncated = True

        return f"```{rendered}```", truncated

    def upload_csv_file(
        self,
        *,
        title: str,
        content: str,
        channel_id: str,
        thread_ts: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Any:
        """
        Upload CSV file to Slack.
        content: CSV as string.
        """
        filename = filename or f"{title}.csv"

        try:
            return self.client.files_upload_v2(
                channel=channel_id,
                thread_ts=thread_ts,
                title=title,
                filename=filename,
                content=content,
            )
        except SlackApiError as exc:
            logger.exception("Failed to upload CSV file %s", filename)
            raise SlackWorkerError(f"Failed to upload CSV file: {exc}") from exc
