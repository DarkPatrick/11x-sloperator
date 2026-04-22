import os
import logging
from typing import Optional, Any

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
