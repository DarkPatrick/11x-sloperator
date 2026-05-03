import os
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional, Any

from cryptography.fernet import Fernet

from clickhouse_worker import _get_client, execute_sql_modify

logger = logging.getLogger(__name__)


CONVERSATIONS_TABLE = "sandbox.ug_monetization_sloperator_conversations"
HEARTBEAT_TABLE = "sandbox.ug_monetization_sloperator_heartbeat"


class ConversationStore:
    def __init__(self) -> None:
        self.cipher = Fernet(os.environ["ENCRYPTION_KEY"].encode())
        self.bot_user_id = os.environ.get("SLACK_BOT_ID", "")

    def init_tables(self) -> None:
        execute_sql_modify(f"""
        CREATE TABLE IF NOT EXISTS {CONVERSATIONS_TABLE} on cluster ug_core
        (
            channel_id String,
            channel_type String,
            thread_ts String,
            message_ts String,
            message_dt DateTime('UTC'),

            user_id String,
            user_name String,

            is_bot UInt8,
            bot_user_id String,

            reply_to_message_ts String,

            text_encrypted String,
            text_sha256 String,

            created_at DateTime('UTC') DEFAULT now('UTC')
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(message_dt)
        ORDER BY (channel_id, thread_ts, message_ts)
        """)

        execute_sql_modify(f"""
        CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} on cluster ug_core
        (
            ts DateTime('UTC'),
            created_at DateTime('UTC') DEFAULT now('UTC')
        )
        ENGINE = MergeTree
        ORDER BY ts
        """)

    def encrypt_text(self, text: str) -> str:
        return self.cipher.encrypt(text.encode("utf-8")).decode("utf-8")

    def decrypt_text(self, encrypted_text: str) -> str:
        return self.cipher.decrypt(encrypted_text.encode("utf-8")).decode("utf-8")

    def text_hash(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def get_channel_type(self, channel_id: str) -> str:
        if channel_id.startswith("D"):
            return "dm"
        if channel_id.startswith("C"):
            return "channel"
        if channel_id.startswith("G"):
            return "private_or_mpim"
        return "unknown"

    def ts_to_utc_dt(self, ts: str) -> datetime:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc)

    def escape_sql(self, value: Optional[str]) -> str:
        if value is None:
            return ""
        return str(value).replace("\\", "\\\\").replace("'", "''")

    def message_exists(self, *, channel_id: str, message_ts: str) -> bool:
        client = _get_client()
        try:
            result = client.query(f"""
            SELECT count()
            FROM {CONVERSATIONS_TABLE}
            WHERE channel_id = '{self.escape_sql(channel_id)}'
              AND message_ts = '{self.escape_sql(message_ts)}'
            """)
            return result.result_rows[0][0] > 0
        finally:
            try:
                client.close()
            except Exception:
                pass

    def save_user_message(self, event: dict[str, Any], user_name: str = "") -> None:
        channel_id = event.get("channel", "")
        message_ts = event.get("ts", "")
        text = event.get("text", "") or ""

        if not channel_id or not message_ts:
            logger.warning("Cannot save message without channel_id/message_ts: %s", event)
            return

        if self.message_exists(channel_id=channel_id, message_ts=message_ts):
            return

        thread_ts = event.get("thread_ts") or message_ts
        message_dt = self.ts_to_utc_dt(message_ts)

        encrypted_text = self.encrypt_text(text)
        text_sha256 = self.text_hash(text)

        execute_sql_modify(f"""
        INSERT INTO {CONVERSATIONS_TABLE}
        (
            channel_id,
            channel_type,
            thread_ts,
            message_ts,
            message_dt,
            user_id,
            user_name,
            is_bot,
            bot_user_id,
            reply_to_message_ts,
            text_encrypted,
            text_sha256
        )
        VALUES
        (
            '{self.escape_sql(channel_id)}',
            '{self.escape_sql(self.get_channel_type(channel_id))}',
            '{self.escape_sql(thread_ts)}',
            '{self.escape_sql(message_ts)}',
            toDateTime('{message_dt.strftime("%Y-%m-%d %H:%M:%S")}', 'UTC'),
            '{self.escape_sql(event.get("user", ""))}',
            '{self.escape_sql(user_name)}',
            0,
            '',
            '',
            '{self.escape_sql(encrypted_text)}',
            '{self.escape_sql(text_sha256)}'
        )
        """)

    def save_bot_message(
        self,
        *,
        channel_id: str,
        message_ts: str,
        text: str,
        thread_ts: Optional[str] = None,
        reply_to_message_ts: Optional[str] = None,
    ) -> None:
        if not channel_id or not message_ts:
            logger.warning("Cannot save bot message without channel_id/message_ts")
            return

        if self.message_exists(channel_id=channel_id, message_ts=message_ts):
            return

        thread_ts = thread_ts or message_ts
        message_dt = self.ts_to_utc_dt(message_ts)

        encrypted_text = self.encrypt_text(text)
        text_sha256 = self.text_hash(text)

        execute_sql_modify(f"""
        INSERT INTO {CONVERSATIONS_TABLE}
        (
            channel_id,
            channel_type,
            thread_ts,
            message_ts,
            message_dt,
            user_id,
            user_name,
            is_bot,
            bot_user_id,
            reply_to_message_ts,
            text_encrypted,
            text_sha256
        )
        VALUES
        (
            '{self.escape_sql(channel_id)}',
            '{self.escape_sql(self.get_channel_type(channel_id))}',
            '{self.escape_sql(thread_ts)}',
            '{self.escape_sql(message_ts)}',
            toDateTime('{message_dt.strftime("%Y-%m-%d %H:%M:%S")}', 'UTC'),
            '{self.escape_sql(self.bot_user_id)}',
            '11x Sloperator',
            1,
            '{self.escape_sql(self.bot_user_id)}',
            '{self.escape_sql(reply_to_message_ts or "")}',
            '{self.escape_sql(encrypted_text)}',
            '{self.escape_sql(text_sha256)}'
        )
        """)

    def write_heartbeat(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        execute_sql_modify(f"""
        INSERT INTO {HEARTBEAT_TABLE} (ts)
        VALUES (toDateTime('{now}', 'UTC'))
        """)

    def get_last_heartbeat_ts(self) -> Optional[float]:
        client = _get_client()
        try:
            result = client.query(f"""
            SELECT max(ts)
            FROM {HEARTBEAT_TABLE}
            """)
            value = result.result_rows[0][0]
            if not value:
                return None

            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)

            return value.timestamp()
        finally:
            try:
                client.close()
            except Exception:
                pass

    def get_unanswered_messages_since_last_heartbeat(self) -> list[dict[str, Any]]:
        last_heartbeat_ts = self.get_last_heartbeat_ts()

        if last_heartbeat_ts is None:
            return []

        oldest_dt = datetime.fromtimestamp(last_heartbeat_ts, tz=timezone.utc)

        client = _get_client()
        try:
            result = client.query(f"""
            SELECT
                u.channel_id,
                u.channel_type,
                u.thread_ts,
                u.message_ts,
                u.message_dt,
                u.user_id,
                u.user_name,
                u.text_encrypted
            FROM {CONVERSATIONS_TABLE} AS u
            LEFT JOIN
            (
                SELECT
                    channel_id,
                    reply_to_message_ts,
                    count() AS replies_count
                FROM {CONVERSATIONS_TABLE}
                WHERE is_bot = 1
                  AND reply_to_message_ts != ''
                GROUP BY
                    channel_id,
                    reply_to_message_ts
            ) AS b
                ON u.channel_id = b.channel_id
               AND u.message_ts = b.reply_to_message_ts
            WHERE u.is_bot = 0
              AND u.message_dt > toDateTime('{oldest_dt.strftime("%Y-%m-%d %H:%M:%S")}', 'UTC')
              AND coalesce(b.replies_count, 0) = 0
            ORDER BY u.message_dt
            """)

            messages = []
            for row in result.result_rows:
                encrypted_text = row[7]
                messages.append({
                    "channel": row[0],
                    "channel_type": row[1],
                    "thread_ts": row[2],
                    "ts": row[3],
                    "message_dt": row[4],
                    "user": row[5],
                    "user_name": row[6],
                    "text": self.decrypt_text(encrypted_text),
                })

            return messages
        finally:
            try:
                client.close()
            except Exception:
                pass
