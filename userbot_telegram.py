"""
TDbridge Userbot — Telegram (Telethon) adapter

The ONLY module that knows about Telethon. It owns the user-account client and
exposes a small, gateway-agnostic surface:

  * start(code_provider)     — connect and ensure login (via userbot_login).
  * on_bot_message(callback) — register a handler called for each incoming
                               message authored by a bot OTHER than our sibling
                               (config.gw_bot_username). Human messages and our
                               sibling's messages are ignored.
  * send_text(...)           — post a message into a group as the user account.
  * add_reaction(...)        — react to a message as the user account.
  * edit_text(...)           — edit one of our messages.
  * delete_messages(...)     — delete message(s).
  * download_attachment(msg) — fetch a message's media bytes.
  * run_until_disconnected() / stop()

It imports userbot_login (one direction — no cycle: userbot_login never imports
this module; it operates on the client object passed to it). It does NOT import
userbot_gateway or userbot_bridge. Routing, attribution, and mapping are not its
concern; it is a pure Telegram adapter.
"""

import logging
from typing import Awaitable, Callable, Optional

from telethon import TelegramClient, events
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

import userbot_login

logger = logging.getLogger("userbot_telegram")

# A message handler: async callable receiving a normalized dict (see
# _normalize_message) for each relayable bot message.
MessageHandler = Callable[[dict], Awaitable[None]]


class UserbotTelegram:
    """Owns the Telethon user-account client and its read/write operations."""

    def __init__(self, *, session_file: str, api_id: int, api_hash: str,
                 phone: str, gw_bot_username: str, two_fa_password: str = "",
                 login_code_wait_min: int = 15, login_code_max_retries: int = 3):
        self._client = TelegramClient(session_file, api_id, api_hash)
        self._phone = phone
        self._two_fa_password = two_fa_password
        self._login_code_wait_min = login_code_wait_min
        self._login_code_max_retries = login_code_max_retries
        # The sibling bot whose messages we must NOT relay (denylist-of-one).
        # Compared case-insensitively, without a leading '@'.
        self._gw_bot_username = (gw_bot_username or "").lstrip("@").lower()
        self._message_handler: Optional[MessageHandler] = None
        # Cache of our own account id, filled after login, so we never relay our
        # own posts (defensive; our posts aren't from a bot anyway).
        self._me_id: Optional[int] = None

    # ----------------------------------------------------------------- #
    # Lifecycle                                                          #
    # ----------------------------------------------------------------- #
    async def start(self, code_provider) -> None:
        """Connect and ensure the session is authorized.

        `code_provider` is an async callable returning the login code; it is
        only invoked if there is no valid saved session. See userbot_login.
        """
        await self._client.connect()
        await userbot_login.ensure_logged_in(
            self._client,
            code_provider,
            phone=self._phone,
            two_fa_password=self._two_fa_password,
            code_wait_min=self._login_code_wait_min,
            max_retries=self._login_code_max_retries,
        )
        me = await self._client.get_me()
        self._me_id = getattr(me, "id", None)
        # Register the incoming-message handler now that we're authorized.
        self._client.add_event_handler(
            self._raw_new_message, events.NewMessage(incoming=True)
        )

    async def run_until_disconnected(self) -> None:
        await self._client.run_until_disconnected()

    async def stop(self) -> None:
        try:
            await self._client.disconnect()
        except Exception:
            pass

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected()

    # ----------------------------------------------------------------- #
    # Inbound: register the relayable-bot-message handler                #
    # ----------------------------------------------------------------- #
    def on_bot_message(self, callback: MessageHandler) -> None:
        """Register the async callback invoked for each relayable bot message."""
        self._message_handler = callback

    async def _raw_new_message(self, event) -> None:
        """Telethon event entry point. Filters to bot messages (not our sibling,
        not ourselves) and dispatches a normalized dict to the handler."""
        if self._message_handler is None:
            return
        try:
            sender = await event.get_sender()
        except Exception as e:
            logger.debug("could not resolve sender: %s", e)
            return

        # Must be a bot.
        if not getattr(sender, "bot", False):
            return
        # Must not be our sibling (the gateway's own TDbridge bot).
        uname = (getattr(sender, "username", "") or "").lower()
        if uname and uname == self._gw_bot_username:
            return
        # Defensive: never relay our own account's messages.
        if self._me_id is not None and getattr(sender, "id", None) == self._me_id:
            return

        try:
            norm = await self._normalize_message(event, sender)
        except Exception as e:
            logger.warning("failed to normalize message: %s", e)
            return
        await self._message_handler(norm)

    async def _normalize_message(self, event, sender) -> dict:
        """Turn a Telethon message event into a plain dict the bridge maps to a
        gateway envelope. No gateway knowledge here — just Telegram facts."""
        msg = event.message
        chat_id = event.chat_id
        # Reply target (Telegram message id) if this is a reply.
        reply_to = None
        if msg.reply_to and getattr(msg.reply_to, "reply_to_msg_id", None):
            reply_to = msg.reply_to.reply_to_msg_id
        # Attribution name for the bot author.
        sender_name = (
            getattr(sender, "username", None)
            or getattr(sender, "first_name", None)
            or "bot"
        )
        # Media presence (bytes fetched lazily by the bridge via
        # download_attachment, mirroring TDbridge's fetch-once discipline).
        has_media = msg.media is not None
        return {
            "chat_id": chat_id,
            "message_id": msg.id,
            "sender_name": sender_name,
            "sender_username": getattr(sender, "username", None),
            "text": msg.message or "",
            "reply_to": reply_to,
            "has_media": has_media,
            "media_filename": self._guess_media_filename(msg),
            "_event": event,   # retained so the bridge can download media
        }

    @staticmethod
    def _guess_media_filename(msg) -> Optional[str]:
        """Best-effort filename for a message's media (for the attachment)."""
        try:
            if msg.file is not None:
                return msg.file.name or (
                    f"attachment{msg.file.ext or ''}" if msg.file.ext else "attachment"
                )
        except Exception:
            pass
        return None

    # ----------------------------------------------------------------- #
    # Outbound actions (as the user account)                             #
    # ----------------------------------------------------------------- #
    async def send_text(self, chat_id, text: str,
                        reply_to: Optional[int] = None) -> int:
        """Post a text message; return the new Telegram message id."""
        sent = await self._client.send_message(
            int(chat_id), text or "", reply_to=reply_to
        )
        return sent.id

    async def send_file(self, chat_id, file_bytes: bytes, *,
                        filename: Optional[str] = None,
                        caption: str = "", reply_to: Optional[int] = None,
                        force_document: bool = False) -> int:
        """Post a file (bytes) with optional caption; return new message id."""
        import io
        bio = io.BytesIO(file_bytes)
        if filename:
            bio.name = filename
        sent = await self._client.send_file(
            int(chat_id), bio, caption=caption or None,
            reply_to=reply_to, force_document=force_document,
        )
        # send_file may return a list for albums; we send one file → one msg.
        if isinstance(sent, list):
            return sent[0].id if sent else 0
        return sent.id

    async def edit_text(self, chat_id, message_id: int, new_text: str) -> None:
        await self._client.edit_message(int(chat_id), int(message_id),
                                        new_text or "")

    async def delete_messages(self, chat_id, message_ids) -> None:
        ids = [int(m) for m in message_ids]
        await self._client.delete_messages(int(chat_id), ids)

    async def add_reaction(self, chat_id, message_id: int, emoji: str) -> None:
        """Add a single native emoji reaction to a message. Empty emoji clears."""
        reactions = [ReactionEmoji(emoticon=emoji)] if emoji else []
        await self._client(SendReactionRequest(
            peer=int(chat_id),
            msg_id=int(message_id),
            reaction=reactions,
        ))

    async def download_attachment(self, event) -> Optional[bytes]:
        """Download a message's media as bytes (None if no media / on failure)."""
        try:
            return await self._client.download_media(event.message, file=bytes)
        except Exception as e:
            logger.warning("attachment download failed: %s", e)
            return None
