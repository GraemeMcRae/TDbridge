"""
TDbridge Bot (bot.py)
Telegram ↔ Discord message bridge — main entry point.

Run with:
    python bot.py --env test
    python bot.py --env prod

Architecture
------------
A single Python process hosts two concurrent async event loops:
  - discord.py   — gateway connection to the Discord server
  - python-telegram-bot — webhook server receiving Telegram updates

Both run on the SAME asyncio event loop (discord.py's loop), achieved by
starting the Telegram Application inside an async task after the Discord
client is ready.

Message flow
------------
  Telegram group → on_tg_message() → route_to_discord()  → Discord channel
  Discord channel → on_dc_message() → route_to_telegram() → Telegram group

Reply chain tracking uses the SQLite message store (db.py).
Mapping lookups use the in-memory cache maintained by sheets_manager.py.
"""

import asyncio
import io
import logging
import mimetypes
import os
import sys
from typing import Optional

import aiohttp
import discord
from telegram import (
    Bot as TelegramBot,
    InputFile,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Update,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ContextTypes,
    MessageHandler,
    MessageReactionHandler,
    filters,
)

# config must be imported first — it parses --env and sets up logging
from config import config, localnow
import db
import sheets_manager
from dashboard_reporter import DashboardReporter, status as bot_status
from gateway_server import GatewayServer
from gateway_retry import with_retry, RetryGaveUp
import gateway_protocol as gp
import gateway_ratelimit
import gateway_files

# On Linux (server), Telegram updates are received via webhook — the bot runs
# its own HTTPS server and Telegram POSTs updates to it.
# On Windows (development), webhook mode is not used because the dev machine
# is not reachable at the public server address.  Polling mode is used instead:
# the bot periodically asks Telegram for new updates.  Polling is functionally
# identical for testing purposes; the only difference is the transport layer.
# See TDbridge_Project_Structure.md § "Platform differences" for full details.
#
# config.telegram_use_polling already encodes the full decision: the platform
# default (polling on Windows/WSL, webhook on Linux) plus any explicit
# TELEGRAM_USE_POLLING override from .env.
_USE_POLLING = config.telegram_use_polling

# Explicit list of update types TDbridge needs.
# We do NOT use Update.ALL_TYPES here because:
#   1. Telegram's Bot API excludes message_reaction and message_reaction_count
#      from the default allowed_updates set (and from the set restored by
#      delete_webhook), so we must name them explicitly.
#   2. An explicit list makes it obvious which update types are handled and
#      avoids silently receiving types the bot has no handler for.
_ALLOWED_UPDATES = [
    "message",
    "edited_message",
    "message_reaction",
    "message_reaction_count",
    "callback_query",
    "my_chat_member",   # bot's own status changes (added/promoted/demoted in groups)
]

logger = logging.getLogger(config.bot_name)

# ---------------------------------------------------------------------------
# Telegram poll counting
# ---------------------------------------------------------------------------
# httpx's per-request INFO logging is silenced in config (raised to WARNING) so
# routine "getUpdates ... 200 OK" lines don't flood the log. Instead we count
# poll results directly in our own HTTPXRequest subclass, which sees the real
# HTTP status code. On a non-200 getUpdates result we log a summary of the
# successes since the last anomaly, then log the anomaly itself. The interval
# counters feed the Manager Dashboard polling-health check via
# config.take_poll_counts().
from telegram.request import HTTPXRequest


class _PollCounters:
    """Holds getUpdates poll counts. interval_* are read+reset by the dashboard
    each cycle; run_ok / since track the current run for the summary line."""
    def __init__(self) -> None:
        self.interval_ok: int = 0
        self.interval_err: int = 0
        self.run_ok: int = 0
        self.since: str = localnow().strftime("%Y-%m-%d %H:%M:%S %Z")

    def take_interval_counts(self) -> tuple:
        ok, err = self.interval_ok, self.interval_err
        self.interval_ok = 0
        self.interval_err = 0
        return ok, err


class PollCountingRequest(HTTPXRequest):
    """HTTPXRequest that counts getUpdates poll results.

    Routine successful polls are counted silently. On a non-200 getUpdates
    result (or a request that raises), it logs a one-line summary of the
    successful polls since the last anomaly, then logs the anomaly. This
    replaces the old log-filter approach: we have the real status code here, so
    there's no log-text parsing or logger-propagation subtlety.
    """
    def __init__(self, *args, counters: "_PollCounters", **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._counters = counters

    def _note_success(self) -> None:
        c = self._counters
        c.interval_ok += 1
        c.run_ok += 1

    def _note_error(self, detail: str) -> None:
        c = self._counters
        c.interval_err += 1
        if c.run_ok > 0:
            logger.info(
                "getUpdates: %d successful poll(s) since %s "
                "(summarised; individual 200s not logged)",
                c.run_ok, c.since,
            )
        logger.warning("getUpdates non-success: %s", detail)
        c.run_ok = 0
        c.since = localnow().strftime("%Y-%m-%d %H:%M:%S %Z")

    @staticmethod
    def _endpoint(url: str) -> str:
        """Extract the Telegram API method name (e.g. 'editMessageText') from a
        request URL, for concise log messages. Falls back to the full URL."""
        # URLs look like https://api.telegram.org/bot<token>/<method>
        tail = url.rsplit("/", 1)[-1]
        return tail or url

    async def do_request(self, url, method, *args, **kwargs):
        is_getupdates = "getUpdates" in url
        try:
            result = await super().do_request(url, method, *args, **kwargs)
        except Exception as e:
            if is_getupdates:
                self._note_error(f"{type(e).__name__}: {e}")
            # Non-getUpdates exceptions propagate to the call site, which is
            # responsible for contextual logging; we don't double-log here.
            raise
        # result is (status_code, payload)
        try:
            status_code = result[0]
        except Exception:
            status_code = None
        if is_getupdates:
            if status_code == 200:
                self._note_success()
            else:
                self._note_error(f"HTTP {status_code}")
        elif status_code != 200:
            # Catch-all backstop: httpx's own per-request logging is silenced,
            # so without this a failed API call (e.g. editMessageText 400) would
            # be invisible at the transport layer. Log any non-200 at WARNING so
            # no API failure is ever silent, even if a call site lacks its own
            # error handling. (200s stay silent — that's the whole point.)
            logger.warning(
                "Telegram API non-200: %s → HTTP %s",
                self._endpoint(url), status_code,
            )
        return result


def log_poll_summary(counters: "_PollCounters", reason: str = "shutdown") -> None:
    """Emit a final summary of successful getUpdates polls (e.g. at shutdown)."""
    if counters.run_ok > 0:
        logger.info(
            "getUpdates: %d successful poll(s) since %s (polling stopped: %s)",
            counters.run_ok, counters.since, reason,
        )
        counters.run_ok = 0


# ---------------------------------------------------------------------------
# Discord intents
# ---------------------------------------------------------------------------
_intents = discord.Intents.default()
_intents.message_content = True
_intents.members = True
_intents.reactions = True
_intents.guilds = True

# ---------------------------------------------------------------------------
# Module-level references set during startup
# ---------------------------------------------------------------------------
_discord_client: Optional[discord.Client] = None
_tg_app: Optional[Application] = None
_poll_counters: Optional["_PollCounters"] = None
_sheets_refresh_task: Optional[asyncio.Task] = None
_db_purge_task: Optional[asyncio.Task] = None
_discord_refresh_task: Optional[asyncio.Task] = None
_dashboard_task: Optional[asyncio.Task] = None
_t_group_flush_task: Optional[asyncio.Task] = None

# Guard so _startup() runs only once even if Discord's on_ready fires again
# after a reconnect (which it can). A reconnect should not re-initialize.
_startup_done: bool = False

# Dashboard reporter — emits Status Report log lines every 30 minutes
_dashboard_reporter = DashboardReporter(config, bot_status)

# Gateway server — serves this instance's OWN_GATEWAY (no-op if client-only).
_gateway_server = GatewayServer(config)


# ===========================================================================
# Utility helpers
# ===========================================================================

# All ID values from the Telegram and Discord APIs arrive as Python ints.
# They must be converted to strings immediately at the point of receipt so
# that every subsequent comparison, cache lookup, and database operation
# works with strings only.  Google Sheets stores these IDs as Text columns
# and returns them as Python strings via UNFORMATTED_VALUE, so the types
# will agree as long as we stringify at the API boundary.

def _tg_group_id_str(chat_id) -> str:
    return str(chat_id)


def _dc_channel_id_str(channel_id) -> str:
    return str(channel_id)


def _dc_msg_id_str(msg_id) -> str:
    return str(msg_id)


def _tg_msg_id_str(msg_id) -> str:
    return str(msg_id)


async def _download_tg_file(bot: TelegramBot, file_id: str) -> bytes:
    """Download a Telegram file by file_id and return raw bytes."""
    tg_file = await bot.get_file(file_id)
    buf = io.BytesIO()
    await tg_file.download_to_memory(buf)
    buf.seek(0)
    return buf.read()


# MIME types where Python's mimetypes.guess_extension() returns a less common
# or undesirable extension; we override these for friendlier, widely-recognised
# extensions that Discord and mobile OSes render/play correctly.
_MIME_EXT_OVERRIDES = {
    "image/jpeg": ".jpg",      # guess_extension gives ".jpe"
    "image/jpg": ".jpg",
    "video/quicktime": ".mov",
    "video/mp4": ".mp4",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/ogg": ".ogg",
    "image/webp": ".webp",
}


def _ext_from_mime(mime_type: str) -> str:
    """Return a leading-dot file extension for a MIME type, or "" if unknown.
    Generic/empty MIME types (e.g. application/octet-stream) yield ""."""
    if not mime_type:
        return ""
    mime_type = mime_type.split(";", 1)[0].strip().lower()
    if mime_type in ("application/octet-stream", "binary/octet-stream"):
        return ""
    if mime_type in _MIME_EXT_OVERRIDES:
        return _MIME_EXT_OVERRIDES[mime_type]
    ext = mimetypes.guess_extension(mime_type) or ""
    return ext


def _ensure_filename(
    raw_name: str,
    mime_type: str,
    default_stem: str,
    default_ext: str,
) -> str:
    """Build a Discord-friendly filename that always has a sensible extension.

    Telegram does not always supply a file_name (notably for forwarded videos,
    which often arrive as a Document with no name and a generic MIME type). A
    filename without an extension makes Discord store the file as
    'application.octet-stream', which won't render or play. This derives a name
    with a real extension using, in order of preference:
      1. the provided file_name, IF it already has an extension;
      2. the file_name's stem plus an extension inferred from the MIME type;
      3. the MIME-inferred extension on the default stem;
      4. the type-based default extension on the default stem.
    """
    raw_name = (raw_name or "").strip()
    stem, existing_ext = os.path.splitext(raw_name)

    if raw_name and existing_ext:
        # Already has a usable extension — keep as-is.
        return raw_name

    mime_ext = _ext_from_mime(mime_type)

    if raw_name and not existing_ext:
        # Have a name but no extension — append the best extension we can find.
        return raw_name + (mime_ext or default_ext)

    # No name at all — use MIME-inferred extension if available, else the
    # type-based default, on the default stem.
    return default_stem + (mime_ext or default_ext)


def _resolve_discord_mentions(text: str) -> str:
    """Replace Discord @user and #channel mention tokens with readable names.

    Discord encodes mentions as:
        <@discord_user_id>    — user mention
        <@!discord_user_id>   — user mention (legacy nickname variant)
        <@&role_id>           — role mention
        <#discord_channel_id> — channel mention

    On the Telegram side these raw snowflake IDs are meaningless.  This
    function replaces each token with a human-readable pseudo-mention looked
    up from the in-memory Sheets cache:

        <@450693109496545280>      →  @Boont
        <#1509669940125241354>     →  # dispatch-test

    Name priority for user mentions: D_Nickname → D_DisplayName → D_UserName.
    If the ID is not found in the cache the token is left unchanged so no
    information is lost.
    """
    import re

    def _resolve_user(match: re.Match) -> str:
        uid = match.group(1)
        row = sheets_manager.get_user_by_discord_id(uid)
        if not row:
            return match.group(0)  # leave unchanged if not found
        name = (
            str(row.get("D_Nickname", "")).strip()
            or str(row.get("D_DisplayName", "")).strip()
            or str(row.get("D_UserName", "")).strip()
            or uid
        )
        # Space after @ prevents Telegram from treating this as a real mention
        return f"@ {name}"

    def _resolve_channel(match: re.Match) -> str:
        cid = match.group(1)
        row = sheets_manager.get_channel(cid)
        if not row:
            return match.group(0)  # leave unchanged if not found
        name = str(row.get("D_ChannelName", "")).strip() or cid
        # Space after # prevents Telegram from treating this as a channel link
        return f"# {name}"

    def _resolve_role(match: re.Match) -> str:
        # Role mentions are <@&role_id>; D_ID is stored as "&role_id"
        rid = f"&{match.group(1)}"
        row = sheets_manager.get_user_by_discord_id(rid)
        if not row:
            return match.group(0)  # leave unchanged if not found
        name = str(row.get("D_Nickname", "")).strip() or match.group(1)
        return f"@ {name}"

    # <@&id> — role mentions (must be checked before <@!?id> to avoid partial match)
    text = re.sub(r"<@&(\d+)>", _resolve_role, text)
    # <@!id> (legacy) and <@id> (current) — user mentions
    text = re.sub(r"<@!?(\d+)>", _resolve_user, text)
    # <#id> — channel mentions
    text = re.sub(r"<#(\d+)>", _resolve_channel, text)
    return text


async def _get_discord_channel(channel_id: str) -> Optional[discord.TextChannel]:
    """Return a Discord TextChannel object by ID, or None."""
    if _discord_client is None:
        return None
    ch = _discord_client.get_channel(int(channel_id))
    if ch is None:
        try:
            ch = await _discord_client.fetch_channel(int(channel_id))
        except Exception:
            ch = None
    return ch


async def _get_discord_webhook(channel: discord.TextChannel) -> Optional[discord.Webhook]:
    """Return (or create) a webhook named 'TDbridge' in the given channel."""
    try:
        hooks = await channel.webhooks()
        for hook in hooks:
            if hook.name == "TDbridge":
                return hook
        # Create one if it doesn't exist
        hook = await channel.create_webhook(name="TDbridge")
        logger.info(f"Created webhook 'TDbridge' in channel #{channel.name}")
        return hook
    except Exception as e:
        logger.error(f"Failed to get/create webhook in #{channel.name}: {e}")
        return None


async def _send_to_discord(
    channel: discord.TextChannel,
    webhook: Optional[discord.Webhook],
    sender_name: str,
    content: str,
    files: list,
    reference: Optional[discord.MessageReference],
) -> Optional[discord.Message]:
    """Send a message to Discord, choosing the right API based on context.

    Discord webhooks support custom display names and avatars, which is how
    we show "Alice [TG]" instead of the bot's own name.  However, webhooks
    do NOT support the reply reference parameter — that is only available on
    the regular channel send API.

    Strategy:
      - Reply (reference is set): use channel.send() with reference.
        The message appears as a threaded reply.  The sender attribution is
        included in the message content (e.g. "👤 Alice [TG]: text") so the
        source is still clear even though the Discord display name will be
        the bot's own name.
      - New message (no reference): use webhook.send() with custom username.
        This shows the sender name in the message header natively.

    Returns the sent Message object, or None on failure.
    """
    try:
        if reference is not None:
            # Reply: must use channel.send() — webhook.send() has no reference param
            dc_msg = await with_retry(
                f"DC reply send #{channel.name}",
                lambda: channel.send(
                    content=content,
                    files=files if files else discord.utils.MISSING,
                    reference=reference,
                    mention_author=False,
                ),
                platform="discord",
            )
        elif webhook is not None:
            # New message: use webhook for custom display name
            dc_msg = await with_retry(
                f"DC webhook send #{channel.name}",
                lambda: webhook.send(
                    content=content,
                    username=f"{sender_name} [TG]",
                    files=files if files else discord.utils.MISSING,
                    wait=True,
                ),
                platform="discord",
            )
        else:
            # Fallback: no webhook available, use plain channel send
            dc_msg = await with_retry(
                f"DC channel send #{channel.name}",
                lambda: channel.send(
                    content=content,
                    files=files if files else discord.utils.MISSING,
                ),
                platform="discord",
            )
        return dc_msg
    except RetryGaveUp as e:
        logger.error(f"Failed to send to Discord #{channel.name} after retries: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to send to Discord #{channel.name}: {e}")
        return None


# ===========================================================================
# Attachment helpers
# ===========================================================================

DC_MAX_BYTES = 25 * 1024 * 1024   # Discord free-tier upload limit
TG_MAX_BYTES = 50 * 1024 * 1024   # Telegram upload limit


async def _warn_attachment_failure(
    reason: str,
    filename: str,
    attach_type: str,
    direction: str,
    tg_bot: Optional[TelegramBot] = None,
    tg_chat_id: Optional[int] = None,
    tg_reply_to: Optional[int] = None,
    dc_channel: Optional[discord.TextChannel] = None,
    dc_msg_ref: Optional[discord.MessageReference] = None,
) -> None:
    """Log and post a bilingual warning when an attachment cannot be bridged.

    Args:
        reason:      Human-readable reason (e.g. "file too large (52 MB > 25 MB limit)")
        filename:    Original filename of the attachment
        attach_type: Type description (e.g. "photo", "video", "document")
        direction:   "TG→DC" or "DC→TG"
        tg_bot:      Telegram bot instance (to post reply on Telegram side)
        tg_chat_id:  Telegram chat to post warning in
        tg_reply_to: Telegram message ID to reply to
        dc_channel:  Discord channel to post warning in
        dc_msg_ref:  Discord message reference to reply to
    """
    warn_text = (
        f"⚠️ Attachment could not be bridged ({direction}): {reason}. "
        f"File: {filename!r} (type: {attach_type})"
    )
    logger.warning(warn_text)

    if tg_bot and tg_chat_id:
        try:
            await tg_bot.send_message(
                chat_id=tg_chat_id,
                text=warn_text,
                reply_to_message_id=tg_reply_to,
            )
        except Exception as e:
            logger.warning(f"Could not post attachment warning to Telegram: {e}")

    if dc_channel:
        try:
            await dc_channel.send(
                content=warn_text,
                reference=dc_msg_ref,
                mention_author=False,
            )
        except Exception as e:
            logger.warning(f"Could not post attachment warning to Discord: {e}")


async def _collect_tg_attachments(
    msg,
    tg_bot: TelegramBot,
) -> tuple[list[discord.File], list[str]]:
    """Download all attachments from a Telegram message.

    Returns:
        (dc_files, skip_notices) where dc_files is a list of discord.File
        objects ready to upload, and skip_notices is a list of plain-text
        descriptions of any attachments that were skipped (for appending to
        the message content).

    Handles: photo, video, voice, audio, document, sticker, poll.
    A Telegram message can contain at most one media item (plus caption),
    so this returns at most one file in normal usage.  The list form is used
    for symmetry with the Discord side and for future compatibility.
    """
    dc_files: list[discord.File] = []
    skip_notices: list[str] = []

    try:
        if msg.photo:
            photo = msg.photo[-1]  # largest resolution
            data = await _download_tg_file(tg_bot, photo.file_id)
            dc_files.append(discord.File(io.BytesIO(data), filename="photo.jpg"))

        elif msg.video:
            data = await _download_tg_file(tg_bot, msg.video.file_id)
            fname = _ensure_filename(
                msg.video.file_name, msg.video.mime_type,
                default_stem="video", default_ext=".mp4",
            )
            dc_files.append(discord.File(io.BytesIO(data), filename=fname))

        elif msg.voice:
            data = await _download_tg_file(tg_bot, msg.voice.file_id)
            dc_files.append(discord.File(io.BytesIO(data), filename="voice.ogg"))

        elif msg.audio:
            data = await _download_tg_file(tg_bot, msg.audio.file_id)
            fname = _ensure_filename(
                msg.audio.file_name, msg.audio.mime_type,
                default_stem="audio", default_ext=".mp3",
            )
            dc_files.append(discord.File(io.BytesIO(data), filename=fname))

        elif msg.document:
            # Forwarded videos frequently arrive as a Document with no
            # file_name and a generic MIME type; derive a proper name+extension
            # so Discord doesn't store it as 'application.octet-stream'.
            fname = _ensure_filename(
                msg.document.file_name, msg.document.mime_type,
                default_stem="file", default_ext="",
            )
            fsize = msg.document.file_size or 0
            if fsize > DC_MAX_BYTES:
                skip_notices.append(
                    f"[Attachment skipped — file too large "
                    f"({fsize // (1024*1024)} MB > {DC_MAX_BYTES // (1024*1024)} MB Discord limit): "
                    f"{fname}]"
                )
            else:
                data = await _download_tg_file(tg_bot, msg.document.file_id)
                dc_files.append(discord.File(io.BytesIO(data), filename=fname))

        elif msg.sticker:
            if msg.sticker.is_animated or msg.sticker.is_video:
                emoji = msg.sticker.emoji or "🎭"
                skip_notices.append(f"[Sticker: {emoji}]")
            else:
                try:
                    data = await _download_tg_file(tg_bot, msg.sticker.file_id)
                    dc_files.append(discord.File(io.BytesIO(data), filename="sticker.webp"))
                except Exception:
                    emoji = msg.sticker.emoji or "🎭"
                    skip_notices.append(f"[Sticker: {emoji}]")

        elif msg.poll:
            options = " | ".join(o.text for o in msg.poll.options)
            skip_notices.append(f"[Poll: **{msg.poll.question}** | Options: {options}]")

    except Exception as e:
        logger.warning(f"TG→DC attachment download failed: {e}")
        skip_notices.append("[Attachment could not be downloaded]")

    return dc_files, skip_notices


class _GatewayAttachment:
    """Adapter that presents gateway-file bytes with the same interface as a
    discord.Attachment (.filename, .content_type, .size, async .read()), so the
    existing _send_attachments_to_telegram() can be reused unchanged for the
    inbound gateway path. The bytes are already local (read from our file store),
    so read() just returns them.
    """
    def __init__(self, data: bytes, filename: str, content_type: str, size: int = 0):
        self._data = data
        self.filename = filename or "attachment"
        self.content_type = content_type or ""
        self.size = size or len(data)
        self.url = ""  # gateway bytes are local; no CDN URL

    async def read(self) -> bytes:
        return self._data


async def _send_attachments_to_telegram(
    attachments: list,
    text: str,
    tg_bot: TelegramBot,
    tg_group_id: str,
    reply_to_telegram_id: Optional[int],
    dc_channel: discord.TextChannel,
    dc_msg_ref: Optional[discord.MessageReference],
) -> list[int]:
    """Upload all Discord attachments to Telegram, returning sent message IDs.

    Sends photos and videos as a media group (album) when there are multiple,
    falling back to individual sends for documents, audio, and voice.
    Files exceeding Telegram's 50 MB limit are skipped with a warning posted
    on both platforms.

    Returns a list of Telegram message IDs for all successfully sent messages.
    """
    from telegram import InputMediaPhoto, InputMediaVideo

    tg_msg_ids: list[int] = []
    chat_id = int(tg_group_id)

    # Partition attachments by type
    photos_videos: list[tuple] = []   # (InputMedia, discord.Attachment)
    documents: list = []              # discord.Attachment

    for att in attachments:
        fname = att.filename
        ctype = att.content_type or ""
        size  = att.size or 0

        if size > TG_MAX_BYTES:
            await _warn_attachment_failure(
                reason=f"file too large ({size // (1024*1024)} MB > {TG_MAX_BYTES // (1024*1024)} MB Telegram limit)",
                filename=fname,
                attach_type=ctype or "file",
                direction="DC→TG",
                tg_bot=tg_bot,
                tg_chat_id=chat_id,
                tg_reply_to=reply_to_telegram_id,
                dc_channel=dc_channel,
                dc_msg_ref=dc_msg_ref,
            )
            continue

        try:
            data = await att.read()
        except Exception as e:
            await _warn_attachment_failure(
                reason=f"download failed: {e}",
                filename=fname,
                attach_type=ctype or "file",
                direction="DC→TG",
                tg_bot=tg_bot,
                tg_chat_id=chat_id,
                tg_reply_to=reply_to_telegram_id,
                dc_channel=dc_channel,
                dc_msg_ref=dc_msg_ref,
            )
            continue

        # Store (raw_bytes, InputFile_for_documents, attachment) so we can
        # pass raw bytes to InputMediaPhoto/InputMediaVideo (required by the
        # Telegram Bot API — InputFile is not accepted there) while still
        # having an InputFile available for send_document which needs filename.
        input_file = InputFile(io.BytesIO(data), filename=fname)

        if ctype.startswith("image/"):
            photos_videos.append((data, att))   # (bytes, discord.Attachment)
        elif ctype.startswith("video/"):
            photos_videos.append((data, att))
        else:
            documents.append((input_file, att))

    # Send photos/videos as a media group (album) — up to 10 per group.
    # Caption goes on the first item only (Telegram album convention).
    # InputMediaPhoto/InputMediaVideo require raw bytes or a URL, NOT InputFile.
    if photos_videos:
        for chunk_start in range(0, len(photos_videos), 10):
            chunk_pv = photos_videos[chunk_start:chunk_start + 10]
            chunk_caption = text if chunk_start == 0 else None

            if len(chunk_pv) == 1:
                # Single item — use the regular send method, not send_media_group.
                # Wrap bytes in InputFile WITH the Discord filename so Telegram
                # stores a proper name+extension. Passing raw bytes alone makes
                # Telegram fall back to naming the file after its MIME type
                # (e.g. "application.octet-stream"), which then surfaces as a
                # broken filename when the message is forwarded or bridged back.
                raw, att = chunk_pv[0]
                ctype_att = att.content_type or ""
                _fname = att.filename or "attachment"
                try:
                    if ctype_att.startswith("video/"):
                        sent = await tg_bot.send_video(
                            chat_id=chat_id,
                            video=InputFile(io.BytesIO(raw), filename=_fname),
                            caption=chunk_caption or None,
                            reply_to_message_id=reply_to_telegram_id,
                        )
                    else:
                        sent = await tg_bot.send_photo(
                            chat_id=chat_id,
                            photo=InputFile(io.BytesIO(raw), filename=_fname),
                            caption=chunk_caption or None,
                            reply_to_message_id=reply_to_telegram_id,
                        )
                    tg_msg_ids.append(sent.message_id)
                except Exception as e:
                    await _warn_attachment_failure(
                        reason=f"send failed: {e}",
                        filename=att.filename,
                        attach_type=ctype_att or "image/video",
                        direction="DC→TG",
                        tg_bot=tg_bot, tg_chat_id=chat_id,
                        tg_reply_to=reply_to_telegram_id,
                        dc_channel=dc_channel, dc_msg_ref=dc_msg_ref,
                    )
            else:
                # Multiple items — build InputMedia list with raw bytes.
                # Pass filename= so Telegram stores a proper name+extension for
                # each album item (otherwise it names them after the MIME type,
                # e.g. "application.octet-stream").
                media_list = []
                for i, (raw, att) in enumerate(chunk_pv):
                    ctype_att = att.content_type or ""
                    caption = chunk_caption if i == 0 else None
                    _fname = att.filename or "attachment"
                    if ctype_att.startswith("video/"):
                        media_list.append(
                            InputMediaVideo(media=raw, caption=caption, filename=_fname)
                        )
                    else:
                        media_list.append(
                            InputMediaPhoto(media=raw, caption=caption, filename=_fname)
                        )
                try:
                    sent_list = await tg_bot.send_media_group(
                        chat_id=chat_id,
                        media=media_list,
                        reply_to_message_id=reply_to_telegram_id,
                    )
                    tg_msg_ids.extend(m.message_id for m in sent_list)
                except Exception as e:
                    for raw, att in chunk_pv:
                        await _warn_attachment_failure(
                            reason=f"media group send failed: {e}",
                            filename=att.filename,
                            attach_type=att.content_type or "image/video",
                            direction="DC→TG",
                            tg_bot=tg_bot, tg_chat_id=chat_id,
                            tg_reply_to=reply_to_telegram_id,
                            dc_channel=dc_channel, dc_msg_ref=dc_msg_ref,
                        )
            # Reply anchor and caption only on the first chunk
            reply_to_telegram_id = None
            text = ""

    # If there were no photos/videos, send text as a plain message first
    if not photos_videos and text:
        try:
            sent = await tg_bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_to_message_id=reply_to_telegram_id,
            )
            tg_msg_ids.append(sent.message_id)
            reply_to_telegram_id = sent.message_id
            text = ""
        except Exception as e:
            logger.error(f"DC→TG: failed to send text message: {e}")

    # Send documents individually (can't be in a media group)
    for input_file, att in documents:
        try:
            sent = await tg_bot.send_document(
                chat_id=chat_id,
                document=input_file,
                caption=text if text else None,
                reply_to_message_id=reply_to_telegram_id,
            )
            tg_msg_ids.append(sent.message_id)
            text = ""  # caption only on first
            reply_to_telegram_id = None
        except Exception as e:
            await _warn_attachment_failure(
                reason=f"send failed: {e}",
                filename=att.filename,
                attach_type=att.content_type or "document",
                direction="DC→TG",
                tg_bot=tg_bot, tg_chat_id=chat_id,
                tg_reply_to=reply_to_telegram_id,
                dc_channel=dc_channel, dc_msg_ref=dc_msg_ref,
            )

    return tg_msg_ids


# ===========================================================================
# Routing: Telegram → Discord
# ===========================================================================

async def _handle_tg_delete_command(
    tg_bot,
    tg_group_id: str,
    tg_cmd_msg_id: str,
    tg_parent_msg_id: str,
) -> None:
    """Process a Telegram delete command reply.

    Called when a TG reply matches TG_MSG_DELETE_REGEX.  Performs the
    following steps (stopping on failure where noted):

    1. Look up the parent TG message in the DB.
       → If not found: leave the command reply as an untracked TG message.
    2. Delete the parent TG message.
       → If delete fails: post TG_MSG_DELETE_ERRMSG (if configured) and stop.
    3. Check how many TG messages are associated with the same Discord message.
       a. If more than one: remove only this TG message's DB row (disassociate).
          The Discord message and other TG messages remain.
       b. If this is the only one: delete the Discord message.
          → "Message not found" on Discord is treated as success (already gone).
          → Any other Discord error: post TG_MSG_DELETE_ERRMSG and stop.
    4. Remove this TG message's DB row.
    5. Delete the command reply from TG (so it doesn't linger as untracked).

    The command reply is NEVER bridged to Discord regardless of outcome.
    """
    loop = asyncio.get_running_loop()
    chat_id = int(tg_group_id)

    # ── Step 1: look up parent TG message in DB ───────────────────────────────
    record = await loop.run_in_executor(
        None, db.find_by_tg, tg_group_id, tg_parent_msg_id
    )
    if not record:
        logger.info(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"parent_tg_msg={tg_parent_msg_id} | tg_group={tg_group_id} | "
            f"result=NO_ACTION | reason=parent not in message map "
            f"(untracked message — leaving command reply on TG)"
        )
        return

    dc_channel_id = record["dc_channel_id"]
    dc_msg_id     = record["dc_message_id"]

    # ── Step 2: delete parent TG message ─────────────────────────────────────
    try:
        await tg_bot.delete_message(chat_id=chat_id, message_id=int(tg_parent_msg_id))
        logger.info(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"parent_tg_msg={tg_parent_msg_id} | tg_group={tg_group_id} | "
            f"step=TG_DELETE | result=ok"
        )
    except Exception as e:
        logger.warning(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"parent_tg_msg={tg_parent_msg_id} | tg_group={tg_group_id} | "
            f"step=TG_DELETE | result=FAILED | reason={e}"
        )
        if config.tg_msg_delete_errmsg:
            try:
                await tg_bot.send_message(
                    chat_id=chat_id,
                    text=config.tg_msg_delete_errmsg,
                    reply_to_message_id=int(tg_cmd_msg_id),
                )
            except Exception:
                pass
        return

    # ── Step 3: check how many TG messages share this Discord message ─────────
    all_records = await loop.run_in_executor(
        None, db.find_all_by_dc, dc_channel_id, dc_msg_id
    )
    tg_siblings = [r for r in all_records if r["tg_message_id"] != tg_parent_msg_id]

    if tg_siblings:
        # Other TG messages still exist for this Discord message — disassociate only
        await loop.run_in_executor(
            None, db.delete_by_tg, tg_group_id, tg_parent_msg_id
        )
        logger.info(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"parent_tg_msg={tg_parent_msg_id} | dc_msg={dc_msg_id} | "
            f"step=DB_DISASSOCIATE | result=ok | "
            f"remaining_tg_siblings={[r['tg_message_id'] for r in tg_siblings]}"
        )
    else:
        # ── Step 3b: sole TG message — delete the Discord message too ─────────
        dc_channel = await _get_discord_channel(dc_channel_id)
        discord_deleted = False
        if dc_channel:
            try:
                dc_msg_obj = await dc_channel.fetch_message(int(dc_msg_id))
                await with_retry(
                    f"DC delete #{dc_channel.name}",
                    lambda: dc_msg_obj.delete(),
                    platform="discord",
                )
                discord_deleted = True
                logger.info(
                    f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
                    f"parent_tg_msg={tg_parent_msg_id} | dc_msg={dc_msg_id} | "
                    f"step=DC_DELETE | result=ok"
                )
            except discord.NotFound:
                # Already gone — treat as success
                discord_deleted = True
                logger.info(
                    f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
                    f"dc_msg={dc_msg_id} | step=DC_DELETE | result=already_gone"
                )
            except Exception as e:
                logger.warning(
                    f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
                    f"dc_msg={dc_msg_id} | step=DC_DELETE | result=FAILED | reason={e}"
                )
                if config.tg_msg_delete_errmsg:
                    try:
                        await tg_bot.send_message(
                            chat_id=chat_id,
                            text=config.tg_msg_delete_errmsg,
                            reply_to_message_id=int(tg_cmd_msg_id),
                        )
                    except Exception:
                        pass
                return  # leave DB row and command reply intact
        else:
            logger.warning(
                f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
                f"dc_msg={dc_msg_id} | step=DC_DELETE | result=FAILED | "
                f"reason=Discord channel {dc_channel_id} not found"
            )
            if config.tg_msg_delete_errmsg:
                try:
                    await tg_bot.send_message(
                        chat_id=chat_id,
                        text=config.tg_msg_delete_errmsg,
                        reply_to_message_id=int(tg_cmd_msg_id),
                    )
                except Exception:
                    pass
            return

        if not discord_deleted:
            return  # already handled above

        # ── Step 4: remove DB row ─────────────────────────────────────────────
        await loop.run_in_executor(
            None, db.delete_by_tg, tg_group_id, tg_parent_msg_id
        )
        logger.info(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"parent_tg_msg={tg_parent_msg_id} | dc_msg={dc_msg_id} | "
            f"step=DB_DELETE | result=ok"
        )

    # ── Step 5: delete the command reply itself ───────────────────────────────
    try:
        await tg_bot.delete_message(chat_id=chat_id, message_id=int(tg_cmd_msg_id))
        logger.info(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"step=CMD_DELETE | result=ok"
        )
    except Exception as e:
        logger.warning(
            f"TG delete command | cmd_msg={tg_cmd_msg_id} | "
            f"step=CMD_DELETE | result=FAILED | reason={e} | "
            f"(command reply left as untracked TG message)"
        )


async def route_tg_to_discord(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle an incoming Telegram message and mirror it to Discord.

    Called by python-telegram-bot for every message in any group the bot
    is a member of.
    """
    msg = update.effective_message
    if msg is None:
        logger.info("TG→DC: update has no effective_message, skipping")
        return

    tg_chat = update.effective_chat
    tg_group_id = _tg_group_id_str(tg_chat.id)
    tg_msg_id   = _tg_msg_id_str(msg.message_id)

    # ---- Burst circuit breaker (total throughput protection) ----
    if not gateway_ratelimit.check_and_record(tg_group_id, config.telegram_burstrate):
        # Tripped. The breaker's own tripped-set is what suppresses subsequent
        # messages (check_and_record returns False for this group until the next
        # cache refresh calls reset_all_tripped). We also flag the group's
        # in-memory T_Status as "Excessive Rate" for operator visibility.
        sheets_manager.set_group_status_in_memory(
            tg_group_id, gateway_ratelimit.STATUS_EXCESSIVE_RATE
        )
        return

    sender_name = (
        msg.from_user.full_name if msg.from_user else tg_chat.title or "Unknown"
    )
    logger.info(
        f"TG→DC: received message {tg_msg_id} from '{sender_name}' "
        f"in group {tg_group_id} ('{tg_chat.title}')"
    )
    # Update Telegram connectivity timestamp for the health dashboard.
    # Persist immediately so tg_idle_min is accurate after a restart.
    bot_status.tg_last_update = localnow()
    try:
        _dashboard_reporter.save_to_db()
    except Exception:
        pass

    # ---- Telegram-side delete command ----
    # If TG_MSG_DELETE_REGEX is configured and this message is a reply whose
    # trimmed text matches the regex (full-string, case-sensitive by default),
    # treat it as a delete command rather than a bridged message.
    if config.tg_msg_delete_regex and msg.reply_to_message:
        import re as _re
        _msg_text = (msg.text or msg.caption or "")
        try:
            _delete_match = bool(_re.fullmatch(config.tg_msg_delete_regex, _msg_text))
        except _re.error as _e:
            logger.error(
                f"TG_MSG_DELETE_REGEX is invalid: {config.tg_msg_delete_regex!r} — {_e}"
            )
            _delete_match = False

        if _delete_match:
            await _handle_tg_delete_command(
                tg_bot=context.bot,
                tg_group_id=tg_group_id,
                tg_cmd_msg_id=tg_msg_id,
                tg_parent_msg_id=_tg_msg_id_str(msg.reply_to_message.message_id),
            )
            return  # never bridge the delete command to Discord

    # ---- Queue T_Group info for buffered write to Sheets ----
    # upsert_t_group now adds to a write-behind buffer; the actual Sheets
    # write happens in the background flush loop (every 60 seconds).
    sheets_manager.upsert_t_group_buffered(
        tg_group_id, tg_chat.title or "", tg_chat.type or "group"
    )
    # Check admin status the first time we see a group (not on every message).
    # The T_Group cache tells us if this is a known group; if it's new, check.
    if not sheets_manager.get_tg_group(tg_group_id):
        asyncio.ensure_future(
            _check_bot_admin_status(context.bot, tg_chat.id, tg_chat.title or "")
        )

    # ---- Determine target Discord channel ----
    # TG→DC routing — three cases in priority order:
    #   Case 1: Active D_User row matching T_GroupID → normal user/role tag
    #   Case 2: Inactive D_User row matching T_GroupID → normal tag + ERRMSG
    #   Case 3: No row at all → first Active channel + pseudo-tag + ERRMSG
    user_row = sheets_manager.get_user_by_tg_group(tg_group_id)
    _inactive_row: Optional[dict] = None
    if not user_row:
        _inactive_row = sheets_manager.get_user_by_tg_group_inactive(tg_group_id)

    _lookup_result = (
        f"Case 1 Active D_ID={user_row.get('D_ID')!r}" if user_row
        else f"Case 2 Inactive D_ID={_inactive_row.get('D_ID')!r}" if _inactive_row
        else "Case 3 fallback (no row)"
    )
    logger.info(
        f"TG→DC: cache lookup — tg_group_id={tg_group_id!r} → {_lookup_result}"
    )

    async def _post_tg_errmsg(errmsg: str) -> None:
        """Post an ERRMSG reply on Telegram if the string is non-empty."""
        if errmsg:
            try:
                await context.bot.send_message(
                    chat_id=tg_chat.id,
                    text=errmsg,
                    reply_to_message_id=int(tg_msg_id),
                )
            except Exception as _e:
                logger.warning(f"Could not post TG errmsg: {_e}")

    def _tg_unroutable_log(case: str, errmsg: str, extra: str = "") -> None:
        """Log a TG→DC routing event at WARNING or INFO depending on errmsg."""
        tg_raw = (msg.text or msg.caption or "").replace("\n", "\\n")
        log_msg = (
            f"TG→DC unroutable | case={case} | "
            f"tg_msg={tg_msg_id} | tg_group={tg_group_id}('{tg_chat.title}') | "
            f"tg_sender={sender_name!r} | tg_text={tg_raw!r} | "
            f"errmsg_sent={'yes' if errmsg else 'no'}"
            + (f" | {extra}" if extra else "")
        )
        if errmsg:
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

    if user_row:
        # Case 1: Active row — normal routing, no ERRMSG needed
        dc_channel_id = str(user_row.get("D_ChannelID", "")).strip()
        dc_user_id    = str(user_row.get("D_ID", "")).strip()
        user_tag      = f"<@{dc_user_id}>" if dc_user_id else ""
        logger.info(
            f"TG→DC: Case 1 (Active) — channel {dc_channel_id!r}, "
            f"user/role {dc_user_id!r}"
        )
    elif _inactive_row:
        # Case 2: Inactive row — route to its channel and tag normally.
        # Discord renders mentions of departed users as @DeletedUser.
        dc_channel_id = str(_inactive_row.get("D_ChannelID", "")).strip()
        dc_user_id    = str(_inactive_row.get("D_ID", "")).strip()
        user_tag      = f"<@{dc_user_id}>" if dc_user_id else ""
        errmsg2 = config.routed_inactive_ttod_errmsg
        _tg_unroutable_log(
            "2-inactive",
            errmsg2,
            f"dc_channel={dc_channel_id!r} dc_user_id={dc_user_id!r}"
        )
        await _post_tg_errmsg(errmsg2)
    else:
        # Case 3: No row at all — first Active Discord channel + pseudo-tag
        active_channels = sheets_manager.get_active_channels()
        if not active_channels:
            errmsg3 = config.unroutable_ttod_errmsg
            _tg_unroutable_log("3-no-channels", errmsg3)
            await _post_tg_errmsg(errmsg3)
            return
        dc_channel_id = active_channels[0]["D_ChannelID"]
        dc_user_id    = ""
        user_tag      = f"@ {tg_chat.title}"  # space prevents Discord mention
        errmsg3 = config.unroutable_ttod_errmsg
        _tg_unroutable_log(
            "3-fallback",
            errmsg3,
            f"fallback_channel={dc_channel_id!r}"
        )
        await _post_tg_errmsg(errmsg3)

    logger.info(f"TG→DC: fetching Discord channel object for {dc_channel_id}")
    channel = await _get_discord_channel(dc_channel_id)
    if channel is None:
        logger.error(f"TG→DC: Discord channel {dc_channel_id} not found by client")
        return

    logger.info(f"TG→DC: getting webhook for #{channel.name}")
    webhook = await _get_discord_webhook(channel)
    if webhook is None:
        logger.error(f"TG→DC: webhook unavailable for #{channel.name} — message not bridged")
        return

    # ---- Determine reply-chain root ----
    reply_to_tg_id: Optional[str] = None
    discord_reply_to_id: Optional[int] = None
    # origin_gateway inherited from the reply-parent (propagates down the tree);
    # blank for a non-reply message (never gateway-origin unless it came in via
    # the gateway, which is handled in bridge_gateway_message_to_discord).
    inherited_origin_gateway: str = ""
    # The immediate parent's own tg id, used when enqueuing an outbound reply
    # event so the client learns which of its messages was replied to.
    parent_immediate_tg_id: Optional[str] = None

    if msg.reply_to_message:
        parent_tg_id = _tg_msg_id_str(msg.reply_to_message.message_id)
        loop = asyncio.get_running_loop()
        parent_record = await loop.run_in_executor(
            None, db.find_by_tg, tg_group_id, parent_tg_id
        )
        if parent_record:
            discord_reply_to_id = int(parent_record["dc_message_id"])
            reply_to_tg_id      = parent_record["root_tg_msg_id"]
            inherited_origin_gateway = parent_record.get("origin_gateway", "") or ""
            parent_immediate_tg_id = parent_record["tg_message_id"]
        root_tg_msg_id = reply_to_tg_id or tg_msg_id
    else:
        root_tg_msg_id = tg_msg_id  # this message IS the root

    # ---- Build text content ----
    # Determine whether this is a forwarded message and set attribution.
    #
    # Body attribution rules:
    #   New non-forwarded message — the webhook header already shows the sender
    #       name ("Alice [TG]"), so body attribution is redundant. Omit it.
    #   Reply — channel.send() is used (not webhook), so the header shows the
    #       bot name. Body attribution is the only place the sender appears.
    #   Forwarded message — webhook header shows the forwarder; body shows
    #       the original sender. These are different people; keep both.
    #
    # python-telegram-bot v21+ uses forward_origin (a ForwardOrigin object).
    # ForwardOrigin subtypes:
    #   MessageOriginUser       — .sender_user.full_name
    #   MessageOriginHiddenUser — .sender_user_name (plain string)
    #   MessageOriginChat       — .sender_chat.title
    #   MessageOriginChannel    — .chat.title
    is_forwarded = bool(msg.forward_origin)
    is_reply = bool(discord_reply_to_id)

    if is_forwarded:
        fo = msg.forward_origin
        fwd_name = (
            getattr(getattr(fo, "sender_user", None), "full_name", None)
            or getattr(fo, "sender_user_name", None)
            or getattr(getattr(fo, "sender_chat", None), "title", None)
            or getattr(getattr(fo, "chat", None), "title", None)
            or "Hidden Sender"
        )
        attribution = f"↪️ Forwarded from **{fwd_name}** via **{tg_chat.title} [TG]**"
    elif is_reply:
        attribution = f"👤 **{sender_name} [TG]**"
    else:
        attribution = ""  # webhook header already shows sender name

    text_body = msg.text or msg.caption or ""

    # Tag line: mention the mapped Discord user on new (non-reply) messages.
    # Placed at the START of the message per Discord convention so the @mention
    # notification fires immediately and is visible without scrolling.
    # For unmapped groups the pseudo-tag "@ GroupName" goes at the start too.
    # Replies don't get a tag — the reply thread already shows context.
    if user_tag and not is_reply:
        tag_prefix = f"{user_tag}\n"
    else:
        tag_prefix = ""

    # Assemble content: tag, optional attribution, message text.
    # Filter out empty parts so we don't get stray blank lines.
    parts = [p for p in [tag_prefix.rstrip(), attribution, text_body] if p]
    content = "\n".join(parts).strip()
    if len(content) > 2000:
        content = content[:1997] + "…"

    logger.info(
        f"TG→DC: building message — "
        f"is_reply={is_reply}, is_forwarded={is_forwarded}, "
        f"tag_prefix={repr(tag_prefix.strip())}, dc_user_id={dc_user_id!r}"
    )

    # ---- Build Discord message reference for replies ----
    reference = None
    if discord_reply_to_id:
        try:
            ref_msg = await channel.fetch_message(discord_reply_to_id)
            reference = ref_msg.to_reference(fail_if_not_exists=False)
        except Exception:
            reference = None

    # ---- Attachments ----
    tg_bot: TelegramBot = context.bot
    dc_files, skip_notices = await _collect_tg_attachments(msg, tg_bot)

    # Post a warning on Telegram for any skipped attachments, and append
    # a notice to the Discord message content.
    for notice in skip_notices:
        content += f"\n{notice}"
        # Determine attachment info for the warning
        if msg.document:
            fname = msg.document.file_name or "file"
            atype = "document"
            reason = (
                f"file too large ({(msg.document.file_size or 0) // (1024*1024)} MB "
                f"> {DC_MAX_BYTES // (1024*1024)} MB Discord limit)"
                if (msg.document.file_size or 0) > DC_MAX_BYTES
                else "download or send failed"
            )
        else:
            fname = "attachment"
            atype = "unknown"
            reason = notice.strip("[]")
        await _warn_attachment_failure(
            reason=reason,
            filename=fname,
            attach_type=atype,
            direction="TG→DC",
            tg_bot=tg_bot,
            tg_chat_id=tg_chat.id,
            tg_reply_to=int(tg_msg_id),
            # dc_channel and dc_msg_ref not available yet — warning will be
            # posted as a follow-up reply after the main message is sent
        )

    # ---- Send to Discord ----
    # Uses webhook for new messages (custom display name) and channel.send()
    # for replies (webhooks don't support the reference parameter).
    dc_msg = await _send_to_discord(
        channel, webhook, sender_name, content, dc_files, reference
    )
    if dc_msg is None:
        return
    dc_msg_id = _dc_msg_id_str(dc_msg.id)

    # Post Discord-side attachment warnings now that we have the message ref
    if skip_notices:
        dc_ref = dc_msg.to_reference(fail_if_not_exists=False)
        for notice in skip_notices:
            try:
                await channel.send(
                    content=f"⚠️ {notice.strip('[]')}",
                    reference=dc_ref,
                    mention_author=False,
                )
            except Exception as e:
                logger.warning(f"Could not post attachment warning to Discord: {e}")

    # ---- Store mapping ----
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        db.store_message,
        tg_group_id,
        tg_msg_id,
        dc_channel_id,
        dc_msg_id,
        root_tg_msg_id,
        dc_user_id,
        inherited_origin_gateway,
    )
    # Detailed TG→DC bridge log — all key fields on one line for easy grep/research
    _tg_raw_text = (msg.text or msg.caption or "").replace("\n", "\\n")
    _dc_text_esc = content.replace("\n", "\\n")
    _tg_attach = "none"
    if msg.photo:
        _tg_attach = f"photo(largest={msg.photo[-1].file_size or '?'}B)"
    elif msg.video:
        _tg_attach = f"video({msg.video.file_name or 'video.mp4'},{msg.video.file_size or '?'}B)"
    elif msg.voice:
        _tg_attach = f"voice({msg.voice.file_size or '?'}B)"
    elif msg.audio:
        _tg_attach = f"audio({msg.audio.file_name or 'audio'},{msg.audio.file_size or '?'}B)"
    elif msg.document:
        _tg_attach = f"document({msg.document.file_name or 'file'},{msg.document.file_size or '?'}B)"
    elif msg.sticker:
        _tg_attach = f"sticker({msg.sticker.emoji or '?'},animated={msg.sticker.is_animated})"
    elif msg.poll:
        _tg_attach = f"poll({msg.poll.question!r})"
    _tg_reply_info = (
        f"reply_to_tg={msg.reply_to_message.message_id}"
        if msg.reply_to_message else "not_a_reply"
    )
    _dc_reply_info = (
        f"reply_to_dc={discord_reply_to_id}"
        if discord_reply_to_id else "new_message"
    )
    logger.info(
        f"TG→DC bridged | "
        f"tg_msg={tg_msg_id} | "
        f"tg_group={tg_group_id}('{tg_chat.title}') | "
        f"tg_sender={sender_name!r} | "
        f"tg_text={_tg_raw_text!r} | "
        f"tg_attach={_tg_attach} | "
        f"{_tg_reply_info} | "
        f"dc_msg={dc_msg_id} | "
        f"dc_channel=#{channel.name}({dc_channel_id}) | "
        f"dc_text={_dc_text_esc!r} | "
        f"{_dc_reply_info}"
    )
    bot_status.bridged_30m += 1
    try:
        _dashboard_reporter.save_to_db()
    except Exception:
        pass

    # If this Telegram message is a reply to a gateway-origin message, the reply
    # also flows back out the gateway to the client.
    if inherited_origin_gateway:
        await _gw_enqueue_outbound_message(
            origin_gateway=inherited_origin_gateway,
            tg_group_id=tg_group_id,
            tg_msg_id=tg_msg_id,
            text=(msg.text or msg.caption or ""),
            sender_name=sender_name,
            reply_to_tg_id=parent_immediate_tg_id,
        )


async def _gw_enqueue_outbound_message(
    origin_gateway: str, tg_group_id: str, tg_msg_id: str,
    text: str, sender_name: str, reply_to_tg_id: Optional[str],
    edited: bool = False,
) -> None:
    """Enqueue an outbound 'message' (or 'edited_message') event for the gateway
    client — used when a reply to (or edit of) a gateway-origin message occurs."""
    if not _gateway_server.is_serving():
        return
    try:
        gid = int(tg_group_id)
        mid = int(tg_msg_id)
    except (ValueError, TypeError):
        return
    reply_to = None
    if reply_to_tg_id:
        try:
            reply_to = int(reply_to_tg_id)
        except (ValueError, TypeError):
            reply_to = None
    env = gp.make_message(
        origin_gateway, gid,
        message_id=mid,
        text=text,
        reply_to=reply_to,
        from_user=gp.User(first_name=sender_name, is_synthetic=True),
        edited=edited,
    )
    await _gateway_server.enqueue_outbound(
        origin_gateway, gid, env.to_json(include_secret=False)
    )
    logger.info(
        f"GW outbound | gateway={origin_gateway} | type="
        f"{'edited_message' if edited else 'message'} | "
        f"tg_group={tg_group_id} | tg_msg={tg_msg_id} | "
        f"reply_to={reply_to} | sender={sender_name!r}"
    )


async def _gw_enqueue_outbound_reaction(
    origin_gateway: str, tg_group_id: str, tg_msg_id: str,
    emoji: list, sender_name: str,
) -> None:
    """Enqueue an outbound 'reaction' event for the gateway client."""
    if not _gateway_server.is_serving():
        return
    try:
        gid = int(tg_group_id)
        mid = int(tg_msg_id)
    except (ValueError, TypeError):
        return
    env = gp.make_reaction(
        origin_gateway, gid, mid, list(emoji),
        from_user=gp.User(first_name=sender_name, is_synthetic=True),
    )
    await _gateway_server.enqueue_outbound(
        origin_gateway, gid, env.to_json(include_secret=False)
    )
    logger.info(
        f"GW outbound | gateway={origin_gateway} | type=reaction | "
        f"tg_group={tg_group_id} | tg_msg={tg_msg_id} | emoji={emoji}"
    )


async def _gw_enqueue_outbound_deletion(
    origin_gateway: str, tg_group_id: str, tg_msg_ids: list,
) -> None:
    """Enqueue an outbound 'deletion' event for the gateway client."""
    if not _gateway_server.is_serving():
        return
    try:
        gid = int(tg_group_id)
        mids = [int(m) for m in tg_msg_ids]
    except (ValueError, TypeError):
        return
    env = gp.make_deletion(origin_gateway, gid, mids)
    await _gateway_server.enqueue_outbound(
        origin_gateway, gid, env.to_json(include_secret=False)
    )
    logger.info(
        f"GW outbound | gateway={origin_gateway} | type=deletion | "
        f"tg_group={tg_group_id} | tg_msgs={mids}"
    )


async def bridge_gateway_message_to_discord(
    *,
    tg_group_id: str,
    tg_msg_id: str,
    sender_name: str,
    text: str,
    reply_to_tg_id: Optional[str] = None,
    origin_gateway: str = "",
    dc_files: Optional[list] = None,
) -> Optional[str]:
    """Bridge a gateway-originated message to Discord (Phase 6a, text-only).

    This is the "central function" of the gateway server: after a message has
    been placed in the Telegram group (Echo=true, by the caller, who then passes
    the real message_id here) — or after the client asserts it posted the message
    itself (Echo=false, client-supplied id) — we mirror it into Discord exactly
    as if it had arrived as a normal incoming Telegram message.

    Routing is by T_GroupID ALONE (the prod sheet does not model gateways), using
    the same Active/Inactive/fallback cases as route_tg_to_discord. The message
    is attributed to `sender_name` (the gateway message's from.first_name, or the
    gateway name). Returns the Discord message id (as a string) on success, or
    None if it could not be bridged.

    Attachments are NOT handled here (Phase 4 deferred them in the send path);
    text only.
    """
    logger.info(
        f"GW→DC: bridging gateway message {tg_msg_id} for group {tg_group_id} "
        f"(sender {sender_name!r})"
    )

    # ---- Determine target Discord channel (same cases as TG→DC) ----
    user_row = sheets_manager.get_user_by_tg_group(tg_group_id)
    _inactive_row: Optional[dict] = None
    if not user_row:
        _inactive_row = sheets_manager.get_user_by_tg_group_inactive(tg_group_id)

    if user_row:
        dc_channel_id = str(user_row.get("D_ChannelID", "")).strip()
        dc_user_id    = str(user_row.get("D_ID", "")).strip()
        user_tag      = f"<@{dc_user_id}>" if dc_user_id else ""
    elif _inactive_row:
        dc_channel_id = str(_inactive_row.get("D_ChannelID", "")).strip()
        dc_user_id    = str(_inactive_row.get("D_ID", "")).strip()
        user_tag      = f"<@{dc_user_id}>" if dc_user_id else ""
    else:
        active_channels = sheets_manager.get_active_channels()
        if not active_channels:
            logger.warning(
                f"GW→DC unroutable | tg_group={tg_group_id} | no active channels"
            )
            return None
        dc_channel_id = active_channels[0]["D_ChannelID"]
        dc_user_id    = ""
        user_tag      = ""  # no pseudo-tag for gateway messages

    channel = await _get_discord_channel(dc_channel_id)
    if channel is None:
        logger.error(f"GW→DC: Discord channel {dc_channel_id} not found")
        return None
    webhook = await _get_discord_webhook(channel)
    if webhook is None:
        logger.error(f"GW→DC: webhook unavailable for #{channel.name}")
        return None

    # ---- Reply resolution ----
    discord_reply_to_id: Optional[int] = None
    root_tg_msg_id = tg_msg_id
    if reply_to_tg_id:
        loop = asyncio.get_running_loop()
        parent_record = await loop.run_in_executor(
            None, db.find_by_tg, tg_group_id, str(reply_to_tg_id)
        )
        if parent_record:
            discord_reply_to_id = int(parent_record["dc_message_id"])
            root_tg_msg_id = parent_record["root_tg_msg_id"]
    is_reply = bool(discord_reply_to_id)

    # ---- Build content ----
    attribution = f"👤 **{sender_name} [GW]**" if is_reply else ""
    tag_prefix = f"{user_tag}\n" if (user_tag and not is_reply) else ""
    parts = [p for p in [tag_prefix.rstrip(), attribution, text or ""] if p]
    content = "\n".join(parts).strip()
    if len(content) > 2000:
        content = content[:1997] + "…"

    reference = None
    if discord_reply_to_id:
        try:
            ref_msg = await channel.fetch_message(discord_reply_to_id)
            reference = ref_msg.to_reference(fail_if_not_exists=False)
        except Exception:
            reference = None

    dc_msg = await _send_to_discord(
        channel, webhook, sender_name, content, (dc_files or []), reference
    )
    if dc_msg is None:
        return None
    dc_msg_id = _dc_msg_id_str(dc_msg.id)

    # ---- Store mapping (same store as TG→DC, so replies/reactions resolve) ----
    # Mark this message with its origin gateway so that replies/reactions/edits/
    # deletions concerning it (or its descendants) flow back out the gateway.
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, db.store_message,
        tg_group_id, tg_msg_id, dc_channel_id, dc_msg_id, root_tg_msg_id, dc_user_id,
        origin_gateway,
    )

    _dc_text_esc = content.replace("\n", "\\n")
    logger.info(
        f"GW→DC bridged | "
        f"tg_msg={tg_msg_id} | tg_group={tg_group_id} | "
        f"sender={sender_name!r} | "
        f"dc_msg={dc_msg_id} | dc_channel=#{channel.name}({dc_channel_id}) | "
        f"dc_text={_dc_text_esc!r} | "
        f"{'reply_to_dc=' + str(discord_reply_to_id) if is_reply else 'new_message'}"
    )
    bot_status.bridged_30m += 1
    try:
        _dashboard_reporter.save_to_db()
    except Exception:
        pass
    return dc_msg_id


async def route_tg_edit_to_discord(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle an edited Telegram message and mirror the edit to Discord."""
    msg = update.edited_message
    if msg is None:
        return

    tg_chat     = update.effective_chat
    tg_group_id = _tg_group_id_str(tg_chat.id)
    tg_msg_id   = _tg_msg_id_str(msg.message_id)
    sender_name = (
        msg.from_user.full_name if msg.from_user else tg_chat.title or "Unknown"
    )

    loop = asyncio.get_running_loop()
    record = await loop.run_in_executor(None, db.find_by_tg, tg_group_id, tg_msg_id)
    if not record:
        logger.debug(f"Edit received for unknown TG message {tg_msg_id} in group {tg_group_id}")
        return

    dc_channel_id = record["dc_channel_id"]
    dc_msg_id     = int(record["dc_message_id"])

    channel = await _get_discord_channel(dc_channel_id)
    if channel is None:
        return

    new_text = msg.text or msg.caption or ""
    edit_prefix = f"✏️ EDIT — 👤 **{sender_name} [TG]**\n"
    origin_gateway = record.get("origin_gateway", "") or ""

    async def _enqueue_tg_edit_outbound():
        if origin_gateway:
            await _gw_enqueue_outbound_message(
                origin_gateway=origin_gateway,
                tg_group_id=str(tg_group_id),
                tg_msg_id=str(tg_msg_id),
                text=new_text,
                sender_name=sender_name,
                reply_to_tg_id=None,
                edited=True,
            )

    # Cascade 1: try to edit the original Discord message
    try:
        dc_msg = await channel.fetch_message(dc_msg_id)
        await with_retry(
            f"DC edit #{channel.name}",
            lambda: dc_msg.edit(content=edit_prefix + new_text),
            platform="discord",
        )
        logger.info(f"Edited Discord message {dc_msg_id} for TG edit {tg_msg_id}")
        await _enqueue_tg_edit_outbound()
        return
    except discord.NotFound:
        pass
    except discord.Forbidden:
        # Webhooks create messages owned by the webhook; they can be edited via webhook
        pass
    except Exception as e:
        logger.warning(f"Could not edit Discord message {dc_msg_id}: {e}")

    # Cascade 2: post a new reply with the edited content.
    # Always use channel.send() here since we always want a reply reference.
    try:
        ref_msg = await channel.fetch_message(dc_msg_id)
        reference = ref_msg.to_reference(fail_if_not_exists=False)
    except Exception:
        reference = None

    try:
        await channel.send(
            content=edit_prefix + new_text,
            reference=reference,
            mention_author=False,
        )
        await _enqueue_tg_edit_outbound()
    except Exception as e:
        logger.error(f"Failed to post edit fallback to Discord: {e}")


async def route_tg_delete_to_discord(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle a Telegram message deletion and mirror it to Discord.

    Telegram's Bot API does not deliver message deletion events to bots,
    so this handler is a placeholder only.  TG-initiated deletions can be
    performed using the TG_MSG_DELETE_REGEX reply command instead.
    """
    # python-telegram-bot does not deliver a delete event directly.
    # Deletions are detected indirectly via message_id gaps or by the
    # Telegram API's message.is_deleted flag.
    # This handler is a placeholder; actual deletion detection requires
    # polling or webhook event type "message" with empty text and the
    # "message_deleted" service flag.
    # For Phase 1A the delete behavior is configured via .env; if the
    # originating Telegram client sends a service message, it appears as
    # an ordinary message and is logged / bridged as text.
    pass


async def route_tg_reaction_to_discord(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle a Telegram reaction and mirror it as a reply on Discord."""
    reaction_update = update.message_reaction
    if not reaction_update:
        logger.info("TG→DC reaction: update has no message_reaction field, skipping")
        return

    tg_group_id = _tg_group_id_str(reaction_update.chat.id)
    tg_msg_id   = _tg_msg_id_str(reaction_update.message_id)
    actor_name  = (
        reaction_update.user.full_name
        if reaction_update.user
        else "Someone"
    )

    # Get the new reactions (may be empty if all reactions were removed)
    new_reactions = reaction_update.new_reaction
    if not new_reactions:
        logger.info(
            f"TG→DC reaction: reaction removed by {actor_name} on "
            f"tg_msg {tg_msg_id} in group {tg_group_id} — no action"
        )
        return  # Reaction was removed; no action for now

    # Build emoji string from ReactionType objects
    emoji_str = " ".join(
        getattr(r, "emoji", "?") for r in new_reactions
    )
    logger.info(
        f"TG→DC reaction: {actor_name} reacted {emoji_str!r} to "
        f"tg_msg {tg_msg_id} in group {tg_group_id}"
    )
    bot_status.tg_last_update = localnow()
    try:
        _dashboard_reporter.save_to_db()
    except Exception:
        pass

    loop = asyncio.get_running_loop()
    record = await loop.run_in_executor(None, db.find_by_tg, tg_group_id, tg_msg_id)
    if not record:
        logger.info(
            f"TG→DC reaction: tg_msg {tg_msg_id} in group {tg_group_id} "
            f"not found in message map — reaction not bridged. "
            f"(Message may have originated outside TDbridge, or map entry was purged.)"
        )
        return

    dc_channel_id = record["dc_channel_id"]
    dc_msg_id     = int(record["dc_message_id"])

    channel = await _get_discord_channel(dc_channel_id)
    if channel is None:
        return

    behavior = config.reactions_ttod
    if behavior == "neither":
        logger.info(
            f"TG→DC reaction | tg_msg={tg_msg_id} | tg_group={tg_group_id} | "
            f"actor={actor_name!r} | emoji={emoji_str} | "
            f"result=IGNORED | reason=REACTIONS_TTOD=neither"
        )
        return

    try:
        ref_msg   = await channel.fetch_message(dc_msg_id)
        reference = ref_msg.to_reference(fail_if_not_exists=False)

        native_ok = False
        if behavior in ("react", "both"):
            try:
                await with_retry(
                    f"DC add_reaction #{channel.name}",
                    lambda: ref_msg.add_reaction(emoji_str),
                    platform="discord",
                )
                native_ok = True
            except Exception as e:
                logger.warning(
                    f"TG→DC reaction | tg_msg={tg_msg_id} | dc_msg={dc_msg_id} | "
                    f"emoji={emoji_str} | result=NATIVE_FAILED | reason={e}"
                )

        reply_ok = False
        if behavior in ("reply", "both"):
            await channel.send(
                content=f"{emoji_str} **{actor_name}** reacted to this message",
                reference=reference,
                mention_author=False,
            )
            reply_ok = True

        logger.info(
            f"TG→DC reaction | "
            f"tg_msg={tg_msg_id} | tg_group={tg_group_id} | "
            f"actor={actor_name!r} | emoji={emoji_str} | "
            f"dc_msg={dc_msg_id} | dc_channel={dc_channel_id} | "
            f"behavior={behavior} | native={'ok' if native_ok else 'skipped/failed'} | "
            f"reply={'ok' if reply_ok else 'skipped'}"
        )

        # If the reacted-to message is gateway-origin, the reaction flows out.
        origin_gateway = record.get("origin_gateway", "") or ""
        if origin_gateway:
            emoji_list = [getattr(r, "emoji", "?") for r in new_reactions]
            await _gw_enqueue_outbound_reaction(
                origin_gateway=origin_gateway,
                tg_group_id=str(tg_group_id),
                tg_msg_id=str(tg_msg_id),
                emoji=emoji_list,
                sender_name=actor_name,
            )
    except Exception as e:
        logger.warning(
            f"TG→DC reaction | tg_msg={tg_msg_id} | dc_msg={dc_msg_id} | "
            f"emoji={emoji_str} | result=FAILED | reason={e}"
        )


# ===========================================================================
# Routing: Discord → Telegram
# ===========================================================================

class TDbridgeDiscordClient(discord.Client):
    """Discord client with message and reaction event handlers."""

    async def on_ready(self) -> None:
        # This method is overridden by the @client.event on_ready decorator
        # in main() and therefore never fires.  All startup logic lives in
        # _startup(), which is called from that decorator.
        logger.info(
            f"Discord bot ready: {self.user} (id={self.user.id})"
        )

    async def on_message(self, message: discord.Message) -> None:
        """Route a Discord message to the appropriate Telegram group."""
        # Ignore our own messages and webhook messages we sent
        if message.author == self.user:
            return
        if message.webhook_id:
            return  # messages we posted via webhook; don't echo back
        if not isinstance(message.channel, discord.TextChannel):
            return

        # Only bridge messages in Active Discord channels.
        # Channels not in D_Channel_Sheet, or with a non-Active status, are
        # silently ignored — no logging, no Discord warning.  TDbridge can see
        # many channels (general, announcements, etc.) that it should never
        # bridge; producing noise for those would be counterproductive.
        incoming_channel_id = str(message.channel.id)
        channel_record = sheets_manager.get_channel(incoming_channel_id)
        if not channel_record:
            return  # channel not in D_Channel table — silently ignore
        if not sheets_manager._is_active(channel_record.get("D_ChannelStatus", "")):
            return  # channel is Inactive — silently ignore

        dc_channel_id = str(message.channel.id)
        dc_msg_id     = str(message.id)
        sender_name   = (
            message.author.nick
            or message.author.display_name
            or message.author.name
        )

        attribution = f"👤 {sender_name} (Discord):"

        # ---- Determine target Telegram group ----
        tg_group_id: Optional[str] = None
        root_tg_msg_id: Optional[str] = None
        # The Telegram message id to reply to: the IMMEDIATE parent's own id, so
        # the reply tree is preserved faithfully (not flattened to the root).
        # Stays None when this is not a reply or the parent isn't on the TG side,
        # in which case we post a new message rather than walking toward the root.
        immediate_reply_tg_id: Optional[str] = None
        # origin_gateway inherited from the reply-parent (propagates down the
        # tree); blank for a non-reply message.
        inherited_origin_gateway: str = ""

        loop = asyncio.get_running_loop()

        # Case 1: Reply to a bridged message
        if message.reference and message.reference.message_id:
            parent_dc_id = str(message.reference.message_id)
            parent_record = await loop.run_in_executor(
                None, db.find_by_dc, dc_channel_id, parent_dc_id
            )
            if parent_record:
                tg_group_id    = parent_record["tg_group_id"]
                root_tg_msg_id = parent_record["root_tg_msg_id"]
                immediate_reply_tg_id = parent_record["tg_message_id"]
                inherited_origin_gateway = parent_record.get("origin_gateway", "") or ""

        # Case 2: First tagged user OR role (left-to-right in message text)
        # that is Active, has a T_GroupID, and has a D_ChannelID matching
        # the incoming channel.
        #
        # Scans message.content for <@id>, <@!id>, and <@&role_id> tokens in
        # order.  role IDs are looked up with the "&" prefix matching D_ID.
        # message.mentions is unordered and role-unaware, so we parse the text.
        if not tg_group_id:
            import re as _re
            # Extract all mention tokens in left-to-right order.
            # Capture (optional &)(digits) so we can reconstruct the lookup key.
            raw_mentions = _re.findall(r"<@!?(&?)(\d+)>", message.content)
            seen_mention_keys: set[str] = set()
            ordered_keys: list[str] = []
            for amp, digits in raw_mentions:
                key = f"{amp}{digits}"   # "&12345" for roles, "12345" for users
                if key not in seen_mention_keys:
                    seen_mention_keys.add(key)
                    ordered_keys.append(key)

            for key in ordered_keys:
                row = sheets_manager.get_user_by_discord_id(key)
                if not row:
                    logger.info(f"DC→TG routing: tagged {key!r} not in Sheets cache, skipping")
                    continue
                status = row.get("D_UserStatus", "")
                tgid = str(row.get("T_GroupID", "")).strip()
                row_channel = str(row.get("D_ChannelID", "")).strip()
                if not tgid:
                    logger.info(f"DC→TG routing: tagged {key!r} has no T_GroupID, skipping")
                    continue
                if not sheets_manager._is_active(status):
                    logger.info(
                        f"DC→TG routing: tagged {key!r} is not Active "
                        f"(D_UserStatus={status!r}), skipping"
                    )
                    continue
                if row_channel and row_channel != incoming_channel_id:
                    logger.info(
                        f"DC→TG routing: tagged {key!r} D_ChannelID={row_channel!r} "
                        f"does not match incoming channel {incoming_channel_id!r}, skipping"
                    )
                    continue
                tg_group_id = tgid
                logger.info(
                    f"DC→TG routing: routing to tagged {'role' if key.startswith('&') else 'user'} "
                    f"{key!r} → TG group {tg_group_id} via channel {incoming_channel_id}"
                )
                break

        # Case 3: Sender's user ID matches an Active D_User row with a
        # T_GroupID and a D_ChannelID matching the incoming channel.
        if not tg_group_id:
            sender_uid = str(message.author.id)
            sender_row = sheets_manager.get_user_by_discord_id(sender_uid)
            if sender_row:
                status = sender_row.get("D_UserStatus", "")
                tgid = str(sender_row.get("T_GroupID", "")).strip()
                sender_channel = str(sender_row.get("D_ChannelID", "")).strip()
                if not tgid:
                    logger.info(
                        f"DC→TG routing: sender {sender_uid} has no T_GroupID, skipping"
                    )
                elif not sheets_manager._is_active(status):
                    logger.info(
                        f"DC→TG routing: sender {sender_uid} is not Active "
                        f"(D_UserStatus={status!r}), skipping"
                    )
                elif sender_channel and sender_channel != incoming_channel_id:
                    logger.info(
                        f"DC→TG routing: sender {sender_uid} D_ChannelID={sender_channel!r} "
                        f"does not match incoming channel {incoming_channel_id!r}, skipping"
                    )
                else:
                    tg_group_id = tgid
                    logger.info(
                        f"DC→TG routing: routing to sender user {sender_uid} "
                        f"→ TG group {tg_group_id} via channel {incoming_channel_id}"
                    )

        # Case 4: Sender's Discord roles, searched in D_User table row order.
        # If the sender belongs to a role that has an Active D_User row with
        # a T_GroupID and a D_ChannelID matching the incoming channel, use it.
        # Table row order determines priority when the sender has multiple roles.
        if not tg_group_id:
            sender_role_ids = {f"&{r.id}" for r in message.author.roles}
            for row in sheets_manager.get_all_user_rows_in_table_order():
                did = str(row.get("D_ID", "")).strip()
                if not did.startswith("&"):
                    continue   # user row, not a role row
                if did not in sender_role_ids:
                    continue
                status = row.get("D_UserStatus", "")
                tgid = str(row.get("T_GroupID", "")).strip()
                row_channel = str(row.get("D_ChannelID", "")).strip()
                if not tgid:
                    continue
                if not sheets_manager._is_active(status):
                    continue
                if row_channel and row_channel != incoming_channel_id:
                    continue
                tg_group_id = tgid
                logger.info(
                    f"DC→TG routing: routing via sender role {did!r} "
                    f"→ TG group {tg_group_id} via channel {incoming_channel_id}"
                )
                break

        # Unroutable — channel is Active but no Telegram group could be found.
        # If UNROUTABLE_DTOT_ERRMSG is non-empty, post it in Discord and log
        # at WARNING.  If empty, log at INFO only with no Discord message.
        if not tg_group_id:
            _dc_unroutable_text = message.content.replace("\n", "\\n")
            _dc_attach_info = (
                ", ".join(
                    f"{a.filename}({a.content_type or '?'},{(a.size or 0)//1024}KB)"
                    for a in message.attachments
                ) if message.attachments else "none"
            )
            _dc_reply_info = (
                f"reply_to_dc={message.reference.message_id}"
                if message.reference and message.reference.message_id
                else "not_a_reply"
            )
            _dc_mentions = ", ".join(
                f"{a}{b}" for a, b in
                __import__("re").findall(r"<@!?(&?)(\d+)>", message.content)
            ) or "none"
            unroutable_msg = (
                f"DC→TG unroutable | "
                f"dc_msg={dc_msg_id} | "
                f"dc_user={message.author.id}({message.author.name}) | "
                f"dc_channel=#{message.channel.name}({dc_channel_id}) | "
                f"dc_text={_dc_unroutable_text!r} | "
                f"attachments=[{_dc_attach_info}] | "
                f"{_dc_reply_info} | "
                f"mentions=[{_dc_mentions}] | "
                f"reason=no active TG group found for sender or tagged users/roles"
            )
            if config.unroutable_dtot_errmsg:
                logger.warning(unroutable_msg)
                try:
                    reference = message.to_reference(fail_if_not_exists=False)
                    await message.channel.send(
                        config.unroutable_dtot_errmsg,
                        reference=reference,
                        mention_author=False,
                    )
                except Exception:
                    pass
            else:
                logger.info(unroutable_msg)
            return

        # ---- Burst circuit breaker (total throughput protection) ----
        # Now that the target group is known, count this message toward the
        # group's rate. If tripped, mark it "Excessive Rate" in memory and stop.
        if not gateway_ratelimit.check_and_record(tg_group_id, config.telegram_burstrate):
            sheets_manager.set_group_status_in_memory(
                tg_group_id, gateway_ratelimit.STATUS_EXCESSIVE_RATE
            )
            return

        # ---- Build message text ----
        # Resolve <@id> and <#id> Discord mention tokens to readable names
        # before sending to Telegram, where raw snowflake IDs are meaningless.
        resolved_content = _resolve_discord_mentions(message.content)
        text = f"{attribution} {resolved_content}".strip()

        # ---- Send to Telegram ----
        tg_bot: TelegramBot = _tg_app.bot

        reply_to_telegram_id: Optional[int] = None
        if immediate_reply_tg_id:
            try:
                reply_to_telegram_id = int(immediate_reply_tg_id)
            except (ValueError, TypeError):
                pass

        try:
            dc_msg_ref = None
            if message.reference and message.reference.message_id:
                try:
                    ref_dc_msg = await message.channel.fetch_message(
                        message.reference.message_id
                    )
                    dc_msg_ref = ref_dc_msg.to_reference(fail_if_not_exists=False)
                except Exception:
                    pass

            if message.attachments:
                sent_ids = await _send_attachments_to_telegram(
                    attachments=message.attachments,
                    text=text,
                    tg_bot=tg_bot,
                    tg_group_id=tg_group_id,
                    reply_to_telegram_id=reply_to_telegram_id,
                    dc_channel=message.channel,
                    dc_msg_ref=dc_msg_ref,
                )
                # sent_ids contains ALL Telegram message IDs produced by this
                # Discord message (one per photo/video in a media group, or one
                # for a plain text/document message).  We must store a DB mapping
                # for EVERY Telegram message ID — not just the first — so that
                # if the Discord message is later deleted, ALL corresponding
                # Telegram messages are deleted, not just the first one.
                tg_msg_id = _tg_msg_id_str(sent_ids[0]) if sent_ids else None
                tg_all_ids = [_tg_msg_id_str(i) for i in sent_ids] if sent_ids else []
            else:
                sent = await tg_bot.send_message(
                    chat_id=int(tg_group_id),
                    text=text,
                    reply_to_message_id=reply_to_telegram_id,
                )
                tg_msg_id  = _tg_msg_id_str(sent.message_id)
                tg_all_ids = [tg_msg_id]

            # Store mapping for every Telegram message ID produced.
            # The first ID is the "canonical" one used for reply routing;
            # subsequent IDs (additional media group items) map back to the
            # same Discord message so deletion catches them all.
            if tg_msg_id:
                _root = root_tg_msg_id or tg_msg_id
                for _tid in tg_all_ids:
                    await loop.run_in_executor(
                        None,
                        db.store_message,
                        tg_group_id,
                        _tid,
                        dc_channel_id,
                        dc_msg_id,
                        _root,
                        str(message.author.id),
                        inherited_origin_gateway,
                    )
                if len(tg_all_ids) > 1:
                    logger.info(
                        f"DC→TG: stored {len(tg_all_ids)} TG message mappings "
                        f"for DC msg {dc_msg_id}: {tg_all_ids}"
                    )
                bot_status.bridged_30m += 1
                try:
                    _dashboard_reporter.save_to_db()
                except Exception:
                    pass

                # ---- Detailed bridge log ----
                # Escape newlines so the entire record fits on one log line.
                _dc_content_esc = message.content.replace("\n", "\\n")
                _tg_text_esc    = text.replace("\n", "\\n")
                _attach_info    = (
                    ", ".join(
                        f"{a.filename}({a.content_type or '?'},{(a.size or 0) // 1024}KB)"
                        for a in message.attachments
                    ) if message.attachments else "none"
                )
                _reply_info = (
                    f"reply_to_dc={message.reference.message_id}"
                    if message.reference and message.reference.message_id
                    else "not_a_reply"
                )
                _tg_reply_to_info = (
                    f"tg_reply_to={reply_to_telegram_id}"
                    if reply_to_telegram_id else "tg_new_message"
                )
                _tg_all_ids_str = (
                    f"tg_msgs={tg_all_ids}" if len(tg_all_ids) > 1
                    else f"tg_msg={tg_msg_id}"
                )
                logger.info(
                    f"DC→TG bridged | "
                    f"dc_msg={dc_msg_id} | "
                    f"dc_user={message.author.id}({message.author.name}) | "
                    f"dc_channel=#{message.channel.name}({dc_channel_id}) | "
                    f"dc_text={_dc_content_esc!r} | "
                    f"attachments=[{_attach_info}] | "
                    f"{_reply_info} | "
                    f"tg_group={tg_group_id} | "
                    f"{_tg_all_ids_str} | "
                    f"{_tg_reply_to_info} | "
                    f"tg_text={_tg_text_esc!r}"
                )

                # If this Discord message is a reply to a gateway-origin
                # message, the reply also flows back out the gateway.
                if inherited_origin_gateway and tg_msg_id:
                    await _gw_enqueue_outbound_message(
                        origin_gateway=inherited_origin_gateway,
                        tg_group_id=tg_group_id,
                        tg_msg_id=tg_msg_id,
                        text=text,
                        sender_name=sender_name,
                        reply_to_tg_id=immediate_reply_tg_id,
                    )

        except Exception as e:
            logger.error(f"Failed to send Discord message to Telegram group {tg_group_id}: {e}")

    async def on_raw_message_edit(
        self, payload: discord.RawMessageUpdateEvent
    ) -> None:
        """Bridge a Discord message edit to Telegram.

        Uses the RAW event so edits to messages not in discord.py's cache (e.g.
        created before the most recent restart) still bridge. The raw event
        carries the new fields in payload.data (the gateway payload) rather than
        a discord.Message, so we read content/author from there.
        """
        data = payload.data or {}
        # Ignore edits from the bot itself or from webhooks (our own bridged
        # messages are posted via webhook).
        if "webhook_id" in data:
            return
        author = data.get("author") or {}
        author_id = str(author.get("id", ""))
        if self.user and author_id == str(self.user.id):
            return
        # Some edit events (embeds resolving, pins) carry no content change.
        if "content" not in data:
            return

        dc_channel_id = str(payload.channel_id)
        dc_msg_id     = str(payload.message_id)
        sender_name   = (
            author.get("global_name")
            or author.get("username")
            or "Discord user"
        )

        loop = asyncio.get_running_loop()
        record = await loop.run_in_executor(None, db.find_by_dc, dc_channel_id, dc_msg_id)
        if not record:
            return

        tg_group_id  = record["tg_group_id"]
        tg_msg_id    = int(record["tg_message_id"])
        origin_gateway = record.get("origin_gateway", "") or ""
        resolved_edit = _resolve_discord_mentions(data.get("content", ""))
        new_text      = f"✏️ EDIT — 👤 {sender_name} (Discord): {resolved_edit}"

        tg_bot: TelegramBot = _tg_app.bot

        async def _enqueue_edit_outbound():
            # An edit of a gateway-origin message flows back out the gateway as
            # an edited_message carrying the new (resolved) text.
            if origin_gateway:
                await _gw_enqueue_outbound_message(
                    origin_gateway=origin_gateway,
                    tg_group_id=str(tg_group_id),
                    tg_msg_id=str(tg_msg_id),
                    text=resolved_edit,
                    sender_name=sender_name,
                    reply_to_tg_id=None,
                    edited=True,
                )

        # Cascade 1: try to edit the Telegram message
        try:
            await tg_bot.edit_message_text(
                chat_id=int(tg_group_id),
                message_id=tg_msg_id,
                text=new_text,
            )
            await _enqueue_edit_outbound()
            return
        except Exception as e:
            logger.warning(f"Could not edit TG message {tg_msg_id}: {e}")

        # Cascade 2: send a new reply with edit indicator
        try:
            await tg_bot.send_message(
                chat_id=int(tg_group_id),
                text=new_text,
                reply_to_message_id=tg_msg_id,
            )
            await _enqueue_edit_outbound()
        except Exception as e:
            logger.error(f"Failed to post edit fallback to Telegram: {e}")

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent) -> None:
        """Bridge a Discord message deletion to Telegram.

        Uses the RAW event (not on_message_delete) so it fires even for messages
        not in discord.py's in-memory cache — notably messages created before the
        most recent restart. A single Discord message may have produced multiple
        Telegram messages (e.g. a media group); we act on each.
        """
        dc_channel_id = str(payload.channel_id)
        dc_msg_id     = str(payload.message_id)

        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            None, db.find_all_by_dc, dc_channel_id, dc_msg_id
        )
        if not records:
            return

        # Resolve a channel name for logging (best-effort).
        chan = self.get_channel(payload.channel_id)
        dc_channel_name = getattr(chan, "name", str(payload.channel_id))

        tg_bot: TelegramBot = _tg_app.bot
        # dc_msg_delete_behavior: "delete" | "ignore" | "<any other string>"
        # Any string other than "delete" or "ignore" is posted as a TG reply.
        behavior = config.dc_msg_delete_behavior

        # All records share the same tg_group_id (a Discord message only
        # routes to one Telegram group).
        tg_group_id  = records[0]["tg_group_id"]
        tg_msg_ids   = [int(r["tg_message_id"]) for r in records]
        tg_first_id  = tg_msg_ids[0]   # use first as the reply anchor for notices

        if behavior == "ignore":
            logger.info(
                f"DC→TG delete ignored (DC_MSG_DELETE_BEHAVIOR=ignore): "
                f"dc_msg {dc_msg_id} → tg_msgs {tg_msg_ids}"
            )
            return

        if behavior != "delete":
            # Any value other than "delete" or "ignore" is posted as a TG reply
            try:
                await tg_bot.send_message(
                    chat_id=int(tg_group_id),
                    text=behavior,
                    reply_to_message_id=tg_first_id,
                )
            except Exception as e:
                logger.warning(f"Failed to post delete notification to Telegram: {e}")
            return

        # "delete": attempt to delete every Telegram message produced by this
        # Discord message.

        deleted_tg = []
        failed_tg  = []
        for tg_msg_id in tg_msg_ids:
            try:
                await tg_bot.delete_message(
                    chat_id=int(tg_group_id),
                    message_id=tg_msg_id,
                )
                deleted_tg.append(tg_msg_id)
            except Exception as e:
                failed_tg.append((tg_msg_id, str(e)))
                logger.warning(
                    f"DC→TG delete | dc_msg={dc_msg_id} | "
                    f"tg_msg={tg_msg_id} | tg_group={tg_group_id} | "
                    f"result=FAILED | reason={e}"
                )
                if config.delete_fail_errmsg:
                    try:
                        await tg_bot.send_message(
                            chat_id=int(tg_group_id),
                            text=config.delete_fail_errmsg,
                            reply_to_message_id=tg_msg_id,
                        )
                    except Exception:
                        pass

        logger.info(
            f"DC→TG delete | "
            f"dc_msg={dc_msg_id} | "
            f"dc_channel=#{dc_channel_name}({dc_channel_id}) | "
            f"tg_group={tg_group_id} | "
            f"tg_msgs_deleted={deleted_tg} | "
            f"tg_msgs_failed={[t for t, _ in failed_tg]}"
        )

        # If any of the deleted messages were gateway-origin, the deletion also
        # flows back out the gateway. (All records share one origin_gateway, set
        # when the message was bridged / inherited down its reply tree.)
        _origin_gateway = ""
        for r in records:
            og = r.get("origin_gateway", "") or ""
            if og:
                _origin_gateway = og
                break
        if _origin_gateway:
            await _gw_enqueue_outbound_deletion(
                origin_gateway=_origin_gateway,
                tg_group_id=str(tg_group_id),
                tg_msg_ids=tg_msg_ids,
            )

        # Remove all DB records for this Discord message after attempting deletion
        rows_removed = await loop.run_in_executor(
            None, db.delete_by_dc, dc_channel_id, dc_msg_id
        )
        logger.info(
            f"DC→TG delete | db_rows_removed={rows_removed} for dc_msg={dc_msg_id}"
        )

    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        """Bridge a Discord reaction to Telegram as a reply message.

        Uses the RAW event so it fires for messages not in discord.py's cache
        (e.g. created before the most recent restart).
        """
        if payload.user_id == self.user.id:
            return

        dc_channel_id = str(payload.channel_id)
        dc_msg_id     = str(payload.message_id)
        # payload.member is present for guild reactions; fall back to fetching
        # the user if needed.
        member = payload.member
        if member is not None:
            user_id   = member.id
            user_name = (
                getattr(member, "nick", None)
                or member.display_name
                or member.name
            )
        else:
            user_id   = payload.user_id
            fetched   = self.get_user(payload.user_id)
            user_name = (fetched.display_name if fetched else str(payload.user_id))

        loop = asyncio.get_running_loop()
        record = await loop.run_in_executor(None, db.find_by_dc, dc_channel_id, dc_msg_id)
        if not record:
            return

        tg_group_id = record["tg_group_id"]
        tg_msg_id   = int(record["tg_message_id"])
        emoji_str   = str(payload.emoji)

        behavior = config.reactions_dtot
        if behavior == "neither":
            logger.info(
                f"DC→TG reaction | dc_msg={dc_msg_id} | dc_channel={dc_channel_id} | "
                f"user={user_name!r} | emoji={emoji_str} | "
                f"result=IGNORED | reason=REACTIONS_DTOT=neither"
            )
            return

        tg_bot: TelegramBot = _tg_app.bot
        native_ok = False
        reply_ok  = False

        if behavior in ("react", "both"):
            from telegram import ReactionTypeEmoji
            try:
                await tg_bot.set_message_reaction(
                    chat_id=int(tg_group_id),
                    message_id=tg_msg_id,
                    reaction=[ReactionTypeEmoji(emoji=emoji_str)],
                )
                native_ok = True
            except Exception as e:
                logger.warning(
                    f"DC→TG reaction | dc_msg={dc_msg_id} | tg_msg={tg_msg_id} | "
                    f"emoji={emoji_str} | result=NATIVE_FAILED | reason={e}"
                )
                if behavior == "react":
                    logger.info(
                        f"DC→TG reaction | dc_msg={dc_msg_id} | tg_msg={tg_msg_id} | "
                        f"emoji={emoji_str} | result=NOT_BRIDGED | "
                        f"reason=react-only and native failed"
                    )
                    return

        if behavior in ("reply", "both"):
            try:
                await tg_bot.send_message(
                    chat_id=int(tg_group_id),
                    text=f"{emoji_str} {user_name} (Discord) reacted to this message",
                    reply_to_message_id=tg_msg_id,
                )
                reply_ok = True
            except Exception as e:
                logger.warning(
                    f"DC→TG reaction | dc_msg={dc_msg_id} | tg_msg={tg_msg_id} | "
                    f"emoji={emoji_str} | result=REPLY_FAILED | reason={e}"
                )

        logger.info(
            f"DC→TG reaction | "
            f"dc_msg={dc_msg_id} | dc_channel={dc_channel_id} | "
            f"user={user_name!r}({str(user_id)}) | emoji={emoji_str} | "
            f"tg_msg={tg_msg_id} | tg_group={tg_group_id} | "
            f"behavior={behavior} | "
            f"native={'ok' if native_ok else 'skipped/failed'} | "
            f"reply={'ok' if reply_ok else 'skipped'}"
        )

        # If the reacted-to message is gateway-origin, the reaction also flows
        # back out the gateway to the client.
        origin_gateway = record.get("origin_gateway", "") or ""
        if origin_gateway:
            await _gw_enqueue_outbound_reaction(
                origin_gateway=origin_gateway,
                tg_group_id=str(tg_group_id),
                tg_msg_id=str(tg_msg_id),
                emoji=[emoji_str],
                sender_name=user_name,
            )


# ===========================================================================
# Background tasks
# ===========================================================================

async def _refresh_discord_to_sheets(discord_client: discord.Client) -> None:
    """Scan all guild members and text channels visible to the bot and upsert
    them into D_User_Sheet and D_Channel_Sheet.

    This is the TDbridge equivalent of HCF's user_refresh_v2.py.  It runs
    at startup and then every 24 hours.

    What gets updated
    -----------------
    D_User_Sheet  — every member of every guild the bot is in.
                    Columns written: D_ID, D_UserName, D_Nickname,
                    D_DisplayName, D_LastFound.
                    User-maintained columns (D_ChannelID, D_UserStatus,
                    T_GroupID, etc.) are left untouched on update and are
                    left blank on insert, per the table spec.

    D_Channel_Sheet — every text channel in every guild the bot can see.
                    Columns written: D_ChannelID, D_ChannelName, D_LastFound.
                    D_ChannelStatus is left untouched on update and blank on
                    insert.

    Batching strategy
    -----------------
    All members across all guilds are collected first, then written to Sheets
    in a single read-modify-write cycle (one table read, one batch of updates,
    one batch of inserts).  Same for channels.  This is O(1) API reads
    regardless of guild size, compared to O(n) in the per-record approach.

    Duplicate D_ID handling: if the same user appears in multiple guilds
    (which is common), only the first occurrence is kept.
    """
    logger.info("Discord → Sheets refresh starting")

    all_users: list[dict] = []
    all_channels: list[dict] = []
    seen_user_ids: set[str] = set()
    seen_channel_ids: set[str] = set()

    # ── Collect phase: gather everything from Discord first ──────────────────
    for guild in discord_client.guilds:
        logger.info(f"Scanning guild '{guild.name}' ({guild.id})")

        # Members — fetch_members() pages through the full list regardless
        # of cache.  Requires the Members privileged intent.
        try:
            async for member in guild.fetch_members(limit=None):
                if member.bot:
                    continue
                uid = str(member.id)
                if uid in seen_user_ids:
                    continue
                seen_user_ids.add(uid)
                all_users.append({
                    "discord_id":   uid,
                    "username":     member.name,
                    "nickname":     member.nick or "",
                    "display_name": member.display_name,
                })
        except discord.Forbidden:
            logger.warning(
                f"No permission to fetch members in guild '{guild.name}' — "
                f"check that the Members privileged intent is enabled"
            )
        except Exception as e:
            logger.error(f"Error fetching members in guild '{guild.name}': {e}")

        # Roles — stored with D_ID = "&<role_id>" to distinguish from users.
        # D_Nickname holds the role name (server-specific, like a nickname).
        # D_UserName and D_DisplayName are left empty (user-account properties).
        # The @everyone role (id == guild.id) is skipped — it's not useful for routing.
        for role in guild.roles:
            if role.id == guild.id:
                continue  # skip @everyone
            rid = f"&{role.id}"
            if rid in seen_user_ids:
                continue
            seen_user_ids.add(rid)
            all_users.append({
                "discord_id":   rid,
                "username":     "",           # not applicable for roles
                "nickname":     role.name,    # role name is server-specific
                "display_name": "",           # not applicable for roles
            })

        # Text channels the bot can read
        for channel in guild.text_channels:
            cid = str(channel.id)
            if cid in seen_channel_ids:
                continue
            perms = channel.permissions_for(guild.me)
            if not perms.read_messages:
                continue
            seen_channel_ids.add(cid)
            all_channels.append({
                "channel_id":   cid,
                "channel_name": channel.name,
            })

    # Separate counts for logging clarity
    n_users = sum(1 for u in all_users if not u["discord_id"].startswith("&"))
    n_roles = sum(1 for u in all_users if u["discord_id"].startswith("&"))
    logger.info(
        f"Discord scan complete: {n_users} unique users, "
        f"{n_roles} roles, {len(all_channels)} readable channels"
    )

    # ── Write phase: one batched read-modify-write per table ─────────────────
    if all_users:
        await sheets_manager.batch_upsert_d_users(all_users)

    if all_channels:
        await sheets_manager.batch_upsert_d_channels(all_channels)

    logger.info(
        f"Discord → Sheets refresh complete: "
        f"{n_users} users, {n_roles} roles, {len(all_channels)} channels written"
    )
    # The batch_upsert functions keep the TableManager .records current, so
    # we can rebuild the routing cache from in-memory data without any further
    # API calls.  This saves 6 API calls (3 × row1 + 3 × read_all) compared
    # to calling refresh_async() here.
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, sheets_manager._build_caches_from_managers)
    logger.info(sheets_manager.status_summary())


async def _discord_refresh_loop(discord_client: discord.Client) -> None:
    """Run _refresh_discord_to_sheets every 24 hours after the initial run."""
    TWENTY_FOUR_HOURS = 86400
    while True:
        await asyncio.sleep(TWENTY_FOUR_HOURS)
        await _refresh_discord_to_sheets(discord_client)


async def _sheets_refresh_loop() -> None:
    """Periodically refresh the Google Sheets mapping cache (fast path).

    This re-reads the sheets every SHEETS_REFRESH_INTERVAL seconds so that
    changes made manually by the user (e.g. setting D_UserStatus to Active,
    filling in T_GroupID) are picked up quickly without waiting 24 hours for
    the full Discord refresh cycle.
    """
    while True:
        await asyncio.sleep(config.sheets_refresh_interval)
        logger.info("Scheduled Sheets cache refresh starting")
        await sheets_manager.refresh_async()
        # The refresh re-read every group's real T_Status from the sheet, so any
        # group the burst circuit breaker had marked "Excessive Rate" in memory
        # is now restored — clear the breaker's tripped state so it can flow again.
        gateway_ratelimit.reset_all_tripped()
        logger.info(sheets_manager.status_summary())


async def _db_purge_loop() -> None:
    """Purge old SQLite records once per day, and sweep gateway-file leftovers."""
    TWENTY_FOUR_HOURS = 86400
    while True:
        await asyncio.sleep(TWENTY_FOUR_HOURS)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, db.purge_older_than, 30)
        # Sweep any gateway attachment left on disk > 24h (should not happen;
        # sweep_leftovers logs an ERROR per leftover). Only meaningful when this
        # instance owns a gateway, but the call is harmless otherwise.
        if _gateway_server.enabled:
            import gateway_files
            await loop.run_in_executor(None, gateway_files.sweep_leftovers)


# ===========================================================================
# Startup
# ===========================================================================

async def _check_bot_admin_status(
    tg_bot,
    chat_id: int,
    chat_title: str,
) -> None:
    """Check whether the bot is an administrator in a Telegram group.

    Logs a WARNING and sends a message to the group if the bot is not an
    administrator.  This is important because Telegram only delivers
    message_reaction updates to admin bots — reactions from non-admin bots
    are silently dropped by Telegram's servers.

    Safe to call any time; handles exceptions gracefully.
    """
    try:
        from telegram import ChatMemberAdministrator, ChatMemberOwner
        bot_id = tg_bot.id
        member = await tg_bot.get_chat_member(chat_id=chat_id, user_id=bot_id)
        is_admin = isinstance(member, (ChatMemberAdministrator, ChatMemberOwner))
        if not is_admin:
            warn = (
                f"⚠️ TDbridge is not an administrator in this group. "
                f"Reaction bridging requires administrator status. "
                f"Please promote TDbridge to administrator."
            )
            logger.warning(
                f"Bot is not an administrator in Telegram group {chat_id} "
                f"('{chat_title}') — reaction updates will not be delivered by Telegram. "
                f"Promote the bot to administrator to enable reaction bridging."
            )
            try:
                await tg_bot.send_message(chat_id=chat_id, text=warn)
            except Exception as e:
                logger.warning(f"Could not send admin warning to group {chat_id}: {e}")
        else:
            logger.info(
                f"Bot has administrator status in Telegram group {chat_id} "
                f"('{chat_title}') — reaction bridging enabled"
            )
    except Exception as e:
        logger.warning(f"Could not check bot admin status in group {chat_id}: {e}")


async def _on_bot_status_change(
    update: "Update",
    context: "ContextTypes.DEFAULT_TYPE",
) -> None:
    """Handle my_chat_member updates — fires when bot status changes in a group.

    Checks admin status and warns if the bot was added without admin rights.
    Also fires on promotion/demotion, so covers the case where an admin
    later removes the bot's admin rights.
    """
    from telegram import ChatMemberAdministrator, ChatMemberOwner, ChatMemberMember
    change = update.my_chat_member
    if not change:
        return

    chat = change.chat
    new_status = change.new_chat_member

    # Only care about groups and supergroups, not private chats or channels
    if chat.type not in ("group", "supergroup"):
        return

    chat_title = chat.title or str(chat.id)
    logger.info(
        f"Bot status change in '{chat_title}' ({chat.id}): "
        f"{type(new_status).__name__}"
    )

    if isinstance(new_status, (ChatMemberAdministrator, ChatMemberOwner)):
        logger.info(
            f"Bot is now an administrator in '{chat_title}' ({chat.id}) "
            f"— reaction bridging enabled"
        )
    elif isinstance(new_status, ChatMemberMember):
        # Bot was added as regular member, or demoted from admin
        warn = (
            f"⚠️ TDbridge has been added to this group without administrator rights. "
            f"Reaction bridging requires administrator status. "
            f"Please promote TDbridge to administrator."
        )
        logger.warning(
            f"Bot added/demoted to regular member in '{chat_title}' ({chat.id}) "
            f"— reactions will not be bridged until bot is made administrator"
        )
        try:
            await context.bot.send_message(chat_id=chat.id, text=warn)
        except Exception as e:
            logger.warning(f"Could not send admin warning to group {chat.id}: {e}")


async def _start_telegram_app() -> None:
    """Build and start the Telegram application in webhook or polling mode.

    Platform differences (isolated entirely to this function):
    ----------------------------------------------------------
    Linux  — webhook mode.  The bot starts its own HTTPS server (using the
             Let's Encrypt certificate) and registers the public URL with
             Telegram.  Telegram POSTs updates immediately as they arrive.

    Windows — polling mode.  The bot asks Telegram for new updates every few
              seconds using long-polling.  No certificate, no public URL, and
              no open port are required.  Any previously registered webhook is
              deleted on startup so polling and webhooking don't conflict.

    All handler functions (route_tg_to_discord, etc.) are identical in both
    modes — only the transport layer differs.
    """
    global _tg_app, _poll_counters

    # Configure generous HTTP timeouts.
    # PTB's defaults (5s connect, 5s read) are too short for large media uploads
    # such as a 10-photo album.  media_write_timeout covers the actual upload.
    # PollCountingRequest also counts getUpdates poll results for the dashboard
    # polling-health check and the summarised poll logging.
    _poll_counters = _PollCounters()
    config.set_poll_counters(_poll_counters)
    _tg_request = PollCountingRequest(
        counters=_poll_counters,
        connect_timeout=10.0,
        read_timeout=60.0,
        write_timeout=120.0,
        media_write_timeout=180.0,
    )

    tg_app = (
        ApplicationBuilder()
        .token(config.telegram_bot_token)
        .request(_tg_request)
        .build()
    )

    # Register handlers — identical for both webhook and polling modes.
    #
    # Handler type notes:
    #   MessageHandler(filters.TEXT | filters.PHOTO | ...)
    #       — fires for new messages matching the filter
    #   MessageHandler(filters.UpdateType.EDITED_MESSAGE, ...)
    #       — does NOT work; edited_message is a different Update field.
    #         Use filters.UpdateType.EDITED_MESSAGE inside a MessageHandler
    #         only with the edited_message attribute; instead we use
    #         filters.ALL on the handler and check update.edited_message
    #         inside the callback (see route_tg_edit_to_discord).
    #   MessageReactionHandler
    #       — fires for MessageReactionUpdated updates (emoji reactions).
    #         These are NOT MessageHandler updates and require their own
    #         handler type.

    # New messages (text, media, stickers, polls, forwarded, etc.)
    # filters.ALL catches everything including text with no caption.
    # We exclude COMMAND so /start etc. from BotFather don't get bridged.
    tg_app.add_handler(
        MessageHandler(
            filters.ALL & ~filters.COMMAND & filters.UpdateType.MESSAGES,
            route_tg_to_discord,
        )
    )

    # Edited messages — filters.UpdateType.EDITED_MESSAGE selects
    # updates where the edited_message field is set (not message).
    tg_app.add_handler(
        MessageHandler(
            filters.UpdateType.EDITED_MESSAGE,
            route_tg_edit_to_discord,
        )
    )

    # Reactions — MessageReactionUpdated update type, requires its own handler
    tg_app.add_handler(
        MessageReactionHandler(route_tg_reaction_to_discord)
    )

    # my_chat_member — fires when the bot's own status changes in a chat
    # (added to a group, promoted to admin, demoted, removed, etc.).
    # We use this to warn immediately when the bot is added without admin rights,
    # since admin status is required to receive message_reaction updates.
    from telegram.ext import ChatMemberHandler
    tg_app.add_handler(
        ChatMemberHandler(
            _on_bot_status_change,
            ChatMemberHandler.MY_CHAT_MEMBER,
        )
    )

    # Global error handler — catches any unhandled exception raised inside a
    # handler callback and logs it through our logger with a full traceback.
    # Without this, python-telegram-bot logs the error itself at WARNING level
    # and the exception is silently swallowed.
    async def _tg_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.error(
            f"Unhandled exception in Telegram handler "
            f"(update={type(update).__name__}): {context.error}",
            exc_info=context.error,
        )

    tg_app.add_error_handler(_tg_error_handler)

    await tg_app.initialize()
    await tg_app.start()

    if _USE_POLLING:
        # ---- Windows: polling mode ----
        # Delete any webhook that may have been left registered from a prior
        # server run, so Telegram doesn't try to POST to a stale URL while
        # we are polling.
        # delete_webhook resets Telegram's stored allowed_updates to the API
        # default, which excludes message_reaction.  We pass our explicit list
        # to start_polling so every getUpdates call requests reaction updates.
        await tg_app.bot.delete_webhook(drop_pending_updates=False)
        logger.info("Telegram webhook deleted (polling mode)")
        await tg_app.updater.start_polling(
            allowed_updates=_ALLOWED_UPDATES,
            drop_pending_updates=False,
        )
        _real_platform = __import__("platform").system()
        _is_wsl = (_real_platform == "Linux" and config.platform == "Windows")
        if config.platform == "Linux":
            _mode_desc = "Linux, forced by TELEGRAM_USE_POLLING"
        elif _is_wsl:
            _mode_desc = "WSL2"
        else:
            _mode_desc = "Windows"
        logger.info(
            f"Telegram polling started "
            f"({_mode_desc}) "
            f"— allowed_updates={_ALLOWED_UPDATES}"
        )
    else:
        # ---- Linux: webhook mode ----
        # Verify TLS cert files exist and are readable.
        # These are used by stunnel (the TLS terminator), not by the bot
        # process itself.  We check here so a missing/unreadable cert is
        # caught at startup with a clear message rather than silently causing
        # stunnel to fail to deliver webhook updates.
        for label, path in [("cert", config.tls_cert_file), ("key", config.tls_key_file)]:
            if not os.path.exists(path):
                raise FileNotFoundError(
                    f"TLS {label} file not found: {path}\n"
                    f"Check TLS_CERT_FILE / TLS_KEY_FILE in .env"
                )
            try:
                with open(path, "r") as _f:
                    _f.read(1)
            except PermissionError:
                raise PermissionError(
                    f"TLS {label} file exists but is not readable: {path}\n"
                    f"Run: sudo chmod 640 /etc/letsencrypt/archive/hcf.squadrontrucking.com/*.pem"
                )
        # TLS is terminated by stunnel, which listens on the public port
        # (88 for test, 8443 for prod), presents the full Let's Encrypt
        # certificate chain to Telegram, and forwards plain HTTP to the
        # bot on localhost:TELEGRAM_WEBHOOK_PORT.
        #
        # We therefore verify the cert files are readable (so stunnel's
        # config is likely correct) but do NOT pass them to start_webhook —
        # the bot listens on plain HTTP internally.
        logger.info(
            f"TLS termination handled by stunnel "
            f"(cert: {config.tls_cert_file})"
        )

        # start_webhook() registers the webhook with Telegram internally via
        # its own bootstrap loop (including retry on rate-limit).  We do NOT
        # call bot.set_webhook() separately — doing so would trigger two
        # set_webhook calls in quick succession and cause a 429 flood error.
        # url_path must match the path component of the webhook URL so that
        # tornado serves requests at that path.  Extract it from the configured
        # webhook URL (e.g. "https://host:88/tgwebhook" → "/tgwebhook").
        from urllib.parse import urlparse
        webhook_path = urlparse(config.telegram_webhook_url).path.lstrip("/")

        await tg_app.updater.start_webhook(
            listen="127.0.0.1",
            port=config.telegram_webhook_port,
            url_path=webhook_path,
            secret_token=config.telegram_webhook_secret or None,
            webhook_url=config.telegram_webhook_url,
            allowed_updates=_ALLOWED_UPDATES,
        )
        logger.info(
            f"Telegram webhook server listening on 127.0.0.1:{config.telegram_webhook_port}"
            f"/{webhook_path} (plain HTTP — TLS handled by stunnel) "
            f"— allowed_updates={_ALLOWED_UPDATES}"
        )

    _tg_app = tg_app


async def _startup(discord_client: discord.Client) -> None:
    """Run all startup tasks after Discord is connected and ready."""
    global _sheets_refresh_task, _db_purge_task, _discord_refresh_task

    # on_ready only fires once the Discord gateway is established.
    # Set dc_connected here, before emit_startup(), so the first Status
    # Report correctly shows dc=connected.
    bot_status.dc_connected = True

    logger.info("=== TDbridge startup ===")
    logger.info(f"Environment : {config.env.upper()}")
    logger.info(f"Discord bot : {config.discord_bot_name}")
    logger.info(f"Telegram bot: {config.telegram_bot_name} (@{config.telegram_bot_username})")
    logger.info(f"Spreadsheet : {config.google_spreadsheet_name}")
    logger.info(f"SQLite DB   : {config.sqlite_db_file}")
    logger.info(config.gateway_config_summary())

    # ---- Set bot nickname in all guilds ----
    # Nickname is per-guild, so we loop.  Errors are logged at ERROR level
    # because a missing nickname likely means a permissions problem that an
    # administrator should be aware of.
    if config.discord_bot_nickname:
        for guild in discord_client.guilds:
            try:
                await guild.me.edit(nick=config.discord_bot_nickname)
                logger.info(
                    f"Set nickname to {config.discord_bot_nickname!r} "
                    f"in guild '{guild.name}'"
                )
            except discord.Forbidden:
                logger.error(
                    f"Cannot set nickname in guild '{guild.name}' — "
                    f"bot lacks 'Change Nickname' permission. "
                    f"Grant this in Server Settings → Roles."
                )
            except Exception as e:
                logger.error(
                    f"Failed to set nickname in guild '{guild.name}': {e}"
                )
    else:
        logger.info("No DISCORD_BOT_NICKNAME configured — nickname not changed")

    # Initialise SQLite
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.init_db)

    # Restore persisted status fields so the first Status Report after a
    # restart shows accurate tg_idle_min and bridged_30m rather than 9999/0.
    _dashboard_reporter.load_from_db()

    # Restore any T_Group writes that were pending when the bot last stopped.
    sheets_manager._load_pending_t_group_from_db()

    # Initial Sheets load — read T_Group only.
    # D_User and D_Channel are intentionally skipped here because
    # _refresh_discord_to_sheets() performs a full read-modify-write on
    # both tables immediately below, making an initial read redundant.
    # T_Group is read now because it is not written during the Discord
    # refresh and its data is needed for routing as soon as the bot is ready.
    # This saves 4 API calls (D_User row1+data + D_Channel row1+data).
    logger.info("Loading Google Sheets mapping tables (startup: T_Group only)…")
    await sheets_manager.startup_refresh_async()
    logger.info(sheets_manager.status_summary())

    # Start Telegram webhook app
    await _start_telegram_app()

    # Initial Discord → Sheets refresh (discover all members and channels).
    # After the upserts complete, builds the cache from already-loaded data
    # (no extra API reads).
    logger.info("Running initial Discord → Sheets member/channel refresh…")
    await _refresh_discord_to_sheets(discord_client)

    # Background tasks
    _sheets_refresh_task   = asyncio.create_task(_sheets_refresh_loop())
    _db_purge_task         = asyncio.create_task(_db_purge_loop())
    _discord_refresh_task  = asyncio.create_task(_discord_refresh_loop(discord_client))
    _t_group_flush_task    = asyncio.create_task(sheets_manager.t_group_flush_loop())

    # Start the gateway server before the first Status Report so its state is
    # reflected accurately (no-op for a client-only instance).
    bot_status.gateway_expected = _gateway_server.enabled

    async def _gateway_bridge(*, chat_id, text, reply_to, sender_name, echo,
                              client_msg_id, attachments=None):
        """The gateway's central function: place the message in Telegram
        (Echo=true) or accept the client-supplied id (Echo=false), then bridge
        it to Discord exactly as an incoming Telegram message would be.

        Phase 6d: inbound attachments. `attachments` is a list of dicts
        {file_ref, file_name, mime_type, size} referencing files the client
        uploaded to our store. We read the bytes, send them to Telegram (echo)
        and bridge them to Discord, then delete the gateway files at the
        terminal moment.

        Returns {"message_ids": [...], "dc_message_id": <str or None>,
                 "notes": [...]}.
        """
        attachments = attachments or []
        notes: list = []

        # ---- Burst circuit breaker (total throughput protection) ----
        gid_str = _tg_group_id_str(chat_id)
        if not gateway_ratelimit.check_and_record(gid_str, config.telegram_burstrate):
            sheets_manager.set_group_status_in_memory(
                gid_str, gateway_ratelimit.STATUS_EXCESSIVE_RATE
            )
            logger.warning(
                f"Gateway send suppressed by burst circuit breaker for group {gid_str}"
            )
            return {"message_ids": [], "dc_message_id": None, "suppressed": True}

        # ---- Read attachment bytes from our file store (the client uploaded
        # them to us). Collect the file_refs so we can delete them at the end. ----
        loop = asyncio.get_running_loop()
        loaded: list = []          # list of {data, file_name, mime_type, size}
        file_refs: list = []       # file_refs to delete at the terminal moment
        for a in attachments:
            ref = a.get("file_ref")
            if not ref:
                continue
            file_refs.append(ref)
            info = await loop.run_in_executor(
                None, gateway_files.read_file_by_ref, ref
            )
            if info is None:
                notes.append(f"attachment {a.get('file_name','?')} unavailable (bad file_ref)")
                continue
            # Prefer the message's declared name/mime over the stored metadata.
            info["file_name"] = a.get("file_name") or info["file_name"]
            info["mime_type"] = a.get("mime_type") or info["mime_type"]
            loaded.append(info)

        tg_msg_id = None
        tg_msg_ids: list = []
        telegram_failed = False
        try:
            # ---- Step 1: place the message in Telegram (echo) ----
            if echo:
                bot = _tg_app.bot if _tg_app else None
                if bot is None:
                    raise RuntimeError("Telegram app not available")
                if loaded:
                    # Send attachments (with text as caption) via the reused
                    # Telegram attachment sender, adapting gateway bytes.
                    tg_attachments = [
                        _GatewayAttachment(
                            i["data"], i["file_name"], i["mime_type"], i["size"]
                        )
                        for i in loaded
                    ]
                    tg_msg_ids = await _send_attachments_to_telegram(
                        attachments=tg_attachments,
                        text=(text or ""),
                        tg_bot=bot,
                        tg_group_id=gid_str,
                        reply_to_telegram_id=reply_to,
                        dc_channel=None,        # no Discord context for warnings here
                        dc_msg_ref=None,
                    )
                    if not tg_msg_ids:
                        # Everything was skipped (e.g. too large) — fall back to
                        # sending the text so the message isn't lost.
                        sent = await bot.send_message(
                            chat_id=chat_id,
                            text=(text or "(attachment could not be sent)"),
                            reply_to_message_id=reply_to,
                        )
                        tg_msg_ids = [sent.message_id]
                    tg_msg_id = tg_msg_ids[0]
                else:
                    sent = await bot.send_message(
                        chat_id=chat_id,
                        text=(text or ""),
                        reply_to_message_id=reply_to,
                    )
                    tg_msg_id = sent.message_id
                    tg_msg_ids = [tg_msg_id]
            else:
                # Echo=false: the client asserts it posted the message itself.
                if client_msg_id is None:
                    raise RuntimeError("Echo=false send requires a client-supplied message_id")
                tg_msg_id = client_msg_id
                tg_msg_ids = [tg_msg_id]
        except Exception:
            telegram_failed = True
            # Per design: if the Telegram side fails, there is nothing to bridge
            # to Discord, so we do not attempt it. Delete the inbound files since
            # nothing more can be done with them, then re-raise.
            for ref in file_refs:
                try:
                    await loop.run_in_executor(None, gateway_files.delete_file, ref)
                except Exception:
                    pass
            raise

        # ---- Step 2: build Discord files (respecting Discord's size limit) and
        # bridge to Discord. ----
        dc_files = []
        for i in loaded:
            if i["size"] > DC_MAX_BYTES:
                notes.append(
                    f"attachment {i['file_name']} too large for Discord "
                    f"({i['size'] // (1024*1024)} MB > {DC_MAX_BYTES // (1024*1024)} MB)"
                )
                continue
            dc_files.append(discord.File(io.BytesIO(i["data"]), filename=i["file_name"]))

        dc_msg_id = await bridge_gateway_message_to_discord(
            tg_group_id=gid_str,
            tg_msg_id=_tg_msg_id_str(tg_msg_id),
            sender_name=sender_name,
            text=(text or ""),
            reply_to_tg_id=(_tg_msg_id_str(reply_to) if reply_to is not None else None),
            origin_gateway=config.own_gateway,
            dc_files=dc_files,
        )

        # ---- Step 3: terminal moment — both sends concluded. Delete the inbound
        # gateway files (the bytes have been delivered as far as they can go). ----
        for ref in file_refs:
            try:
                await loop.run_in_executor(None, gateway_files.delete_file, ref)
            except Exception as e:
                logger.warning(f"Could not delete inbound gateway file {ref}: {e}")

        return {
            "message_ids": tg_msg_ids or ([tg_msg_id] if tg_msg_id is not None else []),
            "dc_message_id": dc_msg_id,
            "notes": notes,
        }

    _gateway_server.set_bridge_hook(_gateway_bridge)
    await _gateway_server.start()
    bot_status.gateway_serving = _gateway_server.is_serving()

    # Emit startup Status Report and start the 30-minute reporting loop
    _dashboard_reporter.emit_startup()
    _dashboard_task = asyncio.create_task(_dashboard_reporter.run_loop())

    logger.info("=== TDbridge ready ===")


# ===========================================================================
# Shutdown
# ===========================================================================

_shutdown_called = False  # guard against double-call from signal handler + finally


async def _shutdown() -> None:
    """Graceful shutdown: stop background tasks, then Telegram app."""
    global _shutdown_called
    if _shutdown_called:
        return
    _shutdown_called = True
    logger.info("TDbridge shutting down…")

    # Emit shutdown Status Report before stopping
    _dashboard_reporter.emit_shutdown()
    _dashboard_reporter.stop()

    # Stop the gateway server (releases the listen port). No-op if not started.
    try:
        await _gateway_server.stop()
        bot_status.gateway_serving = False
    except Exception as e:
        logger.warning(f"Error during gateway server shutdown: {e}")

    bot_status.dc_connected = False

    # Flush any pending T_Group writes before the event loop stops
    try:
        await sheets_manager.flush_t_group_buffer()
        logger.info("T_Group buffer: flushed on shutdown")
    except Exception as e:
        logger.warning(f"T_Group buffer: shutdown flush failed: {e}")

    for task in [_sheets_refresh_task, _db_purge_task, _discord_refresh_task,
                 _dashboard_task, _t_group_flush_task]:
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    if _tg_app:
        try:
            await _tg_app.updater.stop()
            # Polling has now stopped; record how many routine 200 polls
            # occurred in the final window (counted but not individually logged).
            if _poll_counters is not None:
                log_poll_summary(_poll_counters, reason="shutdown")
            await _tg_app.stop()
            await _tg_app.shutdown()
        except Exception as e:
            logger.warning(f"Error during Telegram shutdown: {e}")

    logger.info("TDbridge shutdown complete")


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    global _discord_client

    client = TDbridgeDiscordClient(intents=_intents)
    _discord_client = client

    @client.event
    async def on_ready() -> None:
        # on_ready can fire again after a Discord reconnect; only run startup
        # once. A reconnect must not re-initialize tasks or re-run startup.
        global _startup_done
        if _startup_done:
            logger.info("Discord on_ready fired again (reconnect) — startup already done")
            return
        # Startup must succeed fully or the process should exit non-zero so
        # systemd restarts it (and eventually gives up if it keeps failing),
        # rather than leaving the bot connected to Discord but half-initialized
        # (e.g. a transient Telegram get_me() timeout during initialize()).
        # discord.py swallows exceptions raised in event handlers, so we cannot
        # rely on the exception propagating — we catch it and force-exit.
        try:
            await _startup(client)
            _startup_done = True
        except Exception as e:
            logger.critical(
                f"FATAL: startup failed ({type(e).__name__}: {e}). "
                f"Exiting non-zero so the service manager can restart.",
                exc_info=True,
            )
            # os._exit (not sys.exit) terminates immediately with a non-zero
            # code; SystemExit would itself be swallowed by discord.py's event
            # runner. There is nothing to clean up — the bot never became
            # operational.
            os._exit(1)

    async def runner() -> None:
        # Platform-aware shutdown signal handling.
        #
        # Linux (systemd): SIGTERM is sent by systemd when the service is
        # stopped.  We register an asyncio signal handler that triggers a
        # clean shutdown.  SIGINT (Ctrl-C) is also handled for convenience
        # when running manually on Linux.
        #
        # Windows: asyncio signal handlers are not supported on Windows.
        # KeyboardInterrupt (Ctrl-C) is the only available stop mechanism
        # during development.  The outer try/except in main() catches it.
        if config.platform == "Linux":
            import signal

            loop = asyncio.get_running_loop()

            def _handle_signal(sig_name: str) -> None:
                logger.info(f"Signal {sig_name} received — initiating shutdown")
                # Schedule shutdown as a task so it runs cleanly on the loop
                loop.create_task(_shutdown())
                # Stop the Discord client, which will cause runner() to exit
                loop.create_task(client.close())

            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig, _handle_signal, sig.name
                )
            logger.info("Signal handlers registered (Linux mode)")

        try:
            async with client:
                await client.start(config.discord_bot_token)
        finally:
            await _shutdown()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        # Windows development: Ctrl-C is the normal stop mechanism.
        # On Linux this path is not reached because SIGINT is handled above.
        logger.info("Keyboard interrupt received — exiting")


if __name__ == "__main__":
    main()
