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

# On Linux (server), Telegram updates are received via webhook — the bot runs
# its own HTTPS server and Telegram POSTs updates to it.
# On Windows (development), webhook mode is not used because the dev machine
# is not reachable at the public server address.  Polling mode is used instead:
# the bot periodically asks Telegram for new updates.  Polling is functionally
# identical for testing purposes; the only difference is the transport layer.
# See TDbridge_Project_Structure.md § "Platform differences" for full details.
_USE_POLLING = config.platform == "Windows"

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
_sheets_refresh_task: Optional[asyncio.Task] = None
_db_purge_task: Optional[asyncio.Task] = None
_discord_refresh_task: Optional[asyncio.Task] = None


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


def _resolve_discord_mentions(text: str) -> str:
    """Replace Discord @user and #channel mention tokens with readable names.

    Discord encodes mentions as:
        <@discord_user_id>    — user mention
        <@!discord_user_id>   — user mention (legacy nickname variant)
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
            dc_msg = await channel.send(
                content=content,
                files=files if files else discord.utils.MISSING,
                reference=reference,
                mention_author=False,
            )
        elif webhook is not None:
            # New message: use webhook for custom display name
            dc_msg = await webhook.send(
                content=content,
                username=f"{sender_name} [TG]",
                files=files if files else discord.utils.MISSING,
                wait=True,
            )
        else:
            # Fallback: no webhook available, use plain channel send
            dc_msg = await channel.send(
                content=content,
                files=files if files else discord.utils.MISSING,
            )
        return dc_msg
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
            fname = msg.video.file_name or "video.mp4"
            dc_files.append(discord.File(io.BytesIO(data), filename=fname))

        elif msg.voice:
            data = await _download_tg_file(tg_bot, msg.voice.file_id)
            dc_files.append(discord.File(io.BytesIO(data), filename="voice.ogg"))

        elif msg.audio:
            data = await _download_tg_file(tg_bot, msg.audio.file_id)
            fname = msg.audio.file_name or "audio.mp3"
            dc_files.append(discord.File(io.BytesIO(data), filename=fname))

        elif msg.document:
            fname = msg.document.file_name or "file"
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
                # Single item — use the regular send method, not send_media_group
                raw, att = chunk_pv[0]
                ctype_att = att.content_type or ""
                try:
                    if ctype_att.startswith("video/"):
                        sent = await tg_bot.send_video(
                            chat_id=chat_id,
                            video=raw,
                            caption=chunk_caption or None,
                            reply_to_message_id=reply_to_telegram_id,
                        )
                    else:
                        sent = await tg_bot.send_photo(
                            chat_id=chat_id,
                            photo=raw,
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
                # Multiple items — build InputMedia list with raw bytes
                media_list = []
                for i, (raw, att) in enumerate(chunk_pv):
                    ctype_att = att.content_type or ""
                    caption = chunk_caption if i == 0 else None
                    if ctype_att.startswith("video/"):
                        media_list.append(InputMediaVideo(media=raw, caption=caption))
                    else:
                        media_list.append(InputMediaPhoto(media=raw, caption=caption))
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
    sender_name = (
        msg.from_user.full_name if msg.from_user else tg_chat.title or "Unknown"
    )
    logger.info(
        f"TG→DC: received message {tg_msg_id} from '{sender_name}' "
        f"in group {tg_group_id} ('{tg_chat.title}')"
    )

    # ---- Upsert T_Group info and check admin status ----
    asyncio.ensure_future(sheets_manager.upsert_t_group(
        tg_group_id, tg_chat.title or "", tg_chat.type or "group"
    ))
    # Check admin status the first time we see a group (not on every message).
    # The T_Group cache tells us if this is a known group; if it's new, check.
    if not sheets_manager.get_tg_group(tg_group_id):
        asyncio.ensure_future(
            _check_bot_admin_status(context.bot, tg_chat.id, tg_chat.title or "")
        )

    # ---- Determine target Discord channel ----
    user_row = sheets_manager.get_user_by_tg_group(tg_group_id)
    # Log the raw chat.id type so we can confirm it arrives as a string
    # after _tg_group_id_str() conversion, and see exactly what value is
    # used for the cache lookup.
    _lookup_result = (
        f"found user row D_ID={user_row.get('D_ID')!r}"
        if user_row else "NOT FOUND in user_by_tg_group_id"
    )
    logger.info(
        f"TG→DC: cache lookup — "
        f"tg_group_id={tg_group_id!r} (type={type(tg_group_id).__name__}) → {_lookup_result}"
    )
    if user_row:
        dc_channel_id = str(user_row.get("D_ChannelID", "")).strip()
        dc_user_id    = str(user_row.get("D_ID", "")).strip()
        # Build @mention tag if we have a valid Discord user ID
        user_tag = f"<@{dc_user_id}>" if dc_user_id else ""
        logger.info(
            f"TG→DC: mapped to DC channel {dc_channel_id!r}, user {dc_user_id!r}"
        )
    else:
        # No mapping found — fall back to any Active Discord channel, or warn
        active_channels = sheets_manager.get_active_channels()
        logger.info(
            f"TG→DC: no user mapping for TG group {tg_group_id}; "
            f"active channels available: {[c.get('D_ChannelID') for c in active_channels]}"
        )
        if active_channels:
            dc_channel_id = active_channels[0]["D_ChannelID"]
        else:
            logger.warning(
                f"TG→DC: no active Discord channels found — cannot bridge message "
                f"from TG group {tg_group_id} ({tg_chat.title})"
            )
            return
        dc_user_id  = ""
        user_tag  = f"@ {tg_chat.title}"  # space prevents accidental Discord mention

    if not dc_channel_id:
        logger.warning(
            f"TG→DC: D_ChannelID is empty for TG group {tg_group_id}; skipping bridge"
        )
        return

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

    if msg.reply_to_message:
        parent_tg_id = _tg_msg_id_str(msg.reply_to_message.message_id)
        loop = asyncio.get_running_loop()
        parent_record = await loop.run_in_executor(
            None, db.find_by_tg, tg_group_id, parent_tg_id
        )
        if parent_record:
            discord_reply_to_id = int(parent_record["dc_message_id"])
            reply_to_tg_id      = parent_record["root_tg_msg_id"]
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
    )
    logger.info(
        f"TG→DC: bridged TG msg {tg_msg_id} → DC msg {dc_msg_id} "
        f"in #{channel.name}"
    )


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

    # Cascade 1: try to edit the original Discord message
    try:
        dc_msg = await channel.fetch_message(dc_msg_id)
        await dc_msg.edit(content=edit_prefix + new_text)
        logger.info(f"Edited Discord message {dc_msg_id} for TG edit {tg_msg_id}")
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
    except Exception as e:
        logger.error(f"Failed to post edit fallback to Discord: {e}")


async def route_tg_delete_to_discord(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle a Telegram message deletion and mirror it to Discord.

    Behavior is controlled by the env var TDBRIDGE_DELETE_BEHAVIOR
    (or its TEST_/PROD_ variant in .env):
        "delete"  — attempt to delete the Discord message (default)
        "notify"  — post a reply noting the deletion
        "ignore"  — do nothing (log only)
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
        logger.info(f"TG→DC reaction ignored (REACTIONS_TTOD=neither)")
        return

    try:
        ref_msg   = await channel.fetch_message(dc_msg_id)
        reference = ref_msg.to_reference(fail_if_not_exists=False)

        if behavior in ("react", "both"):
            # Add the emoji as a native Discord reaction on the original message.
            # Only standard Unicode emoji and custom Discord emoji are supported
            # by the add_reaction API; if the emoji is unsupported it raises
            # HTTPException which we catch gracefully.
            try:
                await ref_msg.add_reaction(emoji_str)
            except Exception as e:
                logger.warning(f"TG→DC: could not add native reaction {emoji_str!r}: {e}")

        if behavior in ("reply", "both"):
            await channel.send(
                content=f"{emoji_str} **{actor_name}** reacted to this message",
                reference=reference,
                mention_author=False,
            )
    except Exception as e:
        logger.warning(f"Failed to bridge TG reaction to Discord: {e}")


# ===========================================================================
# Routing: Discord → Telegram
# ===========================================================================

class TDbridgeDiscordClient(discord.Client):
    """Discord client with message and reaction event handlers."""

    async def on_ready(self) -> None:
        logger.info(
            f"Discord bot ready: {self.user} (id={self.user.id})"
        )
        # Set the bot's nickname in every guild it belongs to, as configured
        # in .env (e.g. TEST_DISCORD_BOT_NICKNAME="TDbridge TESTING").
        # Nickname is per-guild, so we loop over all guilds.
        if config.discord_bot_nickname:
            for guild in self.guilds:
                try:
                    await guild.me.edit(nick=config.discord_bot_nickname)
                    logger.info(
                        f"Set nickname to '{config.discord_bot_nickname}' in guild '{guild.name}'"
                    )
                except discord.Forbidden:
                    logger.warning(
                        f"No permission to set nickname in guild '{guild.name}'"
                    )
                except Exception as e:
                    logger.warning(f"Could not set nickname in '{guild.name}': {e}")

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

        # Case 2: New message — find the first tagged user (in the order they
        # appear in the message text) who is Active and has a T_GroupID.
        #
        # IMPORTANT: message.mentions is an UNORDERED collection — Discord does
        # not guarantee it matches the left-to-right order of @mentions in the
        # text.  To respect the order the sender wrote them, we scan
        # message.content for <@id> tokens using a regex and look each one up
        # in turn.  This ensures "@Angel @Boont" routes to Angel, not Boont.
        if not tg_group_id and message.mentions:
            import re as _re
            # Extract mention IDs in the order they appear in the message text.
            # Both <@id> and <@!id> (legacy nickname variant) are matched.
            mention_ids_in_order = _re.findall(r"<@!?(\d+)>", message.content)
            # Deduplicate while preserving order (same user mentioned twice)
            seen_uids: set[str] = set()
            ordered_uids = []
            for uid in mention_ids_in_order:
                if uid not in seen_uids:
                    seen_uids.add(uid)
                    ordered_uids.append(uid)

            for uid in ordered_uids:
                user_row = sheets_manager.get_user_by_discord_id(uid)
                if not user_row:
                    logger.info(f"DC→TG routing: tagged user {uid} not in Sheets cache, skipping")
                    continue
                status = user_row.get("D_UserStatus", "")
                tgid = str(user_row.get("T_GroupID", "")).strip()
                user_channel = str(user_row.get("D_ChannelID", "")).strip()
                if not tgid:
                    logger.info(f"DC→TG routing: tagged user {uid} has no T_GroupID, skipping")
                    continue
                if not sheets_manager._is_active(status):
                    logger.info(
                        f"DC→TG routing: tagged user {uid} is not Active "
                        f"(D_UserStatus={status!r}), skipping"
                    )
                    continue
                if user_channel and user_channel != incoming_channel_id:
                    logger.info(
                        f"DC→TG routing: tagged user {uid} D_ChannelID={user_channel!r} "
                        f"does not match incoming channel {incoming_channel_id!r}, skipping"
                    )
                    continue
                tg_group_id = tgid
                logger.info(
                    f"DC→TG routing: routing to tagged user {uid} "
                    f"→ TG group {tg_group_id} via channel {incoming_channel_id}"
                )
                break

        # Case 3: Sender is a mapped Active user with their own group,
        # and the message was sent from their assigned D_ChannelID.
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
                        f"DC→TG routing: no active tagged user found; "
                        f"routing to sender {sender_uid} → TG group {tg_group_id} "
                        f"via channel {incoming_channel_id}"
                    )

        # Unroutable — channel is Active but no Telegram group could be found.
        # Behavior is controlled by UNROUTABLE_BEHAVIOR in .env:
        #   "warn"   — log WARNING + post a notice in Discord
        #   "ignore" — log INFO only, no Discord message
        if not tg_group_id:
            unroutable_msg = (
                f"DC→TG: unroutable message in #{message.channel.name} "
                f"from {str(message.author.id)} — "
                f"no active TG group found for sender or tagged users"
            )
            if config.unroutable_behavior == "warn":
                logger.warning(unroutable_msg)
                try:
                    reference = message.to_reference(fail_if_not_exists=False)
                    await message.channel.send(
                        "⚠️ Could not route this message to Telegram.",
                        reference=reference,
                        mention_author=False,
                    )
                except Exception:
                    pass
            else:
                logger.info(unroutable_msg)
            return

        # ---- Build message text ----
        # Resolve <@id> and <#id> Discord mention tokens to readable names
        # before sending to Telegram, where raw snowflake IDs are meaningless.
        resolved_content = _resolve_discord_mentions(message.content)
        text = f"{attribution} {resolved_content}".strip()

        # ---- Send to Telegram ----
        tg_bot: TelegramBot = _tg_app.bot

        reply_to_telegram_id: Optional[int] = None
        if root_tg_msg_id:
            try:
                reply_to_telegram_id = int(root_tg_msg_id)
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
                tg_msg_id = _tg_msg_id_str(sent_ids[0]) if sent_ids else None
            else:
                sent = await tg_bot.send_message(
                    chat_id=int(tg_group_id),
                    text=text,
                    reply_to_message_id=reply_to_telegram_id,
                )
                tg_msg_id = _tg_msg_id_str(sent.message_id)

            # Store mapping (only if we have a Telegram message ID to map to)
            if tg_msg_id:
                await loop.run_in_executor(
                    None,
                    db.store_message,
                    tg_group_id,
                    tg_msg_id,
                    dc_channel_id,
                    dc_msg_id,
                    root_tg_msg_id or tg_msg_id,
                    str(message.author.id),
                )

        except Exception as e:
            logger.error(f"Failed to send Discord message to Telegram group {tg_group_id}: {e}")

    async def on_message_edit(
        self, before: discord.Message, after: discord.Message
    ) -> None:
        """Bridge a Discord message edit to Telegram."""
        if after.author == self.user or after.webhook_id:
            return

        dc_channel_id = str(after.channel.id)
        dc_msg_id     = str(after.id)
        sender_name   = (
            after.author.nick
            or after.author.display_name
            or after.author.name
        )

        loop = asyncio.get_running_loop()
        record = await loop.run_in_executor(None, db.find_by_dc, dc_channel_id, dc_msg_id)
        if not record:
            return

        tg_group_id  = record["tg_group_id"]
        tg_msg_id    = int(record["tg_message_id"])
        resolved_edit = _resolve_discord_mentions(after.content)
        new_text      = f"✏️ EDIT — 👤 {sender_name} (Discord): {resolved_edit}"

        tg_bot: TelegramBot = _tg_app.bot

        # Cascade 1: try to edit the Telegram message
        try:
            await tg_bot.edit_message_text(
                chat_id=int(tg_group_id),
                message_id=tg_msg_id,
                text=new_text,
            )
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
        except Exception as e:
            logger.error(f"Failed to post edit fallback to Telegram: {e}")

    async def on_message_delete(self, message: discord.Message) -> None:
        """Bridge a Discord message deletion to Telegram."""
        dc_channel_id = str(message.channel.id)
        dc_msg_id     = str(message.id)

        loop = asyncio.get_running_loop()
        record = await loop.run_in_executor(None, db.find_by_dc, dc_channel_id, dc_msg_id)
        if not record:
            return

        tg_group_id = record["tg_group_id"]
        tg_msg_id   = int(record["tg_message_id"])
        tg_bot: TelegramBot = _tg_app.bot

        behavior = os.getenv(
            config.env_prefix + "DELETE_BEHAVIOR",
            os.getenv("DELETE_BEHAVIOR", "delete"),
        ).lower()

        if behavior == "ignore":
            logger.info(
                f"Discord delete ignored (DELETE_BEHAVIOR=ignore): "
                f"dc_msg {dc_msg_id} → tg_msg {tg_msg_id}"
            )
            return

        if behavior == "notify":
            try:
                await tg_bot.send_message(
                    chat_id=int(tg_group_id),
                    text="[A Discord message was deleted]",
                    reply_to_message_id=tg_msg_id,
                )
            except Exception as e:
                logger.warning(f"Failed to post delete notification to Telegram: {e}")
            return

        # Default: attempt deletion
        try:
            await tg_bot.delete_message(
                chat_id=int(tg_group_id),
                message_id=tg_msg_id,
            )
            await loop.run_in_executor(None, db.delete_by_dc, dc_channel_id, dc_msg_id)
            logger.info(f"Deleted TG message {tg_msg_id} (Discord message {dc_msg_id} deleted)")
        except Exception as e:
            logger.warning(f"Failed to delete TG message {tg_msg_id}: {e}")
            # Case 4: deletion failed for another reason — try notify
            notify_on_fail = os.getenv(
                config.env_prefix + "DELETE_FAIL_NOTIFY", "true"
            ).lower() == "true"
            if notify_on_fail:
                try:
                    await tg_bot.send_message(
                        chat_id=int(tg_group_id),
                        text="[Discord message was deleted — Telegram deletion failed]",
                        reply_to_message_id=tg_msg_id,
                    )
                except Exception:
                    pass

    async def on_reaction_add(
        self, reaction: discord.Reaction, user: discord.User
    ) -> None:
        """Bridge a Discord reaction to Telegram as a reply message."""
        if user == self.user:
            return

        dc_channel_id = str(reaction.message.channel.id)
        dc_msg_id     = str(reaction.message.id)
        user_name     = (
            getattr(user, "nick", None)
            or user.display_name
            or user.name
        )

        loop = asyncio.get_running_loop()
        record = await loop.run_in_executor(None, db.find_by_dc, dc_channel_id, dc_msg_id)
        if not record:
            return

        tg_group_id = record["tg_group_id"]
        tg_msg_id   = int(record["tg_message_id"])
        emoji_str   = str(reaction.emoji)

        behavior = config.reactions_dtot
        if behavior == "neither":
            logger.info(f"DC→TG reaction ignored (REACTIONS_DTOT=neither)")
            return

        tg_bot: TelegramBot = _tg_app.bot

        if behavior in ("react", "both"):
            # Telegram's sendReaction API requires a ReactionTypeEmoji object.
            # Only a limited set of emoji are allowed by Telegram; unsupported
            # emoji fall back gracefully to the reply method if "both" is set,
            # or log a warning if "react" only.
            from telegram import ReactionTypeEmoji
            try:
                await tg_bot.set_message_reaction(
                    chat_id=int(tg_group_id),
                    message_id=tg_msg_id,
                    reaction=[ReactionTypeEmoji(emoji=emoji_str)],
                )
            except Exception as e:
                logger.warning(f"DC→TG: could not add native reaction {emoji_str!r}: {e}")
                if behavior == "react":
                    return  # don't fall through to reply if react-only was requested

        if behavior in ("reply", "both"):
            try:
                await tg_bot.send_message(
                    chat_id=int(tg_group_id),
                    text=f"{emoji_str} {user_name} (Discord) reacted to this message",
                    reply_to_message_id=tg_msg_id,
                )
            except Exception as e:
                logger.warning(f"Failed to bridge Discord reaction to Telegram: {e}")


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

    logger.info(
        f"Discord scan complete: {len(all_users)} unique users, "
        f"{len(all_channels)} readable channels"
    )

    # ── Write phase: one batched read-modify-write per table ─────────────────
    if all_users:
        await sheets_manager.batch_upsert_d_users(all_users)

    if all_channels:
        await sheets_manager.batch_upsert_d_channels(all_channels)

    logger.info(
        f"Discord → Sheets refresh complete: "
        f"{len(all_users)} users, {len(all_channels)} channels written"
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
        logger.info(sheets_manager.status_summary())


async def _db_purge_loop() -> None:
    """Purge old SQLite records once per day."""
    TWENTY_FOUR_HOURS = 86400
    while True:
        await asyncio.sleep(TWENTY_FOUR_HOURS)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, db.purge_older_than, 30)


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
    global _tg_app

    tg_app = (
        ApplicationBuilder()
        .token(config.telegram_bot_token)
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
        logger.info(
            f"Telegram polling started (Windows development mode) "
            f"— allowed_updates={_ALLOWED_UPDATES}"
        )
    else:
        # ---- Linux: webhook mode ----
        # Verify TLS cert files are readable before starting the server.
        # Fail loudly here rather than with a cryptic SSL error later.
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
        logger.info(f"TLS certificate loaded: {config.tls_cert_file}")

        await tg_app.bot.set_webhook(
            url=config.telegram_webhook_url,
            secret_token=config.telegram_webhook_secret or None,
            allowed_updates=_ALLOWED_UPDATES,
        )
        logger.info(
            f"Telegram webhook registered: {config.telegram_webhook_url} "
            f"— allowed_updates={_ALLOWED_UPDATES}"
        )

        # In python-telegram-bot v21, ssl_context is no longer accepted by
        # start_webhook().  Instead, pass the cert and key file paths directly;
        # PTB builds its own SSLContext internally from these paths.
        await tg_app.updater.start_webhook(
            listen="0.0.0.0",
            port=config.telegram_webhook_port,
            secret_token=config.telegram_webhook_secret or None,
            webhook_url=config.telegram_webhook_url,
            key=config.tls_key_file,
            cert=config.tls_cert_file,
        )
        logger.info(
            f"Telegram webhook HTTPS server listening on port {config.telegram_webhook_port}"
        )

    _tg_app = tg_app


async def _startup(discord_client: discord.Client) -> None:
    """Run all startup tasks after Discord is connected and ready."""
    global _sheets_refresh_task, _db_purge_task, _discord_refresh_task

    logger.info("=== TDbridge startup ===")
    logger.info(f"Environment : {config.env.upper()}")
    logger.info(f"Discord bot : {config.discord_bot_name}")
    logger.info(f"Telegram bot: {config.telegram_bot_name} (@{config.telegram_bot_username})")
    logger.info(f"Spreadsheet : {config.google_spreadsheet_name}")
    logger.info(f"SQLite DB   : {config.sqlite_db_file}")

    # Initialise SQLite
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, db.init_db)

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

    logger.info("=== TDbridge ready ===")


# ===========================================================================
# Shutdown
# ===========================================================================

async def _shutdown() -> None:
    """Graceful shutdown: stop background tasks, then Telegram app."""
    logger.info("TDbridge shutting down…")

    for task in [_sheets_refresh_task, _db_purge_task, _discord_refresh_task]:
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    if _tg_app:
        try:
            await _tg_app.updater.stop()
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
        await _startup(client)

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
