"""
TDbridge Userbot — Bridge (the two-way glue)

The only module that imports BOTH the Telegram adapter (userbot_telegram) and
the gateway wrapper (userbot_gateway). It translates in both directions and does
nothing else: no routing, no attribution resolution, no mapping decisions —
TDbridge owns all of that. If this module starts growing that kind of logic, it
has thickened and should be pared back.

Directions:

  Inbound to bridge (Telegram -> gateway):
    A relayable bot message (from userbot_telegram's filter) becomes a gateway
    'message' event. Media is downloaded from Telegram and uploaded to the
    server's store, then referenced. The REAL Telegram message id is supplied
    (Echo=false: the message is already in Telegram; the server must not repost).

  Outbound from bridge (gateway -> Telegram):
    Poll events (message/edited_message/reaction/deletion) become Telethon
    actions, performed as the user account, paced by the OutboundMeter.

Also hosts the gateway-mediated login code_provider: during login it sends a
prompt out over the gateway (as a message from the primary group) and awaits the
human's reply arriving back inbound, via a one-shot future. This is why the poll
loop checks for a pending login waiter before treating an inbound message
normally.

Loop discipline (mirrors TDbridge's own): dedupe inbound by (chat_id,
message_id) so we never act twice on the same event, and never relay back out an
event we received from the gateway.
"""

import asyncio
import logging
import mimetypes
from typing import Optional

import gateway_protocol as gp

logger = logging.getLogger("userbot_bridge")


class UserbotBridge:
    def __init__(self, telegram, gateway, *, outbox, primary_group_id: str = ""):
        self._tg = telegram          # UserbotTelegram
        self._gw = gateway           # UserbotGateway
        self._outbox = outbox        # Outbox (persistent FIFO drain worker)
        self._primary_group_id = primary_group_id

        # One-shot login waiter: set to an asyncio.Future while a gateway-
        # mediated login is awaiting the code reply; the inbound path fulfills
        # it with the reply text instead of relaying that message.
        self._login_waiter: Optional[asyncio.Future] = None

        # Dedupe set of (chat_id, message_id) we've already processed inbound,
        # so a redelivered event (pre-ack) isn't acted on twice.
        self._seen_inbound: set = set()

    # ================================================================= #
    # Wire the Telegram inbound handler                                  #
    # ================================================================= #
    def attach(self) -> None:
        """Register our inbound-from-Telegram handler with the Telegram adapter."""
        self._tg.on_bot_message(self._on_telegram_bot_message)

    # ================================================================= #
    # Inbound: Telegram bot message -> gateway                           #
    # ================================================================= #
    async def _on_telegram_bot_message(self, norm: dict) -> None:
        """A relayable bot message arrived in Telegram. Relay it over the
        gateway with its real Telegram id (Echo=false)."""
        chat_id = norm["chat_id"]
        message_id = norm["message_id"]

        # Upload media, if any, to the server's store and reference it.
        attachments = []
        if norm.get("has_media"):
            data = await self._tg.download_attachment(norm["_event"])
            if data is not None:
                fname = norm.get("media_filename") or "attachment"
                mime = mimetypes.guess_type(fname)[0] or "application/octet-stream"
                try:
                    up = await self._gw.upload_file(data, fname, mime)
                    attachments.append(gp.Attachment(
                        file_ref=up["file_ref"],
                        file_name=up.get("file_name", fname),
                        mime_type=up.get("mime_type", mime),
                        size=up.get("size", len(data)),
                    ))
                except Exception as e:
                    logger.warning("TG->GW upload failed for %s: %s", fname, e)

        try:
            await self._gw.send_message(
                int(chat_id),
                text=norm.get("text") or "",
                message_id=int(message_id),          # real id; Echo=false
                reply_to=norm.get("reply_to"),
                attachments=attachments or None,
            )
            logger.info(
                "TG->GW relayed | chat=%s | msg=%s | from=%s | attachments=%d",
                chat_id, message_id, norm.get("sender_username"), len(attachments),
            )
        except Exception as e:
            logger.warning("TG->GW send failed | chat=%s msg=%s: %s",
                           chat_id, message_id, e)

    # ================================================================= #
    # Inbound: gateway events -> Telegram (the poll-loop callback)       #
    # ================================================================= #
    async def on_gateway_events(self, events: list) -> None:
        for ev in events:
            try:
                await self._handle_one_event(ev)
            except Exception as e:
                logger.exception("error handling gateway event: %s", e)

    async def _handle_one_event(self, ev: dict) -> None:
        etype = ev.get("event_type", "?")
        payload = ev.get("payload", {}) or {}
        chat = payload.get("chat", {}) or {}
        chat_id = chat.get("id")

        # --- Login interception: if a gateway-mediated login is awaiting a
        # code, the first inbound message's text IS the code. Consume it and
        # do NOT relay it as a normal action. ---
        if (self._login_waiter is not None
                and not self._login_waiter.done()
                and etype in ("message", "edited_message")):
            code_text = (payload.get("text") or "").strip()
            if code_text:
                self._login_waiter.set_result(code_text)
                await self._ack(chat_id, self._ids(payload))
                return

        # --- Dedupe by (chat_id, message_id[s]) ---
        ids = self._ids(payload)
        fresh = [i for i in ids if (chat_id, i) not in self._seen_inbound]
        for i in fresh:
            self._seen_inbound.add((chat_id, i))
        if etype in ("message", "edited_message") and not fresh:
            await self._ack(chat_id, ids)   # already handled; just re-ack
            return

        # --- Dispatch: enqueue the outbound Telegram action, then ack ---
        # We enqueue (durably) and ack immediately. The outbox worker performs
        # the action later, paced by metering and resilient to FLOOD. "Acked"
        # means "durably received", NOT "already performed" — this is what lets
        # polling continue at full speed while the queue drains.
        if etype in ("message", "edited_message"):
            await self._outbox.enqueue(chat_id, etype, self._msg_action(payload))
        elif etype == "reaction":
            await self._outbox.enqueue(chat_id, "reaction", self._reaction_action(payload))
        elif etype == "deletion":
            await self._outbox.enqueue(chat_id, "deletion", self._deletion_action(payload))
        else:
            logger.info("ignoring unknown gateway event_type=%s", etype)

        await self._ack(chat_id, ids)

    # ---- Build serializable action payloads for the outbox -------------- #
    # These capture everything perform_action() needs; attachments are kept as
    # file_refs (bytes are fetched from the server store at perform time, so the
    # queue stays small and the fetch-once discipline is honored on send).
    @staticmethod
    def _msg_action(payload: dict) -> dict:
        return {
            "text": payload.get("text") or "",
            "reply_to": payload.get("reply_to"),
            "message_id": payload.get("message_id"),
            "attachments": payload.get("attachments") or [],
        }

    @staticmethod
    def _reaction_action(payload: dict) -> dict:
        emoji_list = payload.get("emoji") or []
        return {
            "message_id": payload.get("message_id"),
            "emoji": emoji_list[0] if emoji_list else "",
        }

    @staticmethod
    def _deletion_action(payload: dict) -> dict:
        ids = payload.get("message_ids") or (
            [payload["message_id"]] if payload.get("message_id") else []
        )
        return {"message_ids": [int(i) for i in ids]}

    # ---- perform_action: called by the outbox worker at send time ------- #
    async def perform_action(self, action_type: str, chat_id, payload: dict) -> None:
        """Perform one queued outbound Telegram action. Raises FloodWaitError on
        a flood (the outbox re-appends); other exceptions propagate (dropped)."""
        if action_type == "edited_message":
            mid = payload.get("message_id")
            if mid is not None:
                await self._tg.edit_text(chat_id, int(mid), payload.get("text") or "")
            return
        if action_type == "message":
            text = payload.get("text") or ""
            reply_to = payload.get("reply_to")
            attachments = payload.get("attachments") or []
            if attachments:
                first = True
                for a in attachments:
                    data = await self._download_ref(a)
                    if data is None:
                        continue
                    caption = text if first else ""
                    await self._tg.send_file(
                        chat_id, data, filename=a.get("file_name"),
                        caption=caption, reply_to=reply_to,
                    )
                    first = False
                if first and text:
                    await self._tg.send_text(chat_id, text, reply_to=reply_to)
            else:
                await self._tg.send_text(chat_id, text, reply_to=reply_to)
            return
        if action_type == "reaction":
            mid = payload.get("message_id")
            if mid is not None:
                await self._tg.add_reaction(chat_id, int(mid), payload.get("emoji") or "")
            return
        if action_type == "deletion":
            ids = payload.get("message_ids") or []
            if ids:
                await self._tg.delete_messages(chat_id, [int(i) for i in ids])
            return
        logger.warning("perform_action: unknown action_type=%s", action_type)
        """Fetch an attachment's bytes from the server store by file_ref."""
        try:
            return await self._gw.download_file(att["file_ref"])
        except Exception as e:
            logger.warning("GW->TG attachment fetch failed for %s: %s",
                           att.get("file_name", "?"), e)
            return None

    # ---- helpers --------------------------------------------------------- #
    @staticmethod
    def _ids(payload: dict) -> list:
        ids = payload.get("message_ids")
        if ids:
            return list(ids)
        single = payload.get("message_id")
        return [single] if single is not None else []

    async def _ack(self, chat_id, ids) -> None:
        if chat_id is None or not ids:
            return
        try:
            await self._gw.ack(int(chat_id), [int(i) for i in ids])
        except Exception as e:
            logger.debug("ack failed (non-fatal): %s", e)

    # ================================================================= #
    # Gateway-mediated login code provider                               #
    # ================================================================= #
    def make_gateway_code_provider(self, *, wait_min: int):
        """Return a code_provider (async, no-arg) that prompts over the gateway
        and awaits the human's reply. Raises asyncio.TimeoutError if no reply
        arrives within wait_min minutes (so ensure_logged_in re-requests)."""
        async def _provider() -> str:
            if not self._primary_group_id:
                raise RuntimeError(
                    "Gateway-mediated login needs USERBOT_PRIMARY_GROUP set."
                )
            loop = asyncio.get_running_loop()
            self._login_waiter = loop.create_future()
            # Send the prompt out over the gateway, sourced from the primary
            # group so TDbridge routes it to that group's Discord channel/tag.
            prompt = ("🔐 TDbridge userbot login: reply to this message with the "
                      "Telegram login code just sent to the userbot's phone.")
            try:
                await self._gw.send_message(
                    int(self._primary_group_id),
                    text=prompt,
                    message_id=None,   # let the server post it (Echo path)
                )
            except Exception as e:
                self._login_waiter = None
                raise RuntimeError(f"could not send login prompt: {e}") from e
            try:
                code = await asyncio.wait_for(
                    self._login_waiter, timeout=wait_min * 60
                )
                return code
            except asyncio.TimeoutError:
                raise
            finally:
                self._login_waiter = None
        return _provider
