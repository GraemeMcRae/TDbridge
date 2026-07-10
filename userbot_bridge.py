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
    def __init__(self, telegram, gateway, *, outbox):
        self._tg = telegram          # UserbotTelegram
        self._gw = gateway           # UserbotGateway
        self._outbox = outbox        # Outbox (persistent FIFO drain worker)

        # Dedupe set of event_ids we've already processed inbound, so a
        # redelivered event (pre-ack) isn't acted on twice.
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
        # The server-assigned event_id is the stable identity: always present on
        # outbound events, and the handle we ACK and dedupe by (a freshly relayed
        # message has message_id=None, so message-id keying cannot work here).
        event_id = ev.get("event_id")

        if event_id is None:
            # Should not happen with a v2 server; log and skip to avoid a loop
            # (we cannot ack what we cannot name).
            logger.warning("GW event without event_id (cannot ack): %s", etype)
            return

        # --- Dedupe by event_id ---
        if event_id in self._seen_inbound:
            await self._ack_events([event_id])   # already handled; just re-ack
            logger.info("GW event re-acked (dup) | event_id=%s", event_id)
            return
        self._seen_inbound.add(event_id)

        # --- Dispatch: enqueue the outbound Telegram action, then ack ---
        # We enqueue (durably) and ack immediately. The outbox worker performs
        # the action later, paced by metering and resilient to FLOOD. "Acked"
        # means "durably received", NOT "already performed" — this is what lets
        # polling continue at full speed while the queue drains.
        _txt = (payload.get("text") or "").replace("\n", "\\n")
        if len(_txt) > 200:
            _txt = _txt[:200] + "…"
        if etype in ("message", "edited_message"):
            await self._outbox.enqueue(chat_id, etype, self._msg_action(payload),
                                       event_id=event_id)
            logger.info(
                "GW->TG enqueued | event_id=%s | event=%s | chat=%s | reply_to=%s | "
                "attachments=%d | text='%s'",
                event_id, etype, chat_id, payload.get("reply_to"),
                len(payload.get("attachments") or []), _txt,
            )
        elif etype == "reaction":
            await self._outbox.enqueue(chat_id, "reaction", self._reaction_action(payload),
                                       event_id=event_id)
            logger.info(
                "GW->TG enqueued | event_id=%s | event=reaction | chat=%s | msg_id=%s | emoji=%s",
                event_id, chat_id, payload.get("message_id"), payload.get("emoji"),
            )
        elif etype == "deletion":
            await self._outbox.enqueue(chat_id, "deletion", self._deletion_action(payload),
                                       event_id=event_id)
            logger.info(
                "GW->TG enqueued | event_id=%s | event=deletion | chat=%s | msg_ids=%s",
                event_id, chat_id, self._ids(payload),
            )
        else:
            logger.info("ignoring unknown gateway event_type=%s (event_id=%s)", etype, event_id)

        await self._ack_events([event_id])
        logger.info("GW event acked | event_id=%s", event_id)

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
    async def perform_action(self, action_type: str, chat_id, payload: dict) -> list:
        """Perform one queued outbound Telegram action. Returns the Telegram
        target id(s) it produced (for correlation): a message → [new_id]; a
        media group → [id, id, ...]; a bare reaction/edit/deletion → [] (no new
        message). Raises FloodWaitError on a flood (the outbox re-appends);
        other exceptions propagate (dropped)."""
        if action_type == "edited_message":
            mid = payload.get("message_id")
            if mid is not None:
                await self._tg.edit_text(chat_id, int(mid), payload.get("text") or "")
            return []
        if action_type == "message":
            text = payload.get("text") or ""
            reply_to = payload.get("reply_to")
            attachments = payload.get("attachments") or []
            produced = []
            _tprev = text.replace("\n", "\\n")
            if len(_tprev) > 200:
                _tprev = _tprev[:200] + "…"
            if attachments:
                first = True
                for a in attachments:
                    data = await self._download_ref(a)
                    if data is None:
                        continue
                    caption = text if first else ""
                    new_id = await self._tg.send_file(
                        chat_id, data, filename=a.get("file_name"),
                        caption=caption, reply_to=reply_to,
                    )
                    produced.append(new_id)
                    logger.info(
                        "TG POST (file) | chat=%s | new_tg_msg=%s | reply_to=%s | "
                        "file=%s | caption='%s'",
                        chat_id, new_id, reply_to, a.get("file_name"),
                        _tprev if first else "",
                    )
                    first = False
                if first and text:
                    nid = await self._tg.send_text(chat_id, text, reply_to=reply_to)
                    produced.append(nid)
            else:
                new_id = await self._tg.send_text(chat_id, text, reply_to=reply_to)
                produced.append(new_id)
                logger.info(
                    "TG POST (text) | chat=%s | new_tg_msg=%s | reply_to=%s | text='%s'",
                    chat_id, new_id, reply_to, _tprev,
                )
            return produced
        if action_type == "reaction":
            mid = payload.get("message_id")
            if mid is not None:
                await self._tg.add_reaction(chat_id, int(mid), payload.get("emoji") or "")
                logger.info("TG REACT | chat=%s | tg_msg=%s | emoji=%s",
                            chat_id, mid, payload.get("emoji"))
            return []
        if action_type == "deletion":
            ids = payload.get("message_ids") or []
            if ids:
                await self._tg.delete_messages(chat_id, [int(i) for i in ids])
                logger.info("TG DELETE | chat=%s | tg_msgs=%s", chat_id, ids)
            return []
        logger.warning("perform_action: unknown action_type=%s", action_type)
        return []

    async def send_correlate(self, event_id: int, telegram_ids: list) -> None:
        """Report to the server that outbound `event_id` became telegram_ids
        (empty list = deliberately nothing). Called by the outbox after a
        successful post, in ACK/post order."""
        try:
            await self._gw.send_correlate(int(event_id), [int(t) for t in telegram_ids])
            logger.info("GW correlate sent | event_id=%s | telegram_ids=%s",
                        event_id, telegram_ids)
        except Exception as e:
            logger.warning("correlate send failed for event_id=%s: %s", event_id, e)

    async def _download_ref(self, att: dict):
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

    async def _ack_events(self, event_ids) -> None:
        if not event_ids:
            return
        try:
            await self._gw.ack([int(e) for e in event_ids])
        except Exception as e:
            logger.debug("ack failed (non-fatal): %s", e)

