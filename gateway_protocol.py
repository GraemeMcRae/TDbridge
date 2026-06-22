"""
gateway_protocol.py — TDbridge Gateway Protocol wire format.

Defines the on-the-wire event envelope and payloads exchanged between gateway
peers, and the serialize/deserialize functions both the server and client use.
This is the single source of truth for the contract described in
TDbridge_Gateway_Protocol.md; both halves of the gateway depend on it.

Design notes:
  • Standard library only (no bot runtime, no httpx) so it can be imported by
    either side and unit-tested in isolation.
  • Payloads mirror Telegram Bot API object shapes (Message / User /
    MessageReactionUpdated) to minimise work for a Telegram-aware peer.
  • Dataclasses model each payload; to_dict()/from_dict() handle the wire form.
  • Parsing is strict where it matters (envelope structure, event_type,
    protocol_version, secret presence on inbound) and lenient about optional
    fields, which default sensibly.

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# Wire-protocol version this build speaks. Kept in sync with
# gateway_config.GATEWAY_PROTOCOL_VERSION (imported there as the canonical
# value); duplicated as a module constant for callers that only import this.
PROTOCOL_VERSION = 1

# The five event types carried by the protocol.
EVENT_MESSAGE = "message"
EVENT_EDITED_MESSAGE = "edited_message"
EVENT_REACTION = "reaction"
EVENT_DELETION = "deletion"
EVENT_ACK = "ack"

_EVENT_TYPES = frozenset({
    EVENT_MESSAGE,
    EVENT_EDITED_MESSAGE,
    EVENT_REACTION,
    EVENT_DELETION,
    EVENT_ACK,
})


class GatewayProtocolError(Exception):
    """Raised when an envelope or payload is malformed or violates the contract."""


# --------------------------------------------------------------------------- #
# Small helpers                                                               #
# --------------------------------------------------------------------------- #
def _require(d: Dict[str, Any], key: str, where: str) -> Any:
    if key not in d:
        raise GatewayProtocolError(f"{where}: missing required field '{key}'")
    return d[key]


def _as_int(value: Any, where: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise GatewayProtocolError(f"{where}: expected integer, got {value!r}")


# --------------------------------------------------------------------------- #
# Sub-objects                                                                 #
# --------------------------------------------------------------------------- #
@dataclass
class User:
    """Telegram-User-shaped, NON-AUTHORITATIVE sender info.

    Informational only — MUST NOT be used for routing. is_synthetic=True means
    the identity was synthesised (e.g. from a Discord user) and id is not a real
    Telegram user id.
    """
    id: Optional[int] = None
    first_name: str = ""
    username: Optional[str] = None
    is_synthetic: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "first_name": self.first_name,
            "username": self.username,
            "is_synthetic": self.is_synthetic,
        }

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> Optional["User"]:
        if d is None:
            return None
        return cls(
            id=d.get("id"),
            first_name=d.get("first_name", "") or "",
            username=d.get("username"),
            is_synthetic=bool(d.get("is_synthetic", False)),
        )


@dataclass
class Attachment:
    """A two-hop file reference (see protocol §6). Bytes travel separately via
    upload/getfile; the message carries only this reference."""
    file_ref: str
    file_name: str = ""
    mime_type: str = ""
    size: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_ref": self.file_ref,
            "file_name": self.file_name,
            "mime_type": self.mime_type,
            "size": self.size,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Attachment":
        ref = d.get("file_ref")
        if not ref:
            raise GatewayProtocolError("attachment: missing 'file_ref'")
        return cls(
            file_ref=str(ref),
            file_name=d.get("file_name", "") or "",
            mime_type=d.get("mime_type", "") or "",
            size=int(d.get("size", 0) or 0),
        )


# --------------------------------------------------------------------------- #
# Payloads (one per event_type)                                               #
# --------------------------------------------------------------------------- #
@dataclass
class MessagePayload:
    """Payload for 'message' and 'edited_message' events.

    chat_id is the Telegram T_GroupID — the SOLE routing key. message_id is the
    correlation id (real Telegram id when Echo=true; client-supplied when
    Echo=false). media_group_id groups members of one multi-attachment message.
    """
    chat_id: int
    message_id: Optional[int] = None
    media_group_id: Optional[str] = None
    from_user: Optional[User] = None
    date: Optional[int] = None
    text: Optional[str] = None
    reply_to: Optional[int] = None
    attachments: List[Attachment] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chat": {"id": self.chat_id},
            "message_id": self.message_id,
            "media_group_id": self.media_group_id,
            "from": self.from_user.to_dict() if self.from_user else None,
            "date": self.date,
            "text": self.text,
            "reply_to": self.reply_to,
            "attachments": [a.to_dict() for a in self.attachments],
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MessagePayload":
        chat = _require(d, "chat", "message payload")
        if not isinstance(chat, dict) or "id" not in chat:
            raise GatewayProtocolError("message payload: 'chat' must be an object with 'id'")
        atts = d.get("attachments") or []
        if not isinstance(atts, list):
            raise GatewayProtocolError("message payload: 'attachments' must be an array")
        return cls(
            chat_id=_as_int(chat["id"], "message payload chat.id"),
            message_id=(None if d.get("message_id") is None else _as_int(d["message_id"], "message_id")),
            media_group_id=d.get("media_group_id"),
            from_user=User.from_dict(d.get("from")),
            date=(None if d.get("date") is None else _as_int(d["date"], "date")),
            text=d.get("text"),
            reply_to=(None if d.get("reply_to") is None else _as_int(d["reply_to"], "reply_to")),
            attachments=[Attachment.from_dict(a) for a in atts],
        )


@dataclass
class ReactionPayload:
    """Payload for 'reaction' events (Telegram MessageReactionUpdated-shaped).
    emoji is the full current reaction set; an empty list means reactions
    cleared."""
    chat_id: int
    message_id: int
    emoji: List[str] = field(default_factory=list)
    from_user: Optional[User] = None
    date: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chat": {"id": self.chat_id},
            "message_id": self.message_id,
            "from": self.from_user.to_dict() if self.from_user else None,
            "date": self.date,
            "emoji": list(self.emoji),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ReactionPayload":
        chat = _require(d, "chat", "reaction payload")
        if not isinstance(chat, dict) or "id" not in chat:
            raise GatewayProtocolError("reaction payload: 'chat' must be an object with 'id'")
        emoji = d.get("emoji") or []
        if not isinstance(emoji, list):
            raise GatewayProtocolError("reaction payload: 'emoji' must be an array")
        return cls(
            chat_id=_as_int(chat["id"], "reaction payload chat.id"),
            message_id=_as_int(_require(d, "message_id", "reaction payload"), "message_id"),
            emoji=[str(e) for e in emoji],
            from_user=User.from_dict(d.get("from")),
            date=(None if d.get("date") is None else _as_int(d["date"], "date")),
        )


@dataclass
class IdsPayload:
    """Payload for 'deletion' and 'ack' events: a chat plus a list of
    message_ids. For a media group, lists all member ids."""
    chat_id: int
    message_ids: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chat": {"id": self.chat_id},
            "message_ids": list(self.message_ids),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "IdsPayload":
        chat = _require(d, "chat", "ids payload")
        if not isinstance(chat, dict) or "id" not in chat:
            raise GatewayProtocolError("ids payload: 'chat' must be an object with 'id'")
        ids = _require(d, "message_ids", "ids payload")
        if not isinstance(ids, list):
            raise GatewayProtocolError("ids payload: 'message_ids' must be an array")
        return cls(
            chat_id=_as_int(chat["id"], "ids payload chat.id"),
            message_ids=[_as_int(i, "message_ids[]") for i in ids],
        )


# Map event_type → payload class, for dispatch in from_dict.
_PAYLOAD_CLASS = {
    EVENT_MESSAGE: MessagePayload,
    EVENT_EDITED_MESSAGE: MessagePayload,
    EVENT_REACTION: ReactionPayload,
    EVENT_DELETION: IdsPayload,
    EVENT_ACK: IdsPayload,
}


# --------------------------------------------------------------------------- #
# Envelope                                                                    #
# --------------------------------------------------------------------------- #
@dataclass
class Envelope:
    """The outer wire object wrapping every event in either direction.

    secret is included on requests (client → server) and omitted on responses
    (server → client). protocol_version guards format evolution. event_type
    selects the payload shape.
    """
    gateway: str
    event_type: str
    payload: Any                       # one of the *Payload dataclasses
    secret: Optional[str] = None
    protocol_version: int = PROTOCOL_VERSION

    def to_dict(self, *, include_secret: bool = True) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "protocol_version": self.protocol_version,
            "gateway": self.gateway,
            "event_type": self.event_type,
            "payload": self.payload.to_dict(),
        }
        if include_secret and self.secret is not None:
            d["secret"] = self.secret
        return d

    def to_json(self, *, include_secret: bool = True) -> str:
        return json.dumps(self.to_dict(include_secret=include_secret), ensure_ascii=False)

    @classmethod
    def from_dict(cls, d: Dict[str, Any], *, require_secret: bool = False) -> "Envelope":
        if not isinstance(d, dict):
            raise GatewayProtocolError("envelope: not a JSON object")

        pv = d.get("protocol_version")
        if pv is None:
            raise GatewayProtocolError("envelope: missing 'protocol_version'")
        pv = _as_int(pv, "protocol_version")
        if pv != PROTOCOL_VERSION:
            raise GatewayProtocolError(
                f"envelope: unsupported protocol_version {pv} (this build speaks {PROTOCOL_VERSION})"
            )

        gateway = _require(d, "gateway", "envelope")
        if not isinstance(gateway, str) or not gateway:
            raise GatewayProtocolError("envelope: 'gateway' must be a non-empty string")

        event_type = _require(d, "event_type", "envelope")
        if event_type not in _EVENT_TYPES:
            raise GatewayProtocolError(
                f"envelope: unknown event_type {event_type!r} "
                f"(expected one of {sorted(_EVENT_TYPES)})"
            )

        secret = d.get("secret")
        if require_secret and (secret is None or secret == ""):
            raise GatewayProtocolError("envelope: missing 'secret' on inbound request")

        payload_dict = _require(d, "payload", "envelope")
        if not isinstance(payload_dict, dict):
            raise GatewayProtocolError("envelope: 'payload' must be an object")
        payload = _PAYLOAD_CLASS[event_type].from_dict(payload_dict)

        return cls(
            gateway=gateway,
            event_type=event_type,
            payload=payload,
            secret=secret,
            protocol_version=pv,
        )

    @classmethod
    def from_json(cls, raw: str, *, require_secret: bool = False) -> "Envelope":
        try:
            d = json.loads(raw)
        except json.JSONDecodeError as e:
            raise GatewayProtocolError(f"envelope: invalid JSON: {e}") from e
        return cls.from_dict(d, require_secret=require_secret)


# --------------------------------------------------------------------------- #
# Convenience constructors                                                    #
# --------------------------------------------------------------------------- #
def make_message(
    gateway: str,
    chat_id: int,
    *,
    secret: Optional[str] = None,
    message_id: Optional[int] = None,
    media_group_id: Optional[str] = None,
    from_user: Optional[User] = None,
    date: Optional[int] = None,
    text: Optional[str] = None,
    reply_to: Optional[int] = None,
    attachments: Optional[List[Attachment]] = None,
    edited: bool = False,
) -> Envelope:
    """Build a 'message' (or 'edited_message') envelope."""
    return Envelope(
        gateway=gateway,
        event_type=(EVENT_EDITED_MESSAGE if edited else EVENT_MESSAGE),
        secret=secret,
        payload=MessagePayload(
            chat_id=chat_id,
            message_id=message_id,
            media_group_id=media_group_id,
            from_user=from_user,
            date=date,
            text=text,
            reply_to=reply_to,
            attachments=list(attachments or []),
        ),
    )


def make_reaction(
    gateway: str,
    chat_id: int,
    message_id: int,
    emoji: List[str],
    *,
    secret: Optional[str] = None,
    from_user: Optional[User] = None,
    date: Optional[int] = None,
) -> Envelope:
    """Build a 'reaction' envelope."""
    return Envelope(
        gateway=gateway,
        event_type=EVENT_REACTION,
        secret=secret,
        payload=ReactionPayload(
            chat_id=chat_id,
            message_id=message_id,
            emoji=list(emoji),
            from_user=from_user,
            date=date,
        ),
    )


def make_deletion(
    gateway: str,
    chat_id: int,
    message_ids: List[int],
    *,
    secret: Optional[str] = None,
) -> Envelope:
    """Build a 'deletion' envelope."""
    return Envelope(
        gateway=gateway,
        event_type=EVENT_DELETION,
        secret=secret,
        payload=IdsPayload(chat_id=chat_id, message_ids=list(message_ids)),
    )


def make_ack(
    gateway: str,
    chat_id: int,
    message_ids: List[int],
    *,
    secret: Optional[str] = None,
) -> Envelope:
    """Build an 'ack' envelope."""
    return Envelope(
        gateway=gateway,
        event_type=EVENT_ACK,
        secret=secret,
        payload=IdsPayload(chat_id=chat_id, message_ids=list(message_ids)),
    )
