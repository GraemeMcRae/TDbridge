"""
TDbridge Userbot — Gateway client wrapper

A thin wrapper around the EXISTING, unchanged GatewayClient (gateway_client.py).
It exists only to give userbot_bridge a small, intention-revealing surface and to
hold the single client instance for the userbot's one gateway. It adds no
protocol logic of its own — all wire behavior (envelopes, long-poll, ack, retry,
upload/download) lives in the reused GatewayClient.

The userbot is a gateway CLIENT of its co-located TDbridge server, reached over
a loopback http:// endpoint (permitted by the surgical gateway_config change).
"""

import logging
from typing import Callable, List, Optional, Awaitable

import gateway_protocol as gp
from gateway_client import GatewayClient
from gateway_config import GatewayDef

logger = logging.getLogger("userbot_gateway")


class UserbotGateway:
    """Owns the single GatewayClient for the userbot's gateway."""

    def __init__(self, gateway: GatewayDef, *, poll_timeout: float = 10.0):
        self._client = GatewayClient(gateway, poll_timeout=poll_timeout)
        self._gateway_name = gateway.name

    @property
    def name(self) -> str:
        return self._gateway_name

    async def close(self) -> None:
        await self._client.close()

    # ---- Outbound (userbot -> server) ---------------------------------- #

    async def send_message(self, chat_id: int, *, text: Optional[str] = None,
                           message_id: Optional[int] = None,
                           reply_to: Optional[int] = None,
                           attachments: Optional[List[gp.Attachment]] = None,
                           edited: bool = False) -> dict:
        return await self._client.send_message(
            chat_id, text=text, message_id=message_id, reply_to=reply_to,
            attachments=attachments, edited=edited,
        )

    async def send_reaction(self, chat_id: int, message_id: int,
                            emoji: List[str], *,
                            sender_name: Optional[str] = None) -> dict:
        return await self._client.send_reaction(
            chat_id, message_id, emoji, sender_name=sender_name
        )

    async def send_deletion(self, chat_id: int, message_ids: List[int]) -> dict:
        return await self._client.send_deletion(chat_id, message_ids)

    async def upload_file(self, data: bytes, file_name: str,
                          mime_type: str) -> dict:
        return await self._client.upload_file(data, file_name, mime_type)

    async def download_file(self, file_ref: str) -> bytes:
        return await self._client.download_file(file_ref)

    async def ack(self, chat_id: int, message_ids: List[int]) -> dict:
        return await self._client.ack(chat_id, message_ids)

    # ---- Inbound (server -> userbot) ----------------------------------- #

    async def run_poll_loop(self, on_events: Callable[[list], Awaitable[None]]) -> None:
        """Long-poll forever, handing each batch of event dicts to on_events."""
        await self._client.run_poll_loop(on_events)
