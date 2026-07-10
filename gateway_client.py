"""
gateway_client.py — TDbridge Gateway Protocol client.

A reusable async client that lets a TDbridge instance (or a test harness) act as
a gateway CLIENT: send events to a remote gateway server, upload/download
attachments via the two-hop file transfer, and long-poll for queued events.

Used in Phase 6 when this instance connects to a gateway it does not own (e.g.
--env test connecting to prod's TDbridge_gw, or prod connecting to a partner's
SquadronDispatchBot_gw). In Phase 5 it is validated as standalone client↔server
plumbing.

All requests are HTTPS with certificate verification ON. The shared secret rides
in the envelope JSON for event endpoints (send/poll/ack) and in headers for the
file endpoints (upload/getfile), matching the server.

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

import aiohttp

import gateway_protocol as gp
from gateway_config import GatewayDef

logger = logging.getLogger("TDbridge")


class GatewayClientError(Exception):
    """Raised on a transport or protocol error talking to a gateway server."""


class GatewayClient:
    """Async client for one gateway (one GatewayDef).

    Create with the GatewayDef of the gateway to connect to. Reuses a single
    aiohttp ClientSession for connection pooling; call close() when done (or use
    as an async context manager).
    """

    def __init__(self, gateway: GatewayDef, *, poll_timeout: float = 10.0) -> None:
        self._gw = gateway
        self._poll_timeout = poll_timeout
        # Derive the endpoint root by stripping the trailing /gateway/<name>.
        # gateway.url is https://host:port/gateway/<name>.
        url = gateway.url
        self._root = url.rsplit("/gateway/", 1)[0] if "/gateway/" in url else url
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # A read timeout slightly longer than the server's long-poll so a
            # full 10 s empty poll completes before the client times out.
            timeout = aiohttp.ClientTimeout(
                total=None,
                connect=15,
                sock_read=self._poll_timeout + 20,
            )
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def __aenter__(self) -> "GatewayClient":
        await self._ensure_session()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    def _url(self, endpoint: str) -> str:
        return f"{self._root}/gateway/{endpoint}"

    # ------------------------------------------------------------------ #
    # Health                                                             #
    # ------------------------------------------------------------------ #
    async def health(self) -> dict:
        """GET /gateway/health (no secret). Returns the parsed JSON."""
        session = await self._ensure_session()
        async with session.get(self._url("health")) as resp:
            return await resp.json()

    # ------------------------------------------------------------------ #
    # Events: send / poll / ack                                          #
    # ------------------------------------------------------------------ #
    async def _post_envelope(
        self, endpoint: str, env: gp.Envelope, *, read_timeout: Optional[float] = None
    ) -> Tuple[int, dict]:
        """POST an envelope (secret included) to an event endpoint. Returns
        (status_code, parsed_json).

        read_timeout overrides the session's socket read timeout for this one
        request — used by send, where the server does the full
        upload→Telegram→Discord bridge inline (which, with attachments and
        transient-retry, can take well over the poll-sized default).
        """
        session = await self._ensure_session()
        kwargs = {}
        if read_timeout is not None:
            kwargs["timeout"] = aiohttp.ClientTimeout(
                total=None, connect=15, sock_read=read_timeout
            )
        try:
            async with session.post(
                self._url(endpoint),
                data=env.to_json(include_secret=True),
                headers={"Content-Type": "application/json"},
                **kwargs,
            ) as resp:
                try:
                    body = await resp.json()
                except Exception:
                    body = {"_raw": await resp.text()}
                return resp.status, body
        except aiohttp.ClientError as e:
            raise GatewayClientError(f"{endpoint} request failed: {e}") from e

    async def send_message(
        self,
        chat_id: int,
        *,
        text: Optional[str] = None,
        message_id: Optional[int] = None,
        reply_to: Optional[int] = None,
        attachments: Optional[List[gp.Attachment]] = None,
        edited: bool = False,
        read_timeout: float = 120.0,
    ) -> dict:
        """Send a 'message' (or 'edited_message') event. Returns the server's
        JSON response (which, for Echo=true, includes the real message_ids).

        read_timeout defaults to 120 s because the server performs the full
        upload→Telegram→Discord bridge inline; with attachments and transient
        retries this can take much longer than a text-only send.
        """
        env = gp.make_message(
            self._gw.name, chat_id,
            secret=self._gw.secret,
            text=text,
            message_id=message_id,
            reply_to=reply_to,
            attachments=attachments,
            edited=edited,
        )
        status, body = await self._post_envelope("send", env, read_timeout=read_timeout)
        if status != 200:
            raise GatewayClientError(f"send failed (HTTP {status}): {body}")
        return body

    async def send_reaction(
        self, chat_id: int, message_id: int, emoji: List[str],
        *, sender_name: Optional[str] = None,
    ) -> dict:
        """Send a 'reaction' event. sender_name, when given, is carried as the
        reactor's display name so the receiving side can attribute it correctly
        (otherwise the receiver falls back to the gateway name)."""
        from_user = (
            gp.User(first_name=sender_name, is_synthetic=True)
            if sender_name else None
        )
        env = gp.make_reaction(
            self._gw.name, chat_id, message_id, emoji,
            secret=self._gw.secret, from_user=from_user,
        )
        status, body = await self._post_envelope("send", env)
        if status != 200:
            raise GatewayClientError(f"send reaction failed (HTTP {status}): {body}")
        return body

    async def send_deletion(self, chat_id: int, message_ids: List[int]) -> dict:
        """Send a 'deletion' event."""
        env = gp.make_deletion(
            self._gw.name, chat_id, message_ids, secret=self._gw.secret
        )
        status, body = await self._post_envelope("send", env)
        if status != 200:
            raise GatewayClientError(f"send deletion failed (HTTP {status}): {body}")
        return body

    async def poll_once(self) -> List[dict]:
        """One long-poll (up to the server's ~10 s). Returns the list of event
        dicts (each a parsed envelope), possibly empty."""
        # The poll body is an envelope; event_type is unused by the server's
        # poll handler beyond auth, so an 'ack'-shaped envelope with empty
        # event_ids is a convenient carrier of gateway+secret.
        env = gp.make_ack(self._gw.name, [], secret=self._gw.secret)
        status, body = await self._post_envelope("poll", env)
        if status != 200:
            raise GatewayClientError(f"poll failed (HTTP {status}): {body}")
        return body.get("events", []) or []

    async def ack(self, event_ids: List[int]) -> dict:
        """Acknowledge receipt of events by their server-assigned event_ids
        (RequireACK gateways dequeue on ack)."""
        env = gp.make_ack(self._gw.name, list(event_ids), secret=self._gw.secret)
        status, body = await self._post_envelope("ack", env)
        if status != 200:
            raise GatewayClientError(f"ack failed (HTTP {status}): {body}")
        return body

    async def send_correlate(self, event_id: int, telegram_ids: List[int]) -> dict:
        """Report that outbound `event_id` became the given telegram_ids (empty
        list = deliberately nothing). Sent after the client posts to Telegram."""
        env = gp.make_correlate(self._gw.name, int(event_id),
                                [int(t) for t in telegram_ids],
                                secret=self._gw.secret)
        status, body = await self._post_envelope("correlate", env)
        if status != 200:
            raise GatewayClientError(f"correlate failed (HTTP {status}): {body}")
        return body

    async def run_poll_loop(self, on_events) -> None:
        """Continuously long-poll and dispatch events to the async callback
        `on_events(list_of_event_dicts)`. Runs until cancelled. Transient errors
        are logged and retried after a short backoff so the loop is resilient."""
        backoff = 1.0
        while True:
            try:
                events = await self.poll_once()
                backoff = 1.0
                if events:
                    for _ev in events:
                        _p = _ev.get("payload", {}) or {}
                        _chat = (_p.get("chat", {}) or {}).get("id")
                        _txt = (_p.get("text") or "")
                        _txt_prev = _txt.replace("\n", "\\n")
                        if len(_txt_prev) > 200:
                            _txt_prev = _txt_prev[:200] + "…"
                        logger.info(
                            "GW client RECV | gateway=%s | event=%s | chat=%s | "
                            "msg_id=%s | reply_to=%s | emoji=%s | attachments=%d | text='%s'",
                            self._gw.name, _ev.get("event_type"), _chat,
                            _p.get("message_id"), _p.get("reply_to"),
                            _p.get("emoji"),
                            len(_p.get("attachments") or []), _txt_prev,
                        )
                    await on_events(events)
            except GatewayClientError as e:
                logger.warning("Gateway client poll error: %s (retrying in %.0fs)", e, backoff)
                import asyncio
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
            except Exception:
                # Cancellation propagates; anything else is logged and retried.
                raise

    # ------------------------------------------------------------------ #
    # Attachments: upload / getfile / download                          #
    # ------------------------------------------------------------------ #
    def _file_headers(self, extra: Optional[dict] = None) -> dict:
        h = {
            "X-Gateway-Name": self._gw.name,
            "X-Gateway-Secret": self._gw.secret,
        }
        if extra:
            h.update(extra)
        return h

    async def upload_file(
        self, data: bytes, file_name: str, mime_type: str
    ) -> dict:
        """Upload attachment bytes; returns {file_ref, file_name, mime_type, size}."""
        session = await self._ensure_session()
        headers = self._file_headers({
            "X-File-Name": file_name or "attachment",
            "X-Mime-Type": mime_type or "application/octet-stream",
            "Content-Type": "application/octet-stream",
        })
        try:
            async with session.post(
                self._url("upload"), data=data, headers=headers
            ) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise GatewayClientError(f"upload failed (HTTP {resp.status}): {body}")
                return body
        except aiohttp.ClientError as e:
            raise GatewayClientError(f"upload request failed: {e}") from e

    async def getfile(self, file_ref: str) -> str:
        """Exchange a file_ref for a capability download URL."""
        session = await self._ensure_session()
        headers = self._file_headers({"Content-Type": "application/json"})
        import json as _json
        try:
            async with session.post(
                self._url("getfile"),
                data=_json.dumps({"file_ref": file_ref}),
                headers=headers,
            ) as resp:
                body = await resp.json()
                if resp.status != 200:
                    raise GatewayClientError(f"getfile failed (HTTP {resp.status}): {body}")
                return body["download_url"]
        except aiohttp.ClientError as e:
            raise GatewayClientError(f"getfile request failed: {e}") from e

    async def download_url(self, download_url: str) -> bytes:
        """GET the bytes from a capability download URL (no secret needed)."""
        session = await self._ensure_session()
        try:
            async with session.get(download_url) as resp:
                if resp.status != 200:
                    raise GatewayClientError(f"download failed (HTTP {resp.status})")
                return await resp.read()
        except aiohttp.ClientError as e:
            raise GatewayClientError(f"download request failed: {e}") from e

    async def download_file(self, file_ref: str) -> bytes:
        """Convenience: getfile then download — returns the attachment bytes."""
        url = await self.getfile(file_ref)
        return await self.download_url(url)
