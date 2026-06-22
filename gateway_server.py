"""
gateway_server.py — TDbridge Gateway Protocol server (Phase 2 skeleton).

Runs an aiohttp HTTP server on the configured internal port (behind stunnel,
which terminates TLS on the external port). This instance serves exactly one
gateway: the one named by OWN_GATEWAY. Requests naming any other gateway are
rejected.

Phase 2 scope (this file):
  • Bind the internal listen port on the existing asyncio loop.
  • Authenticate the shared secret on every protected request.
  • Two endpoints:
      GET  /gateway/health          — liveness, no secret required
      POST /gateway/poll            — secret-protected; returns {"events": []}
                                       (real long-poll queue arrives in Phase 3)
  • Expose is_serving() so the dashboard can fold gateway liveness into health.

NOT in Phase 2: the event queue, send/ack, attachments, Telegram sending,
routing integration. Those are later phases.

The server starts only when this instance owns a gateway (OWN_GATEWAY non-empty)
and a listen port is configured; otherwise start() is a no-op (e.g. --env test,
which is client-only).

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from aiohttp import web

import db
import gateway_protocol as gp

logger = logging.getLogger("TDbridge")


class GatewayServer:
    """aiohttp-based gateway server, lifecycle-managed by the bot."""

    def __init__(self, config) -> None:
        self._config = config
        self._own_gateway: str = config.own_gateway or ""
        self._listen_port: int = int(getattr(config, "gateway_listen_port", 0) or 0)
        # The GatewayDef for our own gateway (holds the expected secret/flags).
        self._own_def = (config.gateways or {}).get(self._own_gateway)
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._serving: bool = False
        self._debug_endpoints: bool = bool(
            getattr(config, "gateway_debug_endpoints", False)
        )
        # Async hook injected by bot.py to send a text message to Telegram for
        # the Echo=true path. Signature:
        #   async def send_text(chat_id: int, text: Optional[str],
        #                        reply_to: Optional[int]) -> list[int]
        # Returns the list of real Telegram message_id(s) produced.
        self._send_text_hook = None
        # asyncio.Event used to wake long-poll waiters when an event is queued.
        # Created lazily in start() on the running loop.
        self._poll_wakeup = None

    def set_send_text_hook(self, hook) -> None:
        """Inject the async Telegram text-send hook used by the Echo path."""
        self._send_text_hook = hook

    # ------------------------------------------------------------------ #
    # Lifecycle                                                          #
    # ------------------------------------------------------------------ #
    @property
    def enabled(self) -> bool:
        """True if this instance should run a gateway server at all."""
        return bool(self._own_gateway) and self._listen_port > 0 and self._own_def is not None

    def is_serving(self) -> bool:
        """True once the server is bound and accepting connections."""
        return self._serving

    async def start(self) -> None:
        """Bind and start the server. No-op if not enabled (client-only)."""
        if not self.enabled:
            if self._own_gateway and self._own_def is None:
                logger.warning(
                    "Gateway server NOT started: OWN_GATEWAY=%r is not in the "
                    "gateways file.", self._own_gateway,
                )
            elif self._own_gateway and self._listen_port <= 0:
                logger.warning(
                    "Gateway server NOT started: OWN_GATEWAY=%r but no valid "
                    "GATEWAY_LISTEN_PORT configured.", self._own_gateway,
                )
            else:
                logger.info("Gateway server not started (client-only instance).")
            return

        self._poll_wakeup = asyncio.Event()

        app = web.Application()
        app.add_routes([
            web.get("/gateway/health", self._handle_health),
            web.post("/gateway/send", self._handle_send),
            web.post("/gateway/poll", self._handle_poll),
            web.post("/gateway/ack", self._handle_ack),
        ])
        if self._debug_endpoints:
            app.add_routes([
                web.post("/gateway/debug/enqueue", self._handle_debug_enqueue),
            ])
            logger.warning(
                "Gateway DEBUG endpoints are ENABLED (/gateway/debug/enqueue). "
                "This must be off in normal operation."
            )

        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        # Bind to localhost only — stunnel connects to us on 127.0.0.1; the
        # internal port must never be exposed directly to the network.
        self._site = web.TCPSite(self._runner, host="127.0.0.1", port=self._listen_port)
        await self._site.start()
        self._serving = True
        logger.info(
            "Gateway server listening on 127.0.0.1:%d for gateway %r "
            "(behind stunnel).",
            self._listen_port, self._own_gateway,
        )

    async def stop(self) -> None:
        """Stop the server and release the port. Safe to call if not started."""
        self._serving = False
        if self._site is not None:
            try:
                await self._site.stop()
            except Exception as e:
                logger.warning("Gateway server: error stopping site: %s", e)
            self._site = None
        if self._runner is not None:
            try:
                await self._runner.cleanup()
            except Exception as e:
                logger.warning("Gateway server: error during runner cleanup: %s", e)
            self._runner = None
        logger.info("Gateway server stopped.")

    # ------------------------------------------------------------------ #
    # Auth                                                               #
    # ------------------------------------------------------------------ #
    def _check_secret(self, envelope: gp.Envelope) -> Optional[web.Response]:
        """Validate that the envelope names OUR gateway and carries the correct
        secret. Returns a 401/403 Response on failure, or None on success.

        We do not reveal which check failed (unknown gateway vs. bad secret) to
        avoid leaking which gateway names are valid; both return 401.
        """
        if envelope.gateway != self._own_gateway:
            logger.warning(
                "Gateway auth: request named gateway %r but this server serves "
                "only %r — rejected.", envelope.gateway, self._own_gateway,
            )
            return web.json_response({"error": "unauthorized"}, status=401)
        if not envelope.secret or envelope.secret != self._own_def.secret:
            logger.warning(
                "Gateway auth: bad or missing secret for gateway %r — rejected.",
                envelope.gateway,
            )
            return web.json_response({"error": "unauthorized"}, status=401)
        return None

    async def _read_envelope(self, request: web.Request) -> tuple:
        """Read and parse the request body as an Envelope (secret required).
        Returns (envelope, None) on success or (None, Response) on failure."""
        try:
            raw = await request.text()
        except Exception:
            return None, web.json_response({"error": "bad request"}, status=400)
        try:
            env = gp.Envelope.from_json(raw, require_secret=True)
        except gp.GatewayProtocolError as e:
            logger.warning("Gateway: rejected malformed request: %s", e)
            return None, web.json_response({"error": f"malformed: {e}"}, status=400)
        return env, None

    # ------------------------------------------------------------------ #
    # Endpoints                                                          #
    # ------------------------------------------------------------------ #
    async def _handle_health(self, request: web.Request) -> web.Response:
        """Liveness probe. No secret required. Reveals only that the server is
        up and which gateway it serves — no sensitive data."""
        return web.json_response({
            "ok": True,
            "gateway": self._own_gateway,
            "protocol_version": gp.PROTOCOL_VERSION,
        })

    async def _handle_send(self, request: web.Request) -> web.Response:
        """Inbound event from the client.

        Phase 3 handles event_type 'message' for both Echo modes:
          • Echo=true  → send the text to the real Telegram group, return the
                         real message_id(s).
          • Echo=false → accept the client-supplied message_id; do not send to
                         Telegram. (Bridging onward to Discord is Phase 6.)
        Attachments are skipped with a note (Phase 4). Non-'message' event
        types are accepted but reported as not-yet-wired (later phases).
        """
        env, err = await self._read_envelope(request)
        if err is not None:
            return err
        auth_err = self._check_secret(env)
        if auth_err is not None:
            return auth_err

        if env.event_type not in (gp.EVENT_MESSAGE, gp.EVENT_EDITED_MESSAGE):
            return web.json_response({
                "protocol_version": gp.PROTOCOL_VERSION,
                "status": "not_implemented",
                "note": f"event_type '{env.event_type}' not yet wired (later phase)",
            })

        payload = env.payload   # MessagePayload
        notes = []
        if payload.attachments:
            notes.append(
                f"{len(payload.attachments)} attachment(s) skipped "
                f"(attachment support is Phase 4)"
            )

        echo = bool(self._own_def.echo)
        if echo:
            if self._send_text_hook is None:
                return web.json_response({
                    "protocol_version": gp.PROTOCOL_VERSION,
                    "status": "error",
                    "note": "Echo send hook not available",
                }, status=503)
            try:
                msg_ids = await self._send_text_hook(
                    payload.chat_id, payload.text, payload.reply_to
                )
            except Exception as e:
                logger.warning("Gateway send (echo) failed: %s", e)
                return web.json_response({
                    "protocol_version": gp.PROTOCOL_VERSION,
                    "status": "error",
                    "note": f"Telegram send failed: {e}",
                }, status=502)
            return web.json_response({
                "protocol_version": gp.PROTOCOL_VERSION,
                "status": "sent",
                "chat": {"id": payload.chat_id},
                "message_ids": list(msg_ids),
                "notes": notes,
            })
        else:
            # Echo=false: accept the client-supplied id; nothing sent to Telegram.
            return web.json_response({
                "protocol_version": gp.PROTOCOL_VERSION,
                "status": "accepted",
                "chat": {"id": payload.chat_id},
                "message_ids": ([payload.message_id] if payload.message_id is not None else []),
                "notes": notes,
            })

    async def _handle_poll(self, request: web.Request) -> web.Response:
        """Long-poll for queued events (up to ~10 s). Returns queued events for
        our gateway. For a RequireACK gateway, delivered events are retained
        until acked; otherwise they are deleted as they are returned."""
        env, err = await self._read_envelope(request)
        if err is not None:
            return err
        auth_err = self._check_secret(env)
        if auth_err is not None:
            return auth_err

        deadline = time.monotonic() + 10.0
        while True:
            rows = await asyncio.get_running_loop().run_in_executor(
                None, db.gateway_peek, self._own_gateway, 100
            )
            if rows:
                events = []
                ids = []
                for r in rows:
                    try:
                        events.append(json.loads(r["event_json"]))
                        ids.append(r["id"])
                    except Exception:
                        ids.append(r["id"])  # drop unparseable row from queue
                require_ack = bool(self._own_def.require_ack)
                if require_ack:
                    await asyncio.get_running_loop().run_in_executor(
                        None, db.gateway_mark_delivered, ids
                    )
                else:
                    await asyncio.get_running_loop().run_in_executor(
                        None, db.gateway_delete, ids
                    )
                return web.json_response({
                    "protocol_version": gp.PROTOCOL_VERSION,
                    "events": events,
                })
            # Queue empty — wait for a wakeup or until the 10 s deadline.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return web.json_response({
                    "protocol_version": gp.PROTOCOL_VERSION,
                    "events": [],
                })
            try:
                self._poll_wakeup.clear()
                await asyncio.wait_for(self._poll_wakeup.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return web.json_response({
                    "protocol_version": gp.PROTOCOL_VERSION,
                    "events": [],
                })

    async def _handle_ack(self, request: web.Request) -> web.Response:
        """Acknowledge receipt of events, removing them from the queue (for
        RequireACK gateways). For non-RequireACK gateways the events are already
        gone; a spurious ack is silently accepted as a no-op."""
        env, err = await self._read_envelope(request)
        if err is not None:
            return err
        auth_err = self._check_secret(env)
        if auth_err is not None:
            return auth_err
        if env.event_type != gp.EVENT_ACK:
            return web.json_response(
                {"error": "ack endpoint requires event_type 'ack'"}, status=400
            )
        payload = env.payload   # IdsPayload
        removed = await asyncio.get_running_loop().run_in_executor(
            None, db.gateway_delete_by_chat_and_msgids,
            self._own_gateway, str(payload.chat_id), payload.message_ids,
        )
        return web.json_response({
            "protocol_version": gp.PROTOCOL_VERSION,
            "status": "ack",
            "removed": removed,
        })

    async def _handle_debug_enqueue(self, request: web.Request) -> web.Response:
        """DEBUG-ONLY (gated by GATEWAY_DEBUG_ENDPOINTS): inject an event into
        the queue so poll/ack can be exercised before Phase 6 routing provides
        the real event source. Authenticates the secret like other endpoints."""
        env, err = await self._read_envelope(request)
        if err is not None:
            return err
        auth_err = self._check_secret(env)
        if auth_err is not None:
            return auth_err
        # Store the event as-is (response form, no secret) for later polling.
        chat_id = getattr(env.payload, "chat_id", "")
        event_json = env.to_json(include_secret=False)
        new_id = await asyncio.get_running_loop().run_in_executor(
            None, db.gateway_enqueue, self._own_gateway, str(chat_id), event_json
        )
        self._notify_poll_waiters()
        return web.json_response({
            "protocol_version": gp.PROTOCOL_VERSION,
            "status": "enqueued",
            "queue_id": new_id,
        })

    def _notify_poll_waiters(self) -> None:
        """Wake any in-flight long-poll so a newly-queued event is delivered
        promptly. Safe to call from the loop thread."""
        if self._poll_wakeup is not None:
            self._poll_wakeup.set()
