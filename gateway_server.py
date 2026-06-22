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

import logging
from typing import Optional

from aiohttp import web

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

        app = web.Application()
        app.add_routes([
            web.get("/gateway/health", self._handle_health),
            web.post("/gateway/poll", self._handle_poll),
        ])

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

    async def _handle_poll(self, request: web.Request) -> web.Response:
        """Long-poll for queued events. Phase 2: authenticate and return an
        empty event list immediately. The real slow-poll queue is Phase 3."""
        env, err = await self._read_envelope(request)
        if err is not None:
            return err
        auth_err = self._check_secret(env)
        if auth_err is not None:
            return auth_err
        # Phase 2 stub: no queue yet.
        return web.json_response({
            "protocol_version": gp.PROTOCOL_VERSION,
            "events": [],
        })
