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
import gateway_files
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
        # Async hook injected by bot.py implementing the gateway's central
        # function: place the message in Telegram (Echo=true) or accept the
        # client-supplied id (Echo=false), then bridge it to Discord. Signature:
        #   async def bridge(*, chat_id: int, text: Optional[str],
        #                    reply_to: Optional[int], sender_name: str,
        #                    echo: bool, client_msg_id: Optional[int]) -> dict
        # Returns {"message_ids": [...], "dc_message_id": <str or None>}.
        self._bridge_hook = None
        # asyncio.Event used to wake long-poll waiters when an event is queued.
        # Created lazily in start() on the running loop.
        self._poll_wakeup = None

    def set_bridge_hook(self, hook) -> None:
        """Inject the async send-and-bridge hook (the gateway's central function)."""
        self._bridge_hook = hook

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

        # Ensure the attachment directory exists (we own a gateway).
        try:
            gateway_files.ensure_dir()
        except Exception as e:
            logger.warning("Gateway files dir could not be created: %s", e)

        app = web.Application(client_max_size=self._max_upload_bytes() + (1024 * 1024))
        app.add_routes([
            web.get("/gateway/health", self._handle_health),
            web.post("/gateway/send", self._handle_send),
            web.post("/gateway/poll", self._handle_poll),
            web.post("/gateway/ack", self._handle_ack),
            web.post("/gateway/upload", self._handle_upload),
            web.post("/gateway/getfile", self._handle_getfile),
            web.get("/gateway/file/{token}", self._handle_download),
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

        # Derive the sender name for Discord attribution: the gateway message's
        # from.first_name if present, else the gateway name (per design).
        from_user = payload.from_user
        sender_name = (
            from_user.first_name if (from_user and from_user.first_name)
            else self._own_gateway
        )

        echo = bool(self._own_def.echo)
        if self._bridge_hook is None:
            return web.json_response({
                "protocol_version": gp.PROTOCOL_VERSION,
                "status": "error",
                "note": "bridge hook not available",
            }, status=503)
        try:
            result = await self._bridge_hook(
                chat_id=payload.chat_id,
                text=payload.text,
                reply_to=payload.reply_to,
                sender_name=sender_name,
                echo=echo,
                client_msg_id=payload.message_id,
            )
        except Exception as e:
            logger.warning("Gateway send/bridge failed: %s", e)
            return web.json_response({
                "protocol_version": gp.PROTOCOL_VERSION,
                "status": "error",
                "note": f"send/bridge failed: {e}",
            }, status=502)
        return web.json_response({
            "protocol_version": gp.PROTOCOL_VERSION,
            "status": ("sent" if echo else "accepted"),
            "chat": {"id": payload.chat_id},
            "message_ids": list(result.get("message_ids", [])),
            "dc_message_id": result.get("dc_message_id"),
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
                    # Files are retained until ack (client may still download).
                else:
                    await asyncio.get_running_loop().run_in_executor(
                        None, db.gateway_delete, ids
                    )
                    # Non-ACK: delivery is the terminal moment — delete any
                    # attachment files these events referenced.
                    await self._delete_event_files(events)
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
        loop = asyncio.get_running_loop()
        # Read the matching queued events first so we can delete their attachment
        # files once the rows are removed (ack is the terminal moment for
        # RequireACK gateways — no further download will be requested).
        matching = await loop.run_in_executor(
            None, db.gateway_peek, self._own_gateway, 1000
        )
        want = {str(m) for m in payload.message_ids}
        to_clean = []
        for r in matching:
            try:
                ev = json.loads(r["event_json"])
                mid = ev.get("payload", {}).get("message_id")
                if mid is not None and str(mid) in want:
                    to_clean.append(ev)
            except Exception:
                continue
        removed = await loop.run_in_executor(
            None, db.gateway_delete_by_chat_and_msgids,
            self._own_gateway, str(payload.chat_id), payload.message_ids,
        )
        await self._delete_event_files(to_clean)
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

    async def enqueue_outbound(self, gateway: str, chat_id, event_json: str) -> None:
        """Enqueue an outbound event for a gateway client to poll, and wake any
        waiting long-poll. Called by the bot when a reply/reaction/deletion/edit
        concerns a gateway-originated message. `gateway` is the destination
        gateway name (the message's origin_gateway)."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, db.gateway_enqueue, str(gateway), str(chat_id), event_json
        )
        self._notify_poll_waiters()

    async def _delete_event_files(self, events: list) -> None:
        """Delete any gateway attachment files referenced by the given event
        dicts. Called at a terminal moment (non-ACK delivery, or ack), when the
        files can no longer be requested. No-op for events without attachments
        (the common case until outbound attachments are wired in a later phase).
        """
        refs = []
        for ev in events:
            try:
                for att in (ev.get("payload", {}).get("attachments") or []):
                    ref = att.get("file_ref")
                    if ref:
                        refs.append(ref)
            except Exception:
                continue
        if not refs:
            return
        loop = asyncio.get_running_loop()
        for ref in refs:
            await loop.run_in_executor(None, gateway_files.delete_file, ref)
        logger.debug("Gateway: deleted %d attachment file(s) at terminal moment.", len(refs))

    # ------------------------------------------------------------------ #
    # Attachments (two-hop: upload → getfile → capability GET)           #
    # ------------------------------------------------------------------ #
    def _max_upload_bytes(self) -> int:
        return int(getattr(self._config, "gateway_filesize_mb", 50)) * 1024 * 1024

    def _check_header_secret(self, request: web.Request) -> Optional[web.Response]:
        """Auth for upload/getfile, whose bodies aren't envelope JSON. The
        secret and (for upload) the target gateway ride in headers:
            X-Gateway-Name, X-Gateway-Secret
        Returns a 401 Response on failure, or None on success."""
        name = request.headers.get("X-Gateway-Name", "")
        secret = request.headers.get("X-Gateway-Secret", "")
        if name != self._own_gateway:
            logger.warning(
                "Gateway file auth: header gateway %r != served %r — rejected.",
                name, self._own_gateway,
            )
            return web.json_response({"error": "unauthorized"}, status=401)
        if not secret or secret != self._own_def.secret:
            logger.warning("Gateway file auth: bad/missing secret — rejected.")
            return web.json_response({"error": "unauthorized"}, status=401)
        return None

    async def _handle_upload(self, request: web.Request) -> web.Response:
        """Receive raw attachment bytes; return a file_ref. Secret + metadata in
        headers: X-Gateway-Name, X-Gateway-Secret, X-File-Name, X-Mime-Type."""
        auth_err = self._check_header_secret(request)
        if auth_err is not None:
            return auth_err
        file_name = request.headers.get("X-File-Name", "") or "attachment"
        mime_type = request.headers.get("X-Mime-Type", "") or "application/octet-stream"
        limit = self._max_upload_bytes()
        # Early rejection by Content-Length, before buffering the body, so a
        # very large upload is refused cheaply with a clear 413.
        try:
            declared = int(request.headers.get("Content-Length", "0") or "0")
        except ValueError:
            declared = 0
        if declared > limit:
            return web.json_response({
                "error": f"file too large ({declared} bytes > {limit} byte limit)"
            }, status=413)
        try:
            data = await request.read()
        except web.HTTPRequestEntityTooLarge:
            return web.json_response({
                "error": f"file too large (exceeds {limit} byte limit)"
            }, status=413)
        except Exception:
            return web.json_response({"error": "bad request"}, status=400)
        if len(data) > limit:
            return web.json_response({
                "error": f"file too large ({len(data)} bytes > {limit} byte limit)"
            }, status=413)
        if not data:
            return web.json_response({"error": "empty upload"}, status=400)
        info = await asyncio.get_running_loop().run_in_executor(
            None, gateway_files.store_upload,
            self._own_gateway, data, file_name, mime_type,
        )
        return web.json_response({
            "protocol_version": gp.PROTOCOL_VERSION,
            **info,
        })

    async def _handle_getfile(self, request: web.Request) -> web.Response:
        """Exchange a file_ref for a short-lived capability download URL.
        Secret in header X-Gateway-Secret + X-Gateway-Name; file_ref in the JSON
        body {"file_ref": "..."}."""
        auth_err = self._check_header_secret(request)
        if auth_err is not None:
            return auth_err
        try:
            body = json.loads(await request.text() or "{}")
        except Exception:
            return web.json_response({"error": "bad request"}, status=400)
        file_ref = body.get("file_ref")
        if not file_ref:
            return web.json_response({"error": "missing file_ref"}, status=400)
        token = await asyncio.get_running_loop().run_in_executor(
            None, gateway_files.make_download_token, file_ref
        )
        if token is None:
            return web.json_response({"error": "unknown file_ref"}, status=404)
        # Build the external capability URL from the gateway's configured url.
        # The gateway url looks like https://host:port/gateway/<name>; the file
        # path is served at https://host:port/gateway/file/<token>.
        base = self._own_def.url
        # strip the trailing /gateway/<name> to get the scheme://host:port/gateway root
        root = base.rsplit("/gateway/", 1)[0] if "/gateway/" in base else base
        download_url = f"{root}/gateway/file/{token}"
        return web.json_response({
            "protocol_version": gp.PROTOCOL_VERSION,
            "file_ref": file_ref,
            "download_url": download_url,
        })

    async def _handle_download(self, request: web.Request) -> web.Response:
        """Serve attachment bytes for a capability token (no secret — the
        unguessable token IS the capability, HTTPS-only). The token rides in the
        URL path; possession of the getfile-issued URL authorises the fetch."""
        token = request.match_info.get("token", "")
        row = await asyncio.get_running_loop().run_in_executor(
            None, gateway_files.resolve_token, token
        )
        if row is None:
            return web.json_response({"error": "not found"}, status=404)
        try:
            with open(row["path"], "rb") as f:
                data = f.read()
        except OSError:
            return web.json_response({"error": "not found"}, status=404)
        return web.Response(
            body=data,
            content_type=(row.get("mime_type") or "application/octet-stream"),
            headers={
                "Content-Disposition":
                    f'attachment; filename="{row.get("file_name") or "attachment"}"'
            },
        )
