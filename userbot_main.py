"""
TDbridge Userbot — Entry point and lifecycle

Wires the pieces together and runs the userbot as a service:

    python userbot_main.py --env test    (or prod)

Startup:
  1. Load config (shared .env, shared telegram_gateways.json).
  2. Build the Telegram (Telethon) adapter, the gateway client wrapper, the
     local DB, the outbox drain worker, and the bridge.
  3. Ensure the Telethon session is authorized. If a session exists, this is a
     no-op. If not, use the gateway-mediated code provider (prompt out over the
     gateway, receive the human's reply back) when a primary group is
     configured; otherwise fall back to the terminal prompt.
  4. Run two long-lived tasks concurrently on one event loop:
        - the gateway poll loop (inbound events -> bridge -> outbox)
        - the outbox drain worker (performs queued outbound Telegram actions)
     plus Telethon's own update loop (inbound Telegram bot messages -> bridge).

Shutdown (SIGINT/SIGTERM or KeyboardInterrupt): stop the outbox, disconnect
Telethon, close the gateway client and DB.

For a purely manual, interactive first login, `python userbot_login.py --env X`
remains available (terminal prompt); this service can also self-initiate the
gateway-mediated login on startup when no session is present.
"""

import asyncio
import logging
import signal

from userbot_config import userbot_config as cfg
from userbot_telegram import UserbotTelegram
from userbot_gateway import UserbotGateway
from userbot_db import UserbotDB
from userbot_outbox import Outbox
from userbot_bridge import UserbotBridge
from userbot_login import make_terminal_code_provider


def _setup_logging() -> logging.Logger:
    # Timezone-aware formatter matching TDbridge's style: local time with the
    # tz abbreviation (e.g. PDT) and milliseconds.
    import time as _time
    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        _tz = ZoneInfo(cfg.local_timezone)
    except Exception:
        _tz = None

    class _TZFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, _tz)
            # e.g. 2026-07-09 21:49:16.199 PDT
            return dt.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(record.msecs):03d} " + dt.strftime("%Z")

    fmt = _TZFormatter("%(asctime)s - %(levelname)s - %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Clear any handlers a prior basicConfig may have added.
    for h in list(root.handlers):
        root.removeHandler(h)
    for handler in (logging.StreamHandler(), logging.FileHandler(cfg.log_filename)):
        handler.setFormatter(fmt)
        root.addHandler(handler)
    return logging.getLogger("userbot_main")


async def _run() -> None:
    log = _setup_logging()
    log.info("=== TDbridge Userbot starting (env=%s) ===", cfg.env)
    log.info("Gateway: %s (%s)", cfg.gateway_name, cfg.gateway.url)
    log.info("Relaying bot messages EXCEPT sibling: %s", cfg.gw_bot_username)

    # --- Build components ------------------------------------------------- #
    telegram = UserbotTelegram(
        session_file=cfg.session_file,
        api_id=cfg.tg_api_id,
        api_hash=cfg.tg_api_hash,
        phone=cfg.phone,
        gw_bot_username=cfg.gw_bot_username,
        two_fa_password=cfg.two_fa_password,
        login_code_wait_min=cfg.login_code_wait_min,
        login_code_max_retries=cfg.login_code_max_retries,
    )
    gateway = UserbotGateway(cfg.gateway)
    db = UserbotDB(cfg.sqlite_db_file)

    # The bridge is created before the outbox because the outbox's perform()
    # calls back into the bridge; we close the loop with a small shim.
    bridge_ref = {}

    async def _perform(action_type, chat_id, payload):
        await bridge_ref["bridge"].perform_action(action_type, chat_id, payload)

    outbox = Outbox(
        db,
        metering_ms=cfg.metering_ms,
        flood_action=cfg.flood_action,
        flood_retry_max_sec=cfg.flood_retry_max_sec,
        perform=_perform,
    )
    bridge = UserbotBridge(
        telegram, gateway, outbox=outbox,
    )
    bridge_ref["bridge"] = bridge
    bridge.attach()   # register the Telegram inbound handler

    # --- Choose the login code provider ----------------------------------- #
    # Gateway-mediated when a primary group is set (prompt out over the gateway,
    # human replies on Discord); else terminal. The provider is only invoked if
    # Login uses the terminal code provider: if there is no valid saved
    # session, ensure_logged_in() prompts for the code at the console. The
    # session is created once via `python userbot_login.py --env X` (or on the
    # first run here), then reused silently on every subsequent start.
    code_provider = make_terminal_code_provider()
    log.info("Login: terminal prompt if no valid session.")

    # --- Start the gateway poll loop, then Telethon (connect + login) ----- #
    stop_event = asyncio.Event()

    poll_task = asyncio.ensure_future(
        gateway.run_poll_loop(bridge.on_gateway_events)
    )

    try:
        await telegram.start(code_provider)
    except Exception as e:
        log.error("Login/startup failed: %s", e)
        poll_task.cancel()
        await gateway.close()
        db.close()
        return

    # --- Start the outbox worker ------------------------------------------ #
    outbox_task = asyncio.ensure_future(outbox.run())

    # --- Signal handling -------------------------------------------------- #
    loop = asyncio.get_running_loop()

    def _request_stop():
        log.info("Shutdown signal received.")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            # Signal handlers may be unavailable (e.g. on Windows); rely on
            # KeyboardInterrupt in that case.
            pass

    log.info("=== TDbridge Userbot ready ===")

    # Run until a stop is requested or Telethon disconnects.
    tg_task = asyncio.ensure_future(telegram.run_until_disconnected())
    stop_task = asyncio.ensure_future(stop_event.wait())
    done, pending = await asyncio.wait(
        {tg_task, stop_task, poll_task, outbox_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    # --- Shutdown --------------------------------------------------------- #
    log.info("TDbridge Userbot shutting down…")
    outbox.stop()
    for t in (poll_task, outbox_task, tg_task, stop_task):
        t.cancel()
    await telegram.stop()
    await gateway.close()
    db.close()
    log.info("TDbridge Userbot shutdown complete.")


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        # Windows / no-signal-handler path.
        pass


if __name__ == "__main__":
    main()
