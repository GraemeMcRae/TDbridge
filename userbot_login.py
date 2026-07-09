"""
TDbridge Userbot — Login

Drives the Telethon sign-in state machine for the userbot's Telegram (user)
account. Written to break the import cycle with userbot_telegram: this module
imports NOTHING from the other userbot_* modules except the leaf config. It
operates on a Telethon `client` object passed in, and obtains the per-login code
from an injected async `code_provider`, so it knows nothing about *how* the code
is delivered (terminal prompt vs. gateway-mediated Discord reply).

Two entry points:

  * ensure_logged_in(client, code_provider, ...) — importable by userbot_telegram;
    ensures the client is authorized, running the sign-in flow if no valid
    session exists, using code_provider to fetch the login code (with a wait
    window and bounded retries for the gateway path).

  * __main__ — run `python userbot_login.py --env test|prod` to perform a
    one-time interactive TERMINAL login (code typed at the console) and save the
    session file. This is the manual bootstrap / fallback path.

The 2FA (two-step-verification) password, if the account has one, is a static
secret read from config — distinct from the per-login code — so it needs no
provider.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeEmptyError,
)

logger = logging.getLogger("userbot_login")

# A code_provider is an async callable returning the login code as a string.
# It is called once per attempt; on timeout it should raise asyncio.TimeoutError
# (the gateway provider does this) so ensure_logged_in can re-request a code.
CodeProvider = Callable[[], Awaitable[str]]


class UserbotLoginError(Exception):
    """Raised when login cannot be completed (out of retries, bad password…)."""


async def ensure_logged_in(
    client,
    code_provider: CodeProvider,
    *,
    phone: str,
    two_fa_password: str = "",
    code_wait_min: int = 15,
    max_retries: int = 3,
) -> None:
    """Ensure `client` is authorized. No-op if a valid session already exists.

    Args:
        client:          a connected Telethon TelegramClient.
        code_provider:   async callable returning the login code (may raise
                         asyncio.TimeoutError to signal "no code arrived in
                         time" — we then re-request a fresh code and retry).
        phone:           the account's phone number.
        two_fa_password: optional two-step-verification password.
        code_wait_min:   minutes to allow the code_provider before re-requesting
                         (the provider itself enforces the wait; this is passed
                         through for providers that want it).
        max_retries:     how many times to re-request a code after a timeout or
                         an expired/invalid code before giving up.

    Raises:
        UserbotLoginError on unrecoverable failure (out of retries, bad 2FA
        password, etc.).
    """
    if not client.is_connected():
        await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        logger.info(
            "Userbot already authorized as %s (id=%s) — using saved session.",
            getattr(me, "username", None) or getattr(me, "first_name", "?"),
            getattr(me, "id", "?"),
        )
        return

    logger.info("No valid session — starting login for %s", phone)

    # Request the first code. Telethon returns a SentCode carrying the
    # phone_code_hash, which sign_in needs; the client tracks it internally, so
    # we don't have to thread it through, but we re-request on each retry.
    await client.send_code_request(phone)

    attempt = 0
    while True:
        attempt += 1
        try:
            code = await code_provider()
        except asyncio.TimeoutError:
            if attempt > max_retries:
                raise UserbotLoginError(
                    f"Login gave up after {max_retries} code attempts "
                    f"(no code received). Restart the service to retry."
                )
            logger.warning(
                "Login code not received within the wait window "
                "(attempt %d/%d) — requesting a fresh code.",
                attempt, max_retries,
            )
            # Prior code has likely expired; request a new one (this also
            # produces a new prompt on the gateway path).
            await client.send_code_request(phone)
            continue

        if not code:
            # Empty reply — treat like a miss and retry (bounded).
            if attempt > max_retries:
                raise UserbotLoginError(
                    f"Login gave up after {max_retries} attempts (empty code)."
                )
            logger.warning("Empty login code (attempt %d/%d) — retrying.",
                           attempt, max_retries)
            await client.send_code_request(phone)
            continue

        try:
            await client.sign_in(phone=phone, code=code.strip())
        except SessionPasswordNeededError:
            # Account has two-step verification enabled: supply the static
            # password from config (not a per-login code).
            if not two_fa_password:
                raise UserbotLoginError(
                    "Account requires a two-step-verification password, but "
                    "USERBOT_2FA_PASSWORD is not set."
                )
            try:
                await client.sign_in(password=two_fa_password)
            except Exception as e:
                raise UserbotLoginError(
                    f"Two-step-verification password was rejected: {e}"
                ) from e
        except (PhoneCodeInvalidError, PhoneCodeExpiredError, PhoneCodeEmptyError) as e:
            # Wrong/expired/empty code: re-request and retry (bounded).
            if attempt > max_retries:
                raise UserbotLoginError(
                    f"Login gave up after {max_retries} attempts "
                    f"(last error: {type(e).__name__})."
                )
            logger.warning(
                "Login code rejected (%s) on attempt %d/%d — requesting a "
                "fresh code.", type(e).__name__, attempt, max_retries,
            )
            await client.send_code_request(phone)
            continue

        # If we reach here, sign_in succeeded (with code, or code+password).
        me = await client.get_me()
        logger.info(
            "Userbot login successful as %s (id=%s). Session saved.",
            getattr(me, "username", None) or getattr(me, "first_name", "?"),
            getattr(me, "id", "?"),
        )
        return


# --------------------------------------------------------------------------- #
# Terminal code provider (used by the manual __main__ path, and available as a
# fallback provider). Prompts at the console for the code.
# --------------------------------------------------------------------------- #
def make_terminal_code_provider() -> CodeProvider:
    """Return a code_provider that reads the code from the terminal.

    Runs the blocking input() in a thread so it doesn't stall the event loop.
    """
    async def _provider() -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, input, "Enter the login code Telegram sent: "
        )
    return _provider


# --------------------------------------------------------------------------- #
# Manual one-time login entry point.
#   python userbot_login.py --env test
# Performs an interactive terminal login and saves the session file, so the
# service can later start non-interactively from the saved session.
# --------------------------------------------------------------------------- #
async def _main_async() -> None:
    from telethon import TelegramClient
    from userbot_config import userbot_config as cfg

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    client = TelegramClient(cfg.session_file, cfg.tg_api_id, cfg.tg_api_hash)
    await client.connect()
    try:
        await ensure_logged_in(
            client,
            make_terminal_code_provider(),
            phone=cfg.phone,
            two_fa_password=cfg.two_fa_password,
            code_wait_min=cfg.login_code_wait_min,
            max_retries=cfg.login_code_max_retries,
        )
        print("Login complete. Session saved to:", cfg.session_file)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(_main_async())
