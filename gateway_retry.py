"""
gateway_retry.py — Heartbeat-safe retry helper for Discord and Telegram ops.

Wraps a retryable network operation (send/edit/delete/react) so that transient
failures and rate limits (429) are retried with an escalating backoff, while
NEVER blocking the asyncio event loop (all waits are `await asyncio.sleep`, so
the Discord / Telegram gateway heartbeats keep firing during a backoff).

Backoff rule (per the project spec):
  • Record start wallclock at the first attempt.
  • On a retryable error, wait  server_retry_after + added  seconds, where the
    added component starts at 10 and DOUBLES each retry (10, 20, 40, 80, ...).
    server_retry_after comes from the API's Retry-After when present, else 0.
  • Log a WARNING per retry.
  • After each failure, if elapsed wallclock >= MAX_TOTAL_SECONDS (10 min), log
    an ERROR and raise RetryGaveUp so the caller can do terminal cleanup
    (e.g. delete a gateway file that can no longer be delivered).
  • Non-retryable errors (e.g. 400/401/403/404) raise immediately, unchanged.

Retryable HTTP status codes (from the HCF project): 408, 429, 500, 502, 503, 504.

Usage:
    from gateway_retry import with_retry
    msg = await with_retry("DC send #chan", lambda: channel.send(...), platform="discord")

`coro_factory` must be a zero-arg callable that returns a FRESH awaitable each
time it is called (a spent coroutine cannot be re-awaited), e.g. a lambda.

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("TDbridge")

# Wallclock budget for a single operation, across all its retries.
MAX_TOTAL_SECONDS = 600.0      # 10 minutes
# Initial added-delay component; doubles each retry.
INITIAL_ADDED_SECONDS = 10.0
# Clamp any single wait so we re-check the time budget at sane intervals and
# don't sleep wildly past the 10-minute window on one wait.
MAX_SINGLE_WAIT_SECONDS = 120.0

RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class RetryGaveUp(Exception):
    """Raised when an operation exhausts the retry budget. The caller should
    treat this as the terminal 'retry no longer possible' moment (e.g. delete
    any associated gateway file)."""

    def __init__(self, operation_name: str, elapsed: float, last_error: BaseException):
        super().__init__(
            f"{operation_name}: gave up after {elapsed:.0f}s — last error: {last_error}"
        )
        self.operation_name = operation_name
        self.elapsed = elapsed
        self.last_error = last_error


# --------------------------------------------------------------------------- #
# Platform adapters: classify an exception and extract any server-suggested    #
# Retry-After. Imports are done lazily/defensively so a missing/renamed type   #
# in one library version never breaks the helper.                              #
# --------------------------------------------------------------------------- #
def _classify_discord(exc: BaseException) -> Optional[float]:
    """Return server Retry-After seconds (>=0) if `exc` is a retryable Discord
    error, or None if it is NOT retryable. Returns 0.0 when retryable but no
    Retry-After is supplied."""
    try:
        import discord
    except Exception:
        return None

    # discord.RateLimited carries .retry_after directly.
    RateLimited = getattr(discord, "RateLimited", None)
    if RateLimited is not None and isinstance(exc, RateLimited):
        return float(getattr(exc, "retry_after", 0.0) or 0.0)

    # discord.HTTPException has .status; check against the retryable set.
    HTTPException = getattr(discord, "HTTPException", None)
    if HTTPException is not None and isinstance(exc, HTTPException):
        status = getattr(exc, "status", None)
        if status in RETRYABLE_STATUS:
            ra = 0.0
            resp = getattr(exc, "response", None)
            if resp is not None:
                try:
                    ra = float(resp.headers.get("Retry-After", "0") or "0")
                except Exception:
                    ra = 0.0
            return ra
        return None  # a non-retryable HTTP error (400/401/403/404, ...)

    # Connection-level hiccups are retryable with no server hint.
    DiscordServerError = getattr(discord, "DiscordServerError", None)
    if DiscordServerError is not None and isinstance(exc, DiscordServerError):
        return 0.0
    GatewayNotFound = getattr(discord, "GatewayNotFound", None)
    if GatewayNotFound is not None and isinstance(exc, GatewayNotFound):
        return 0.0

    return None


def _classify_telegram(exc: BaseException) -> Optional[float]:
    """Return server Retry-After seconds (>=0) if `exc` is a retryable Telegram
    error, or None if NOT retryable."""
    try:
        from telegram import error as tg_error
    except Exception:
        tg_error = None

    if tg_error is not None:
        RetryAfter = getattr(tg_error, "RetryAfter", None)
        if RetryAfter is not None and isinstance(exc, RetryAfter):
            return float(getattr(exc, "retry_after", 0.0) or 0.0)
        TimedOut = getattr(tg_error, "TimedOut", None)
        if TimedOut is not None and isinstance(exc, TimedOut):
            return 0.0
        NetworkError = getattr(tg_error, "NetworkError", None)
        # NetworkError is the base of several transient errors; BadRequest is a
        # subclass-sibling that is NOT transient, so exclude it explicitly.
        BadRequest = getattr(tg_error, "BadRequest", None)
        if NetworkError is not None and isinstance(exc, NetworkError):
            if BadRequest is not None and isinstance(exc, BadRequest):
                return None
            return 0.0

    return None


def _classify(exc: BaseException, platform: str) -> Optional[float]:
    if platform == "discord":
        return _classify_discord(exc)
    if platform == "telegram":
        return _classify_telegram(exc)
    return None


# --------------------------------------------------------------------------- #
# The retry helper                                                            #
# --------------------------------------------------------------------------- #
async def with_retry(
    operation_name: str,
    coro_factory: Callable[[], Awaitable[Any]],
    *,
    platform: str,
) -> Any:
    """Run an async operation with rate-limit-aware, heartbeat-safe retries.

    operation_name : short label used in WARNING/ERROR logs.
    coro_factory   : zero-arg callable returning a FRESH awaitable per attempt.
    platform       : "discord" or "telegram" — selects exception classification.

    Returns the operation's result on success. Raises RetryGaveUp if the budget
    is exhausted, or re-raises immediately on a non-retryable error.
    """
    start = time.monotonic()
    added = INITIAL_ADDED_SECONDS
    attempt = 0
    while True:
        attempt += 1
        try:
            return await coro_factory()
        except BaseException as exc:
            server_retry_after = _classify(exc, platform)
            if server_retry_after is None:
                # Not retryable — surface immediately, unchanged.
                raise
            elapsed = time.monotonic() - start
            if elapsed >= MAX_TOTAL_SECONDS:
                logger.error(
                    "%s: giving up after %.0fs (%d attempts) — last error: %s",
                    operation_name, elapsed, attempt, exc,
                )
                raise RetryGaveUp(operation_name, elapsed, exc) from exc
            wait = min(server_retry_after + added, MAX_SINGLE_WAIT_SECONDS)
            logger.warning(
                "%s: retryable error on attempt %d (%s); waiting %.0fs before retry "
                "[elapsed %.0fs / %.0fs budget]",
                operation_name, attempt, exc, wait, elapsed, MAX_TOTAL_SECONDS,
            )
            await asyncio.sleep(wait)
            added *= 2
