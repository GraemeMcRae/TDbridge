"""
TDbridge Userbot — Outbox drain worker

Consumes the persistent FIFO queue (userbot_db) and performs each outbound
Telegram action, subject to two pacing rules folded into one worker:

  * Metering: never return two actions closer together than `metering_ms`
    (Telegram user-account rate-limit safety; the PRIMARY FLOOD defense).
  * FLOOD: if performing an action raises FloodWaitError, re-append it to the
    tail with defer_until = now + wait (bounded by flood_retry_max_sec for the
    "retry" policy; "save" always re-appends), so it is retried later WITHOUT
    blocking the worker forever and WITHOUT being overtaken out of order.

The worker is a "rate-limited blocking consumer":
  * Empty queue  -> await a wake Event (0 CPU) set by enqueue().
  * Non-empty    -> wait until max(metering balance, head.defer_until), then
                    perform the head action.

It calls an injected async `perform(action_type, chat_id, payload)` so it does
not import the Telegram layer (keeps dependencies one-way and makes it testable).
`perform` should raise telethon FloodWaitError on a flood; any other exception is
logged and the action is dropped (a poison action shouldn't wedge the queue).

All DB calls are wrapped in run_in_executor by the worker so the loop never
blocks on sqlite.
"""

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

from telethon.errors import FloodWaitError

logger = logging.getLogger("userbot_outbox")

# perform(action_type, chat_id, payload) -> awaitable
PerformFn = Callable[[str, str, dict], Awaitable[None]]


class Outbox:
    def __init__(self, db, *, metering_ms: int, flood_action: str,
                 flood_retry_max_sec: int, perform: PerformFn):
        self._db = db
        self._min_interval = max(0.0, metering_ms / 1000.0)
        self._flood_action = flood_action if flood_action in ("retry", "save") else "retry"
        self._flood_retry_max_sec = max(0, flood_retry_max_sec)
        self._perform = perform
        self._wake = asyncio.Event()
        self._last_returned_at = 0.0   # in-memory; resets on restart (safe: only adds spacing)
        self._stopped = False

    # ---- enqueue side (called by the bridge) --------------------------- #
    async def enqueue(self, chat_id, action_type: str, payload: dict,
                      defer_until_ts=None) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self._db.enqueue, chat_id, action_type, payload, defer_until_ts
        )
        self._wake.set()   # wake the worker if it was sleeping on an empty queue

    # ---- the worker ---------------------------------------------------- #
    async def run(self) -> None:
        """Drain the queue forever (until stop())."""
        loop = asyncio.get_running_loop()
        logger.info("outbox worker started (metering=%.0fms)", self._min_interval * 1000)
        while not self._stopped:
            head = await loop.run_in_executor(None, self._db.peek_head)
            if head is None:
                # Empty: sleep until enqueue() wakes us.
                self._wake.clear()
                try:
                    await self._wake.wait()
                except asyncio.CancelledError:
                    break
                continue

            # Compute how long to wait before performing this head action:
            #   - metering balance since the last performed action, AND
            #   - the action's own defer_until (future for a FLOOD retry).
            now = time.time()
            metering_ready = self._last_returned_at + self._min_interval
            ready_at = max(metering_ready, head["defer_until_ts"])
            wait = ready_at - now
            if wait > 0:
                try:
                    # Wait, but wake early if a *new* enqueue happens (it won't
                    # change the head under FIFO, but keeps us responsive to
                    # stop()); we simply re-loop and recompute.
                    await asyncio.wait_for(self._wake.wait(), timeout=wait)
                    self._wake.clear()
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    break
                continue   # re-evaluate head (it may have changed only by stop)

            # Perform the head action.
            action_type = head["action_type"]
            chat_id = head["chat_id"]
            try:
                payload = json.loads(head["payload_json"])
            except Exception:
                logger.warning("dropping unparseable outbox row seq=%s", head["seq"])
                await loop.run_in_executor(None, self._db.delete, head["seq"])
                continue

            try:
                await self._perform(action_type, chat_id, payload)
                self._last_returned_at = time.time()
                await loop.run_in_executor(None, self._db.delete, head["seq"])
            except FloodWaitError as e:
                wait_s = int(getattr(e, "seconds", 0) or 0)
                if self._flood_action == "retry" and wait_s > self._flood_retry_max_sec:
                    # Too long to sit on under "retry" — still re-append (we never
                    # simply drop), just with the full requested wait.
                    logger.warning(
                        "FLOOD %ss exceeds retry ceiling %ss — re-appending for "
                        "later.", wait_s, self._flood_retry_max_sec)
                else:
                    logger.warning("FLOOD %ss — re-appending action for retry.", wait_s)
                new_defer = time.time() + max(1, wait_s)
                await loop.run_in_executor(
                    None, self._db.reappend, head["seq"], new_defer
                )
                # Do NOT advance _last_returned_at (nothing was actually sent).
            except Exception as e:
                # Poison action: log and drop so it can't wedge the queue.
                logger.warning(
                    "outbox action seq=%s (%s) failed permanently, dropping: %s",
                    head["seq"], action_type, e)
                await loop.run_in_executor(None, self._db.delete, head["seq"])

        logger.info("outbox worker stopped")

    def stop(self) -> None:
        self._stopped = True
        self._wake.set()
