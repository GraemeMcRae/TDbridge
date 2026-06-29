"""
gateway_ratelimit.py — Per-Telegram-group burst circuit breaker.

Protects against runaway message throughput in a single Telegram group — caused
by a routing loop, an errant gateway client injecting messages in a loop, or
similar programming errors — by tripping BEFORE Telegram/Discord rate limits or
FLOOD warnings shut down the whole bot.

Rate definition (b = TELEGRAM_BURSTRATE, the per-second burst allowance):
    The check trips if, counting the most recent processed messages for a group,
    the n-th most recent message was processed LESS than 2^((n-b)/5) seconds ago,
    for n in {b, b+5, b+10, b+15, b+20, b+25}.

    e.g. with b=10: no more than 10 in 1s, 15 in 2s, 20 in 4s, 25 in 8s,
    30 in 16s, 35 in 32s.

When tripped:
  • An ERROR is logged with the offending rate detail.
  • The group's T_Status is set to "Excessive Rate" in the IN-MEMORY cache only
    (never written to the sheet), which _is_active() treats as not-active, so all
    bridging to/from the group — and all gateway-out enqueueing for it — stops.
  • The next Sheets cache refresh re-reads the real status, auto-resetting.

b = 0 disables the circuit breaker entirely (no tracking, never trips).

Counting is per T_GroupID and covers TOTAL throughput: TG→DC, DC→TG, and
gateway-originated messages all count toward the same group's rate.

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Deque, Dict

logger = logging.getLogger("TDbridge")

# Number of check points and their spacing (n = b, b+5, ..., b+25).
_CHECK_OFFSETS = (0, 5, 10, 15, 20, 25)
# We need to retain the most recent (b + max_offset) timestamps per group.
_MAX_OFFSET = max(_CHECK_OFFSETS)

# Per-group ring buffer of recent processing timestamps (monotonic seconds).
_buffers: Dict[str, Deque[float]] = {}
# Groups currently tripped (so we don't log/trip repeatedly until reset).
_tripped: set = set()
# Count of trip events since last reset (for health reporting).
_trip_events_since_report: int = 0
_lock = threading.Lock()


STATUS_EXCESSIVE_RATE = "Excessive Rate"


def reset_group(tg_group_id: str) -> None:
    """Clear tracking + tripped state for a group (e.g. after a cache refresh
    re-reads its real status)."""
    gid = str(tg_group_id)
    with _lock:
        _buffers.pop(gid, None)
        _tripped.discard(gid)


def reset_all_tripped() -> None:
    """Clear all tripped state. Called after a full cache refresh, which
    re-reads every group's real status from the sheet."""
    with _lock:
        _tripped.clear()
        # Keep buffers — they reflect genuine recent activity — but a refreshed
        # status means a previously-tripped group is allowed to flow again.


def take_trip_count() -> int:
    """Return the number of circuit-breaker trips since the last call, and reset
    the counter. Used by the 30-minute health report."""
    global _trip_events_since_report
    with _lock:
        n = _trip_events_since_report
        _trip_events_since_report = 0
        return n


def check_and_record(tg_group_id: str, burstrate: int) -> bool:
    """Record one processed message for a group and evaluate the burst rule.

    Returns True if the message may be processed normally, or False if the
    circuit breaker is (or has just been) tripped for this group — in which case
    the caller must NOT bridge or enqueue this message.

    burstrate (b): 0 disables the breaker (always returns True).
    """
    if not burstrate or burstrate <= 0:
        return True

    gid = str(tg_group_id)
    now = time.monotonic()
    with _lock:
        if gid in _tripped:
            return False  # already tripped; stay closed until refresh

        buf = _buffers.get(gid)
        if buf is None:
            buf = deque(maxlen=burstrate + _MAX_OFFSET)
            _buffers[gid] = buf
        buf.append(now)

        # Evaluate the rule. buf is oldest→newest; the n-th most recent is
        # buf[-n]. We need at least n entries to test a given check point.
        for off in _CHECK_OFFSETS:
            n = burstrate + off
            if len(buf) < n:
                continue
            nth_recent_ts = buf[-n]
            age = now - nth_recent_ts
            threshold = 2.0 ** ((n - burstrate) / 5.0)
            if age < threshold:
                # Trip.
                _tripped.add(gid)
                global _trip_events_since_report
                _trip_events_since_report += 1
                logger.error(
                    "BURST CIRCUIT BREAKER TRIPPED for group %s: "
                    "%d messages within %.2fs (limit: %d within %.2fs). "
                    "Group set to '%s' in memory; bridging suspended until next "
                    "cache refresh. This should not happen — investigate the "
                    "cause (routing loop or errant gateway client).",
                    gid, n, age, n, threshold, STATUS_EXCESSIVE_RATE,
                )
                return False

    return True
