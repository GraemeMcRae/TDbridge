"""
Correlation Registry — a standalone, domain-agnostic black box.

Purpose: record and look up the correspondence between a server-assigned
outbound *event id* and the *target id(s)* that resulted from it. In the gateway
world an "event" is an outbound gateway event and the "targets" are the Telegram
message id(s) the poster assigned — but this module knows NONE of that. It deals
only in integers: event_id → [target_id, ...]. That generality is deliberate, so
the same registry serves the userbot gateway, the client/server gateway, or any
future use, without duplication.

What it does (and nothing more):
  * record(event_id, target_ids)  — store a correlation (empty list allowed,
    meaning "deliberately nothing"); idempotent per event_id (last write wins,
    which also lets a re-report update the ids).
  * lookup(event_id) -> list | None — the target ids, or None if not recorded.
  * is_recorded(event_id) -> bool
  * highest_recorded() -> int | None — the largest event_id recorded so far
    (the "watermark" the server's ordering/staleness POLICY builds on; the
    registry only reports it, it enforces no policy).
  * recorded_between(low, high) / purge_older_than(ts) — housekeeping helpers.

Policy (ordering enforcement, staleness, fallbacks) lives OUTSIDE this module,
in the server, per the design. The registry is pure record/lookup/report.

Persistence: SQLite, because correlations must outlive a process restart (a
downstream action may arrive minutes later). One tiny table.
"""

import logging
import sqlite3
import time
from typing import List, Optional

logger = logging.getLogger("correlation")


class CorrelationRegistry:
    def __init__(self, path: str):
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS correlations (
                event_id    INTEGER PRIMARY KEY,   -- server-assigned outbound event id
                target_ids  TEXT    NOT NULL,      -- comma-separated target ids ('' = empty)
                recorded_ts REAL    NOT NULL
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    # ---- record / lookup ------------------------------------------------- #
    def record(self, event_id: int, target_ids: List[int]) -> None:
        """Store (or overwrite) the correlation for event_id. An empty list is
        valid and means 'deliberately nothing'."""
        csv = ",".join(str(int(t)) for t in target_ids)
        self._conn.execute(
            "INSERT INTO correlations (event_id, target_ids, recorded_ts) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(event_id) DO UPDATE SET target_ids=excluded.target_ids, "
            "recorded_ts=excluded.recorded_ts",
            (int(event_id), csv, time.time()),
        )
        self._conn.commit()

    def lookup(self, event_id: int) -> Optional[List[int]]:
        """Return the target ids for event_id, or None if not recorded.
        A recorded-but-empty correlation returns [] (distinct from None)."""
        row = self._conn.execute(
            "SELECT target_ids FROM correlations WHERE event_id = ?",
            (int(event_id),),
        ).fetchone()
        if row is None:
            return None
        raw = row["target_ids"]
        if not raw:
            return []
        return [int(x) for x in raw.split(",")]

    def is_recorded(self, event_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM correlations WHERE event_id = ?", (int(event_id),)
        ).fetchone()
        return row is not None

    def highest_recorded(self) -> Optional[int]:
        """The largest event_id recorded so far, or None if empty. This is the
        watermark the server's ordering/staleness policy consults; the registry
        itself enforces no policy."""
        row = self._conn.execute(
            "SELECT MAX(event_id) AS m FROM correlations"
        ).fetchone()
        return row["m"] if row and row["m"] is not None else None

    # ---- housekeeping ---------------------------------------------------- #
    def purge_older_than(self, cutoff_ts: float) -> int:
        """Delete correlations recorded before cutoff_ts. Returns rows removed."""
        cur = self._conn.execute(
            "DELETE FROM correlations WHERE recorded_ts < ?", (float(cutoff_ts),)
        )
        self._conn.commit()
        return cur.rowcount

    def count(self) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) AS n FROM correlations"
        ).fetchone()["n"]
