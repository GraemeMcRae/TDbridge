"""
Pending-Work Store — a standalone, domain-agnostic black box.

Purpose: hold units of work that are waiting for a particular *event id* to be
correlated (i.e. for the real target id(s) of that event to become known). When
the correlation for event N arrives, the server pulls all pending work for N and
executes each item. When an event goes stale (Phase 5), the same items are run
against a missing correlation so their fallback behavior fires.

This module is deliberately domain-agnostic. It knows nothing about Telegram,
Discord, message maps, reactions, or replies. It stores **serializable typed
records** — an opaque `kind` string plus an opaque JSON `data` blob — keyed by
event_id, and hands them back in insertion order on demand. The *meaning* of a
kind (e.g. "complete_mapping", "perform_action") lives in the SERVER's
dispatcher, not here. That separation is what lets the same store serve Phase 3
(mapping completion) and Phase 4 (parked actions) without the store growing any
domain knowledge, and lets it be reused by any future gateway work.

Why persistent (SQLite): a unit of work may be created (e.g. at message-relay
time) just before a restart and need to run when its correlate arrives just
after. In-memory would lose it, leaving a dangling uncorrelated event — exactly
the failure the correlation subsystem exists to prevent. This matches the
durability of the userbot outbox.

Interface (all opaque to domain):
  * add(event_id, kind, data)         — enqueue a work item for an event
  * take(event_id) -> list[Item]      — remove & return all items for an event,
                                         in insertion order (the completion path)
  * peek_older_than(ts) -> list[Item] — items created before ts, WITHOUT removing
                                         (staleness scan; Phase 5 decides policy)
  * drop(item_ids)                    — remove specific items by id (after a
                                         staleness scan has run their fallback)
  * purge_older_than(ts) -> int       — hard delete (housekeeping)
  * count() / count_for(event_id)

An Item is a small record: (id, event_id, kind, data, created_ts).
"""

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List

logger = logging.getLogger("pending_work")


@dataclass
class WorkItem:
    id: int
    event_id: int
    kind: str
    data: Dict[str, Any]
    created_ts: float


class PendingWorkStore:
    def __init__(self, path: str):
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_work (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id    INTEGER NOT NULL,
                kind        TEXT    NOT NULL,   -- opaque; interpreted by the server
                data        TEXT    NOT NULL,   -- opaque JSON blob
                created_ts  REAL    NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pending_event "
            "ON pending_work (event_id)"
        )
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    # ---- add / take ------------------------------------------------------ #
    def add(self, event_id: int, kind: str, data: Dict[str, Any]) -> int:
        cur = self._conn.execute(
            "INSERT INTO pending_work (event_id, kind, data, created_ts) "
            "VALUES (?, ?, ?, ?)",
            (int(event_id), str(kind), json.dumps(data), time.time()),
        )
        self._conn.commit()
        return cur.lastrowid

    def take(self, event_id: int) -> List[WorkItem]:
        """Remove and return all work items for event_id, in insertion order.
        This is the completion path: the caller runs each item, and they are
        already gone from the store (a completed event's work is done)."""
        rows = self._conn.execute(
            "SELECT id, event_id, kind, data, created_ts FROM pending_work "
            "WHERE event_id = ? ORDER BY id ASC",
            (int(event_id),),
        ).fetchall()
        items = [self._row_to_item(r) for r in rows]
        if items:
            self._conn.execute(
                "DELETE FROM pending_work WHERE event_id = ?", (int(event_id),)
            )
            self._conn.commit()
        return items

    # ---- staleness scan (Phase 5 uses these; policy lives in the server) - #
    def peek_older_than(self, cutoff_ts: float) -> List[WorkItem]:
        """Return (without removing) items created before cutoff_ts, oldest
        first. The server decides what to do (run fallback, then drop)."""
        rows = self._conn.execute(
            "SELECT id, event_id, kind, data, created_ts FROM pending_work "
            "WHERE created_ts < ? ORDER BY id ASC",
            (float(cutoff_ts),),
        ).fetchall()
        return [self._row_to_item(r) for r in rows]

    def drop(self, item_ids: List[int]) -> int:
        if not item_ids:
            return 0
        qmarks = ",".join("?" for _ in item_ids)
        cur = self._conn.execute(
            f"DELETE FROM pending_work WHERE id IN ({qmarks})",
            [int(i) for i in item_ids],
        )
        self._conn.commit()
        return cur.rowcount

    # ---- housekeeping ---------------------------------------------------- #
    def purge_older_than(self, cutoff_ts: float) -> int:
        cur = self._conn.execute(
            "DELETE FROM pending_work WHERE created_ts < ?", (float(cutoff_ts),)
        )
        self._conn.commit()
        return cur.rowcount

    def count(self) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) AS n FROM pending_work"
        ).fetchone()["n"]

    def count_for(self, event_id: int) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) AS n FROM pending_work WHERE event_id = ?",
            (int(event_id),),
        ).fetchone()["n"]

    # ---- internal -------------------------------------------------------- #
    @staticmethod
    def _row_to_item(r) -> WorkItem:
        return WorkItem(
            id=r["id"],
            event_id=r["event_id"],
            kind=r["kind"],
            data=json.loads(r["data"]),
            created_ts=r["created_ts"],
        )


class AwaitingIndex:
    """A small, domain-agnostic reverse index: opaque `ref_key` (a string) →
    the `event_id` whose correlation that ref is waiting on. Lets an incoming
    action resolve 'which event is this reference awaiting?' with one indexed
    lookup, WITHOUT the pending-work store having to expose or search its opaque
    data blobs. The server chooses what a ref_key means (here: a Discord
    message's channel:id), so this class stays free of domain knowledge.

    Shares a DB file with PendingWorkStore for lifecycle simplicity, but is an
    independent table. A ref maps to exactly one event_id (last write wins); an
    entry is removed when its event completes or goes stale.
    """
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS awaiting_index (
                ref_key     TEXT PRIMARY KEY,
                event_id    INTEGER NOT NULL,
                created_ts  REAL NOT NULL
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def put(self, ref_key: str, event_id: int) -> None:
        self._conn.execute(
            "INSERT INTO awaiting_index (ref_key, event_id, created_ts) "
            "VALUES (?, ?, ?) ON CONFLICT(ref_key) DO UPDATE SET "
            "event_id=excluded.event_id, created_ts=excluded.created_ts",
            (str(ref_key), int(event_id), time.time()),
        )
        self._conn.commit()

    def get(self, ref_key: str):
        """Return the event_id this ref is awaiting, or None."""
        row = self._conn.execute(
            "SELECT event_id FROM awaiting_index WHERE ref_key = ?",
            (str(ref_key),),
        ).fetchone()
        return row["event_id"] if row else None

    def remove_event(self, event_id: int) -> int:
        """Remove all refs pointing at event_id (on completion/staleness)."""
        cur = self._conn.execute(
            "DELETE FROM awaiting_index WHERE event_id = ?", (int(event_id),)
        )
        self._conn.commit()
        return cur.rowcount

    def purge_older_than(self, cutoff_ts: float) -> int:
        cur = self._conn.execute(
            "DELETE FROM awaiting_index WHERE created_ts < ?", (float(cutoff_ts),)
        )
        self._conn.commit()
        return cur.rowcount
