"""
TDbridge Userbot — Local SQLite state

A deliberately tiny persistent store whose ONLY job is the outbound action queue.
Every outbound Telegram action (message / edited_message / reaction / deletion)
is enqueued here the moment its gateway event is received (and immediately
acked), then performed later by the outbox drain worker. Persisting the queue
means a restart never loses a backlog — the metering/FLOOD pacing resumes from
the stored rows.

Design notes:
  * FIFO by `seq` (autoincrement) — order-preserving, so a FLOOD-delayed action
    blocks the ones behind it rather than being overtaken (which would let, e.g.,
    a reaction be applied before the message it reacts to).
  * `defer_until_ts` is a wall-clock epoch seconds "not before" time. Normal
    actions get now(); a FLOOD retry re-appends with now()+flood_wait.
  * We do NOT persist the metering cadence (`last_returned_at`): on restart it
    resets to "long ago", which can only ADD spacing, never remove it — the safe
    direction, and it makes an explicit restart-normalize pass unnecessary.
  * Inbound dedupe is intentionally in-memory (see userbot_bridge): durability of
    outbound work is carried by THIS queue, so the dedupe set needs no
    persistence and stays bounded.

All access is synchronous sqlite3 wrapped by callers in run_in_executor so the
event loop never blocks.
"""

import json
import logging
import sqlite3
import time
from typing import List, Optional, Tuple

logger = logging.getLogger("userbot_db")


class UserbotDB:
    def __init__(self, path: str):
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deferred_actions (
                seq            INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id       INTEGER,            -- originating gateway event id (for correlate)
                chat_id        TEXT    NOT NULL,
                action_type    TEXT    NOT NULL,   -- message|edited_message|reaction|deletion
                payload_json   TEXT    NOT NULL,   -- the action's parameters
                defer_until_ts REAL    NOT NULL,   -- earliest wall-clock epoch to run
                attempts       INTEGER NOT NULL DEFAULT 0,
                created_ts     REAL    NOT NULL
            )
            """
        )
        # Lightweight migration: a DB created before event_id was added won't
        # have that column (CREATE TABLE IF NOT EXISTS is a no-op on an existing
        # table). Add it if missing. Idempotent and safe on fresh DBs too.
        cols = {
            r["name"]
            for r in self._conn.execute("PRAGMA table_info(deferred_actions)")
        }
        if "event_id" not in cols:
            self._conn.execute(
                "ALTER TABLE deferred_actions ADD COLUMN event_id INTEGER"
            )
        self._conn.commit()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    # ---- enqueue --------------------------------------------------------- #
    def enqueue(self, chat_id, action_type: str, payload: dict,
                defer_until_ts: Optional[float] = None,
                event_id: Optional[int] = None) -> int:
        """Append an outbound action to the tail of the FIFO queue.
        Returns the new row's seq."""
        now = time.time()
        if defer_until_ts is None:
            defer_until_ts = now
        cur = self._conn.execute(
            "INSERT INTO deferred_actions "
            "(event_id, chat_id, action_type, payload_json, defer_until_ts, attempts, created_ts) "
            "VALUES (?, ?, ?, ?, ?, 0, ?)",
            (event_id, str(chat_id), action_type, json.dumps(payload),
             defer_until_ts, now),
        )
        self._conn.commit()
        return cur.lastrowid

    # ---- peek / dequeue -------------------------------------------------- #
    def peek_head(self) -> Optional[sqlite3.Row]:
        """Return the FIFO head row (lowest seq), or None if the queue is empty."""
        cur = self._conn.execute(
            "SELECT * FROM deferred_actions ORDER BY seq ASC LIMIT 1"
        )
        return cur.fetchone()

    def delete(self, seq: int) -> None:
        self._conn.execute("DELETE FROM deferred_actions WHERE seq = ?", (seq,))
        self._conn.commit()

    def reappend(self, seq: int, defer_until_ts: float) -> int:
        """Move a row to the tail (new seq) with a new defer_until, incrementing
        attempts. Used for FLOOD retries so the action keeps its place BEHIND
        anything already queued but is retried later. Returns the new seq."""
        row = self._conn.execute(
            "SELECT event_id, chat_id, action_type, payload_json, attempts, created_ts "
            "FROM deferred_actions WHERE seq = ?", (seq,)
        ).fetchone()
        if row is None:
            return -1
        self._conn.execute("DELETE FROM deferred_actions WHERE seq = ?", (seq,))
        cur = self._conn.execute(
            "INSERT INTO deferred_actions "
            "(event_id, chat_id, action_type, payload_json, defer_until_ts, attempts, created_ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["event_id"], row["chat_id"], row["action_type"], row["payload_json"],
             defer_until_ts, row["attempts"] + 1, row["created_ts"]),
        )
        self._conn.commit()
        return cur.lastrowid

    def count(self) -> int:
        return self._conn.execute(
            "SELECT COUNT(*) AS n FROM deferred_actions"
        ).fetchone()["n"]
