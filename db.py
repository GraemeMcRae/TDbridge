"""
TDbridge SQLite Message Store (db.py)

Provides a persistent mapping between Telegram message IDs and Discord message
IDs so that replies, reactions, edits, and deletions can be correctly routed
in both directions.

Schema
------
message_map
    tg_group_id     TEXT    — Telegram group/chat ID (negative integer as text)
    tg_message_id   TEXT    — Telegram message ID
    dc_channel_id   TEXT    — Discord channel ID (snowflake as text)
    dc_message_id   TEXT    — Discord message ID (snowflake as text)
    root_tg_msg_id  TEXT    — Telegram message ID of the root (first) message
                              in the reply chain.  Allows Discord replies to
                              always route to the correct Telegram group even
                              after many levels of threading.
    dc_user_id      TEXT    — Discord user ID that the message is attributed to
                              (used for @mention tagging on Discord side)
    created_at      REAL    — Unix timestamp of when the record was created

Indexes are created on the most common lookup keys:
    - (tg_group_id, tg_message_id)  — Telegram event lookup
    - (dc_channel_id, dc_message_id) — Discord event lookup

Thread safety
-------------
SQLite connections are NOT thread-safe for sharing across threads.  Each
function opens its own connection, performs the operation, and closes it.
Because all async callers run this module's functions inside
`loop.run_in_executor(None, ...)`, blocking is kept off the event loop.

Usage
-----
    from db import init_db, store_message, find_by_tg, find_by_dc, delete_by_tg, delete_by_dc

    # At bot startup (synchronous):
    init_db()

    # From async code (wrap in executor):
    loop = asyncio.get_running_loop()
    record = await loop.run_in_executor(None, find_by_tg, tg_group_id, tg_message_id)
"""

import logging
import sqlite3
import time
from typing import Optional

from config import config

logger = logging.getLogger(config.bot_name)

# ---------------------------------------------------------------------------
# Database path comes from config (env-specific, e.g. "tdbridge_test.db")
# ---------------------------------------------------------------------------
_DB_PATH = config.sqlite_db_file

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS message_map (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_group_id     TEXT    NOT NULL,
    tg_message_id   TEXT    NOT NULL,
    dc_channel_id   TEXT    NOT NULL,
    dc_message_id   TEXT    NOT NULL,
    root_tg_msg_id  TEXT    NOT NULL,
    dc_user_id      TEXT    NOT NULL DEFAULT '',
    created_at      REAL    NOT NULL
);
"""

_CREATE_INDEX_TG_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_tg
    ON message_map (tg_group_id, tg_message_id);
"""

# Note: the DC index is NOT unique — a single Discord message can produce
# multiple Telegram messages (e.g. a photo album), so multiple rows may share
# the same (dc_channel_id, dc_message_id) pair.
_CREATE_INDEX_DC_SQL = """
CREATE INDEX IF NOT EXISTS idx_dc
    ON message_map (dc_channel_id, dc_message_id);
"""

# bot_status table — persists a small set of key-value pairs across restarts.
# Keys are plain strings; values are stored as text and interpreted by the
# caller.  There is at most one row per key (INSERT OR REPLACE).
_CREATE_BOT_STATUS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bot_status (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""

# gateway_queue table — events queued for delivery to a gateway peer that polls
# us. One row per queued event. The autoincrement id doubles as the delivery
# cursor / FIFO order. delivered_at is NULL until an event is returned in a poll
# response; for RequireACK gateways the row is retained after delivery and only
# removed by a matching ack, so an undelivered poll response can be retried.
_CREATE_GATEWAY_QUEUE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gateway_queue (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    gateway       TEXT    NOT NULL,
    chat_id       TEXT    NOT NULL,
    event_json    TEXT    NOT NULL,
    created_at    REAL    NOT NULL,
    delivered_at  REAL
);
"""

_CREATE_GATEWAY_QUEUE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_gwq_gateway
    ON gateway_queue (gateway, id);
"""


def _connect() -> sqlite3.Connection:
    """Open a new SQLite connection with WAL mode for better concurrency."""
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables and indexes if they don't exist.

    Call once at bot startup before any other db functions.
    This is safe to call repeatedly — uses CREATE IF NOT EXISTS.
    """
    with _connect() as conn:
        conn.execute(_CREATE_TABLE_SQL)
        conn.execute(_CREATE_INDEX_TG_SQL)
        # Drop the old UNIQUE index on (dc_channel_id, dc_message_id) if it
        # exists, because a single Discord message can now produce multiple
        # Telegram messages (photo albums), so the DC index must be non-unique.
        conn.execute("DROP INDEX IF EXISTS idx_dc")
        conn.execute(_CREATE_INDEX_DC_SQL)
        conn.execute(_CREATE_BOT_STATUS_TABLE_SQL)
        conn.execute(_CREATE_GATEWAY_QUEUE_TABLE_SQL)
        conn.execute(_CREATE_GATEWAY_QUEUE_INDEX_SQL)
    logger.info(f"SQLite message store initialised: {_DB_PATH}")


def store_message(
    tg_group_id: str,
    tg_message_id: str,
    dc_channel_id: str,
    dc_message_id: str,
    root_tg_msg_id: str,
    dc_user_id: str = "",
) -> None:
    """Store a Telegram ↔ Discord message ID mapping.

    If a mapping for (tg_group_id, tg_message_id) already exists it is
    replaced.  This handles edge cases such as a message being edited and
    re-sent as a new Discord message.

    Args:
        tg_group_id:    Telegram group/chat ID (as string, typically negative)
        tg_message_id:  Telegram message ID within that group
        dc_channel_id:  Discord channel ID where the bridged message lives
        dc_message_id:  Discord message ID of the bridged message
        root_tg_msg_id: Telegram message ID of the root of this reply chain.
                        For a new (non-reply) message, pass tg_message_id here.
        dc_user_id:     Discord user ID for @mention attribution (may be empty
                        if the Telegram sender has no Discord mapping)
    """
    sql = """
        INSERT OR REPLACE INTO message_map
            (tg_group_id, tg_message_id, dc_channel_id, dc_message_id,
             root_tg_msg_id, dc_user_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    with _connect() as conn:
        conn.execute(sql, (
            str(tg_group_id),
            str(tg_message_id),
            str(dc_channel_id),
            str(dc_message_id),
            str(root_tg_msg_id),
            str(dc_user_id),
            time.time(),
        ))
    logger.debug(
        f"DB store: tg({tg_group_id},{tg_message_id}) ↔ dc({dc_channel_id},{dc_message_id})"
    )


def find_by_tg(
    tg_group_id: str,
    tg_message_id: str,
) -> Optional[dict]:
    """Look up a record by Telegram group + message ID.

    Returns a dict with all columns, or None if not found.
    """
    sql = """
        SELECT * FROM message_map
        WHERE tg_group_id = ? AND tg_message_id = ?
        LIMIT 1
    """
    with _connect() as conn:
        row = conn.execute(sql, (str(tg_group_id), str(tg_message_id))).fetchone()
    return dict(row) if row else None


def find_by_dc(
    dc_channel_id: str,
    dc_message_id: str,
) -> Optional[dict]:
    """Look up a record by Discord channel + message ID.

    Returns a dict with all columns, or None if not found.
    """
    sql = """
        SELECT * FROM message_map
        WHERE dc_channel_id = ? AND dc_message_id = ?
        LIMIT 1
    """
    with _connect() as conn:
        row = conn.execute(sql, (str(dc_channel_id), str(dc_message_id))).fetchone()
    return dict(row) if row else None


def find_all_by_dc(
    dc_channel_id: str,
    dc_message_id: str,
) -> list[dict]:
    """Look up ALL records mapped to a given Discord channel + message ID.

    A single Discord message can produce multiple Telegram messages (e.g. a
    media group with several photos).  This returns all of them so that
    deletion of the Discord message can delete every corresponding Telegram
    message, not just the first one.

    Returns a list of dicts (may be empty if not found).
    """
    sql = """
        SELECT * FROM message_map
        WHERE dc_channel_id = ? AND dc_message_id = ?
        ORDER BY id
    """
    with _connect() as conn:
        rows = conn.execute(sql, (str(dc_channel_id), str(dc_message_id))).fetchall()
    return [dict(r) for r in rows]


def find_root_by_tg(
    tg_group_id: str,
    tg_message_id: str,
) -> Optional[dict]:
    """Follow the reply chain to its root and return that record.

    This is used when a Discord reply arrives: we look up the root Telegram
    message for the chain so the Telegram reply always goes to the correct
    group, even if the Discord user replied to a message many levels deep.

    Returns the root record dict, or None if the message is not in the store.
    """
    record = find_by_tg(tg_group_id, tg_message_id)
    if not record:
        return None
    root_id = record["root_tg_msg_id"]
    if root_id == tg_message_id:
        return record  # already the root
    return find_by_tg(tg_group_id, root_id) or record  # fallback to self


def delete_by_tg(tg_group_id: str, tg_message_id: str) -> bool:
    """Delete the record for a given Telegram message.

    Returns True if a row was deleted, False if it was not found.
    """
    sql = "DELETE FROM message_map WHERE tg_group_id = ? AND tg_message_id = ?"
    with _connect() as conn:
        cursor = conn.execute(sql, (str(tg_group_id), str(tg_message_id)))
    deleted = cursor.rowcount > 0
    if deleted:
        logger.debug(f"DB delete: tg({tg_group_id},{tg_message_id})")
    return deleted


def delete_by_dc(dc_channel_id: str, dc_message_id: str) -> int:
    """Delete ALL records for a given Discord message.

    A single Discord message may have produced multiple Telegram messages
    (e.g. a photo album), so multiple rows can share the same dc_message_id.
    This deletes all of them.

    Returns the number of rows deleted (0 if not found).
    """
    sql = "DELETE FROM message_map WHERE dc_channel_id = ? AND dc_message_id = ?"
    with _connect() as conn:
        cursor = conn.execute(sql, (str(dc_channel_id), str(dc_message_id)))
    count = cursor.rowcount
    if count:
        logger.debug(f"DB delete: dc({dc_channel_id},{dc_message_id}) — {count} row(s)")
    return count


def set_status_value(key: str, value: str) -> None:
    """Persist a key-value pair in the bot_status table.

    Uses INSERT OR REPLACE so the row is created on first write and updated
    on subsequent writes.  Thread-safe (opens its own connection).
    """
    sql = """
        INSERT OR REPLACE INTO bot_status (key, value, updated_at)
        VALUES (?, ?, ?)
    """
    with _connect() as conn:
        conn.execute(sql, (key, value, time.time()))


def get_status_value(key: str, default: str = "") -> str:
    """Retrieve a persisted key-value pair from the bot_status table.

    Returns `default` if the key does not exist.
    """
    sql = "SELECT value FROM bot_status WHERE key = ? LIMIT 1"
    with _connect() as conn:
        row = conn.execute(sql, (key,)).fetchone()
    return row[0] if row else default


def purge_older_than(days: int = 30) -> int:
    """Delete records older than `days` days.

    Keeps the database from growing indefinitely.  Call periodically
    (e.g. once per day from the scheduler).

    Returns the number of rows deleted.
    """
    cutoff = time.time() - (days * 86400)
    sql = "DELETE FROM message_map WHERE created_at < ?"
    with _connect() as conn:
        cursor = conn.execute(sql, (cutoff,))
    deleted = cursor.rowcount
    if deleted:
        logger.info(f"DB purge: removed {deleted} records older than {days} days")
    return deleted


# ===========================================================================
# Gateway event queue
# ===========================================================================
# Events queued for a gateway peer that polls us. See gateway_server.py and
# TDbridge_Gateway_Protocol.md. All functions are synchronous (call via
# run_in_executor from async code), matching the rest of this module.

def gateway_enqueue(gateway: str, chat_id: str, event_json: str) -> int:
    """Append one event to the queue for `gateway`. Returns the new row id
    (which is also the FIFO delivery order)."""
    sql = """
        INSERT INTO gateway_queue (gateway, chat_id, event_json, created_at, delivered_at)
        VALUES (?, ?, ?, ?, NULL)
    """
    with _connect() as conn:
        cursor = conn.execute(sql, (str(gateway), str(chat_id), str(event_json), time.time()))
        new_id = cursor.lastrowid
    logger.debug(f"Gateway queue: enqueued id={new_id} for gateway={gateway}")
    return int(new_id)


def gateway_peek(gateway: str, limit: int = 100) -> list:
    """Return up to `limit` queued events for `gateway`, oldest first, as a list
    of dicts {id, chat_id, event_json}. Does NOT mark them delivered."""
    sql = """
        SELECT id, chat_id, event_json
        FROM gateway_queue
        WHERE gateway = ?
        ORDER BY id ASC
        LIMIT ?
    """
    with _connect() as conn:
        rows = conn.execute(sql, (str(gateway), int(limit))).fetchall()
    return [{"id": r["id"], "chat_id": r["chat_id"], "event_json": r["event_json"]} for r in rows]


def gateway_mark_delivered(ids: list) -> None:
    """Mark the given queue row ids as delivered (sets delivered_at=now).
    Used for RequireACK gateways, which retain rows until acked."""
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    sql = f"UPDATE gateway_queue SET delivered_at = ? WHERE id IN ({placeholders})"
    with _connect() as conn:
        conn.execute(sql, (time.time(), *[int(i) for i in ids]))


def gateway_delete(ids: list) -> int:
    """Delete the given queue row ids. Returns the number of rows removed.
    Used both for non-RequireACK immediate dequeue and for ack-driven dequeue."""
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    sql = f"DELETE FROM gateway_queue WHERE id IN ({placeholders})"
    with _connect() as conn:
        cursor = conn.execute(sql, tuple(int(i) for i in ids))
    return cursor.rowcount


def gateway_delete_by_chat_and_msgids(gateway: str, chat_id: str, message_ids: list) -> int:
    """Delete queued events for `gateway`/`chat_id` whose payload message_id is
    in `message_ids`. Used by ack: the client acks by (chat_id, message_ids),
    not by our internal queue row id. Returns rows removed.

    Because the queue stores the serialized envelope, we match on the message_id
    inside each event's payload. We do this in Python rather than SQL/JSON1 to
    avoid depending on the SQLite JSON1 extension being present.
    """
    if not message_ids:
        return 0
    want = {str(m) for m in message_ids}
    sql = """
        SELECT id, event_json FROM gateway_queue
        WHERE gateway = ? AND chat_id = ?
    """
    to_delete = []
    import json as _json
    with _connect() as conn:
        rows = conn.execute(sql, (str(gateway), str(chat_id))).fetchall()
        for r in rows:
            try:
                env = _json.loads(r["event_json"])
                payload = env.get("payload", {}) or {}
                mid = payload.get("message_id")
                if mid is not None and str(mid) in want:
                    to_delete.append(r["id"])
            except Exception:
                continue
        if to_delete:
            placeholders = ",".join("?" for _ in to_delete)
            cursor = conn.execute(
                f"DELETE FROM gateway_queue WHERE id IN ({placeholders})",
                tuple(to_delete),
            )
            return cursor.rowcount
    return 0


def gateway_queue_depth(gateway: str = None) -> int:
    """Return the number of queued events, optionally for one gateway.
    Used by health reporting."""
    if gateway is None:
        sql = "SELECT COUNT(*) FROM gateway_queue"
        args = ()
    else:
        sql = "SELECT COUNT(*) FROM gateway_queue WHERE gateway = ?"
        args = (str(gateway),)
    with _connect() as conn:
        row = conn.execute(sql, args).fetchone()
    return int(row[0]) if row else 0
