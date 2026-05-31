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

_CREATE_INDEX_DC_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_dc
    ON message_map (dc_channel_id, dc_message_id);
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
        conn.execute(_CREATE_INDEX_DC_SQL)
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


def delete_by_dc(dc_channel_id: str, dc_message_id: str) -> bool:
    """Delete the record for a given Discord message.

    Returns True if a row was deleted, False if it was not found.
    """
    sql = "DELETE FROM message_map WHERE dc_channel_id = ? AND dc_message_id = ?"
    with _connect() as conn:
        cursor = conn.execute(sql, (str(dc_channel_id), str(dc_message_id)))
    deleted = cursor.rowcount > 0
    if deleted:
        logger.debug(f"DB delete: dc({dc_channel_id},{dc_message_id})")
    return deleted


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
