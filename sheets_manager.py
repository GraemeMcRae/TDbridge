"""
TDbridge Sheets Manager (sheets_manager.py)

Reads the three Google Sheets tables (D_User, D_Channel, T_Group) at startup
and refreshes them every SHEETS_REFRESH_INTERVAL seconds.  Exposes a simple
in-memory cache that the routing logic queries without hitting the Sheets API
on every message.

Tables used (from Google_Sheets_Table_Layout.txt)
--------------------------------------------------
D_User  (sheet: D_User_Sheet)
    D_ID            — Discord user snowflake (text)
    D_UserName      — Discord username (property of the user)
    D_Nickname      — Server nickname
    D_DisplayName   — Display name (property of the user)
    D_LastFound     — Serial date; written by TDbridge
    D_ChannelID     — Discord channel ID (user-maintained; used by TDbridge)
    D_ChannelName   — Human-readable channel name (user-maintained; not used)
    D_UserStatus    — "Active" / "Inactive" (user-maintained; used by TDbridge)
    T_GroupID       — Telegram group ID (user-maintained; used by TDbridge)
    T_Title         — Human-readable Telegram group title (user-maintained; not used)
    T_LastFound     — Serial date; not used by TDbridge

D_Channel  (sheet: D_Channel_Sheet)
    D_ChannelID     — Discord channel snowflake (text)
    D_ChannelName   — Human-readable name (written by TDbridge)
    D_LastFound     — Serial date; written by TDbridge
    D_ChannelStatus — "Active" / "Inactive" (user-maintained; used by TDbridge)

T_Group  (sheet: T_Group_Sheet)
    T_GroupID       — Telegram chat ID (text; negative for groups)
    T_Title         — Human-readable title (written by TDbridge)
    T_Type          — "group" / "supergroup" (written by TDbridge)
    T_LastFound     — Serial date; written by TDbridge
    T_Status        — "Active" / "Inactive" (user-maintained; used by TDbridge)

Routing tables exposed
----------------------
After a successful refresh, the module maintains:

    user_by_discord_id     : dict[str, dict]   D_ID → D_User row
    user_by_tg_group_id    : dict[str, dict]   T_GroupID → D_User row (Active only)
    channel_by_id          : dict[str, dict]   D_ChannelID → D_Channel row
    group_by_id            : dict[str, dict]   T_GroupID → T_Group row

Locking
-------
A threading.RLock protects the cache dicts.  Readers acquire the lock for the
duration of their lookup; the refresh thread acquires it only while swapping
in the new dictionaries.  Both Discord and Telegram event handlers are async,
so cache reads are done synchronously (no await needed — dict lookup is fast).
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from config import config, datetime_to_serial, localnow
# table_manager imports google_sheets_connection, which imports config.
# Import order must be: config → google_sheets_connection → table_manager
from table_manager import TableManager

logger = logging.getLogger(config.bot_name)


def _normalise_id(raw: object) -> str:
    """Return raw as a stripped string.

    All ID columns (D_ID, D_ChannelID, T_GroupID) are stored as Text in
    Google Sheets and are read back as Python strings via UNFORMATTED_VALUE.
    IDs arriving from the Telegram or Discord APIs are integers and must be
    converted to string at the point of receipt — never later.  This function
    is simply a safety net: it calls str() and strip() so that any ID value,
    regardless of where it came from, is a clean string in the cache.

    No numeric conversion is ever performed here.  IDs are treated as opaque
    strings throughout the Python codebase.
    """
    return str(raw).strip()


# ---------------------------------------------------------------------------
# TableManager instances (one per table)
# ---------------------------------------------------------------------------
_d_user_tm = TableManager(
    sheet_name="D_User_Sheet",
    required_columns=[
        "D_ID", "D_UserName", "D_Nickname", "D_DisplayName", "D_LastFound",
        "D_ChannelID", "D_ChannelName", "D_UserStatus", "T_GroupID",
        "T_Title", "T_LastFound",
    ],
)

_d_channel_tm = TableManager(
    sheet_name="D_Channel_Sheet",
    required_columns=["D_ChannelID", "D_ChannelName", "D_LastFound", "D_ChannelStatus"],
)

_t_group_tm = TableManager(
    sheet_name="T_Group_Sheet",
    required_columns=["T_GroupID", "T_Title", "T_Type", "T_LastFound", "T_Status"],
)

# ---------------------------------------------------------------------------
# In-memory cache + lock
# ---------------------------------------------------------------------------
_lock = threading.RLock()

user_by_discord_id: dict[str, dict]  = {}
user_by_tg_group_id: dict[str, dict] = {}
channel_by_id: dict[str, dict]       = {}
group_by_id: dict[str, dict]         = {}

_last_refresh_time: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Public lookup helpers (thread-safe, synchronous — safe to call from async)
# ---------------------------------------------------------------------------

def get_user_by_discord_id(discord_id: str) -> Optional[dict]:
    """Return the D_User row for a Discord user ID, or None."""
    with _lock:
        return user_by_discord_id.get(_normalise_id(discord_id))


def get_user_by_tg_group(tg_group_id: str) -> Optional[dict]:
    """Return the Active D_User row mapped to a Telegram group ID, or None."""
    with _lock:
        return user_by_tg_group_id.get(_normalise_id(tg_group_id))


def get_channel(channel_id: str) -> Optional[dict]:
    """Return the D_Channel row for a Discord channel ID, or None."""
    with _lock:
        return channel_by_id.get(_normalise_id(channel_id))


def get_tg_group(tg_group_id: str) -> Optional[dict]:
    """Return the T_Group row for a Telegram group ID, or None."""
    with _lock:
        return group_by_id.get(_normalise_id(tg_group_id))


def get_active_channels() -> list[dict]:
    """Return all D_Channel rows with D_ChannelStatus == 'Active'."""
    with _lock:
        return [r for r in channel_by_id.values() if _is_active(r.get("D_ChannelStatus", ""))]


def get_active_tg_groups() -> list[dict]:
    """Return all T_Group rows with T_Status == 'Active'."""
    with _lock:
        return [r for r in group_by_id.values() if _is_active(r.get("T_Status", ""))]


def _is_active(status: str) -> bool:
    """Return True if the status string contains 'Active' (case-insensitive)."""
    return "active" in status.lower() and "inactive" not in status.lower()


# ---------------------------------------------------------------------------
# Internal: build caches from freshly read table records
# ---------------------------------------------------------------------------

def _build_caches(
    user_records: list[dict],
    channel_records: list[dict],
    group_records: list[dict],
) -> None:
    """Rebuild in-memory routing caches from supplied record lists."""
    new_user_by_id: dict[str, dict]   = {}
    new_user_by_tg: dict[str, dict]   = {}
    new_channel: dict[str, dict]      = {}
    new_group: dict[str, dict]        = {}

    for row in user_records:
        did = _normalise_id(row.get("D_ID", ""))
        if did:
            new_user_by_id[did] = row
        tgid = _normalise_id(row.get("T_GroupID", ""))
        if tgid:
            status = row.get("D_UserStatus", "")
            active = _is_active(status)
            logger.info(
                f"Cache build: D_ID={did!r} T_GroupID={tgid!r} "
                f"D_UserStatus={status!r} → "
                f"{'added to user_by_tg_group_id' if active else 'SKIPPED (not active)'}"
            )
            if active:
                new_user_by_tg[tgid] = row

    for row in channel_records:
        cid = _normalise_id(row.get("D_ChannelID", ""))
        if cid:
            new_channel[cid] = row

    for row in group_records:
        gid = _normalise_id(row.get("T_GroupID", ""))
        if gid:
            new_group[gid] = row

    global user_by_discord_id, user_by_tg_group_id, channel_by_id, group_by_id
    global _last_refresh_time
    with _lock:
        user_by_discord_id  = new_user_by_id
        user_by_tg_group_id = new_user_by_tg
        channel_by_id       = new_channel
        group_by_id         = new_group
        _last_refresh_time  = localnow()

    logger.info(
        f"Sheets cache refreshed: "
        f"{len(new_user_by_id)} users, "
        f"{len(new_channel)} channels, "
        f"{len(new_group)} TG groups"
    )


def _build_caches_from_managers() -> None:
    """Rebuild caches from data already loaded in the TableManager instances.

    Use this instead of _refresh_sync() when the TableManagers have just
    finished a read-modify-write cycle and their .records are already current.
    Saves 6 API calls (3 × read_row1 + 3 × read_all) by skipping a redundant
    re-read of all three tables.
    """
    _build_caches(
        _d_user_tm.records,
        _d_channel_tm.records,
        _t_group_tm.records,
    )


# ---------------------------------------------------------------------------
# Synchronous refresh (runs in executor — must not be called on event loop)
# ---------------------------------------------------------------------------


def _update_lock_status(table_name: str, column_headers: list) -> None:
    """Update per-table lock timestamps in the dashboard status object.

    Called after every refresh_table() call.  A table is user-locked if any
    column header begins with "lock" (case-insensitive).

    Args:
        table_name:      One of "D_User", "D_Channel", "T_Group"
        column_headers:  List of column header strings from the sheet (row 1)
    """
    from datetime import timezone
    from datetime import datetime as _dt
    from dashboard_reporter import status as _status

    now = _dt.now(tz=timezone.utc)
    is_locked = any(h.lower().startswith("lock") for h in column_headers)

    _prefix_map = {
        "D_User":    "d_user",
        "D_Channel": "d_channel",
        "T_Group":   "t_group",
    }
    prefix = _prefix_map.get(table_name, table_name.lower().replace(" ", "_"))

    setattr(_status, f"{prefix}_last_checked", now)
    if not is_locked:
        setattr(_status, f"{prefix}_last_unlocked", now)
        # Persist the updated last-unlocked timestamp immediately so that if
        # the bot restarts while the table is locked, locked_min continues
        # from where it left off rather than resetting to 0.
        # We call save_to_db() on the module-level reporter instance in
        # dashboard_reporter rather than importing from bot.py (which would
        # create a circular import: bot → sheets_manager → bot).
        try:
            import dashboard_reporter as _dr_mod
            # The reporter instance is created in bot.py but save_to_db()
            # only needs the status object, which lives in dashboard_reporter.
            # We call it via a small helper to avoid tight coupling.
            _dr_mod._save_unlocked_timestamps()
        except Exception:
            pass
    else:
        lock_col = next(h for h in column_headers if h.lower().startswith("lock"))
        logger.warning(
            f"Table {table_name} is user-locked "
            f"(column starting with 'lock' found: {lock_col!r})"
        )


def _mark_sheets_ok(ok: bool) -> None:
    """Update sheets_last_ok in the dashboard status object."""
    try:
        from dashboard_reporter import status as _status
        _status.sheets_last_ok = ok
    except Exception:
        pass  # never let status tracking break the main code path


def _refresh_sync(
    skip_d_user: bool = False,
    skip_d_channel: bool = False,
) -> None:
    """Read tables from Sheets and rebuild the in-memory caches.

    This is the blocking implementation.  Always call via run_in_executor
    from async code so the Discord heartbeat is never blocked.

    Args:
        skip_d_user:    If True, skip re-reading D_User_Sheet (use when the
                        TableManager already holds current data, e.g. right
                        after batch_upsert_d_users_sync).
        skip_d_channel: If True, skip re-reading D_Channel_Sheet (same).

    If the TableManagers already have current data (e.g. right after a
    batch_upsert), call _build_caches_from_managers() instead to avoid
    redundant API calls entirely.
    """
    try:
        if not skip_d_user:
            _d_user_tm.refresh_table()
            _update_lock_status("D_User", _d_user_tm.actual_columns)
        if not skip_d_channel:
            _d_channel_tm.refresh_table()
            _update_lock_status("D_Channel", _d_channel_tm.actual_columns)
        _t_group_tm.refresh_table()
        _update_lock_status("T_Group", _t_group_tm.actual_columns)
        _build_caches_from_managers()
        _mark_sheets_ok(True)
    except Exception as e:
        _mark_sheets_ok(False)
        logger.error(f"Sheets refresh failed: {e}", exc_info=True)


def _startup_refresh_sync() -> None:
    """Optimised initial load for startup: read only T_Group_Sheet.

    D_User_Sheet and D_Channel_Sheet are intentionally skipped here because
    _refresh_discord_to_sheets() will perform a full read-modify-write on
    both tables immediately afterwards.  Reading them now would cost 4 API
    calls (2 × row1 + 2 × read_all) that are guaranteed to be redundant.

    T_Group_Sheet is read because it is not written during the Discord
    refresh cycle and its data is needed for routing as soon as the bot
    is ready.

    API calls saved vs. full _refresh_sync(): 4
    (D_User row1 + D_User read_all + D_Channel row1 + D_Channel read_all)
    """
    try:
        _t_group_tm.refresh_table()
        _update_lock_status("T_Group", _t_group_tm.actual_columns)
        _mark_sheets_ok(True)
        # D_User and D_Channel records are empty at this point; the cache
        # will be built correctly by _build_caches_from_managers() after
        # the Discord refresh upserts complete.
        logger.info(
            "Startup Sheets load: T_Group read; "
            "D_User and D_Channel deferred to Discord refresh"
        )
    except Exception as e:
        _mark_sheets_ok(False)
        logger.error(f"Startup Sheets load failed: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Async refresh entry points
# ---------------------------------------------------------------------------

async def refresh_async() -> None:
    """Async wrapper: refresh all tables without blocking the event loop."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _refresh_sync)


async def startup_refresh_async() -> None:
    """Async wrapper for the startup-optimised load (T_Group only)."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _startup_refresh_sync)


# ---------------------------------------------------------------------------
# Discord user/channel upsert helpers
# Called by the Discord event handler when it discovers a new user or channel.
# ---------------------------------------------------------------------------

def _upsert_d_user_sync(
    discord_id: str,
    username: str,
    nickname: str,
    display_name: str,
) -> None:
    """Insert or update a single D_User row.  Blocking — run in executor from async.

    Used for one-at-a-time upserts that happen during normal operation
    (e.g. a new Telegram group is discovered mid-session).
    For bulk startup refresh use batch_upsert_d_users_sync() instead.
    """
    batch_upsert_d_users_sync([{
        "discord_id":    discord_id,
        "username":      username,
        "nickname":      nickname,
        "display_name":  display_name,
    }])


def batch_upsert_d_users_sync(
    users: list[dict],
) -> None:
    """Insert or update multiple D_User rows in a single read-modify-write cycle.

    This is the correct function to call from _refresh_discord_to_sheets().
    It reads the table once, computes all inserts and updates, then writes
    everything in as few API calls as possible — O(1) reads instead of O(n).

    Args:
        users: List of dicts with keys:
               discord_id, username, nickname, display_name
    """
    if not users:
        return

    now_serial = datetime_to_serial(localnow())

    # Single read of the entire table
    _d_user_tm.refresh_table()
    _update_lock_status("D_User", _d_user_tm.actual_columns)
    all_ids = [u["discord_id"] for u in users]
    existing = _d_user_tm.find_rows_in_cache("D_ID", all_ids)

    updates: list[tuple[int, dict]] = []
    inserts: list[dict] = []

    for u in users:
        did = str(u["discord_id"])
        if did in existing:
            row_num, _ = existing[did]
            updates.append((row_num, {
                "D_UserName":    u["username"],
                "D_Nickname":    u["nickname"],
                "D_DisplayName": u["display_name"],
                "D_LastFound":   now_serial,
            }))
        else:
            inserts.append(u)

    # Process updates in one batch call
    if updates:
        _d_user_tm.batch_update_rows(updates)
        logger.info(f"Updated {len(updates)} D_User rows")
        # Keep the in-memory records current so callers can call
        # _build_caches_from_managers() without an extra API read.
        for row_num, row_dict in updates:
            rec_idx = row_num - 2  # records[0] = row 2
            if 0 <= rec_idx < len(_d_user_tm.records):
                _d_user_tm.records[rec_idx].update(row_dict)

    # Process inserts: insert all blank rows in one API call, then write data.
    if inserts:
        n = len(inserts)
        _d_user_tm.insert_rows_at_top([], n)
        # insert_rows_at_top already prepends blank records to _d_user_tm.records

        # The new rows occupy positions 2 .. n+1.
        new_rows_data = []
        for i, u in enumerate(inserts):
            row_num = 2 + i
            row_dict = {
                "D_ID":          u["discord_id"],
                "D_UserName":    u["username"],
                "D_Nickname":    u["nickname"],
                "D_DisplayName": u["display_name"],
                "D_LastFound":   now_serial,
                # User-maintained fields intentionally left empty on insert
            }
            new_rows_data.append((row_num, row_dict))
            # Keep in-memory records current
            _d_user_tm.records[i].update(row_dict)
            logger.info(
                f"Inserting new D_User row for Discord ID {u['discord_id']} ({u['username']})"
            )
        _d_user_tm.batch_update_rows(new_rows_data)
        logger.info(f"Inserted {n} new D_User rows")


async def upsert_d_user(
    discord_id: str,
    username: str,
    nickname: str,
    display_name: str,
) -> None:
    """Async wrapper for _upsert_d_user_sync (single record)."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, _upsert_d_user_sync, discord_id, username, nickname, display_name
    )


async def batch_upsert_d_users(users: list[dict]) -> None:
    """Async wrapper for batch_upsert_d_users_sync."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, batch_upsert_d_users_sync, users)


def _upsert_d_channel_sync(channel_id: str, channel_name: str) -> None:
    """Insert or update a single D_Channel row.  Blocking — run in executor from async.

    For bulk startup refresh use batch_upsert_d_channels_sync() instead.
    """
    batch_upsert_d_channels_sync([{"channel_id": channel_id, "channel_name": channel_name}])


def batch_upsert_d_channels_sync(channels: list[dict]) -> None:
    """Insert or update multiple D_Channel rows in a single read-modify-write cycle.

    Args:
        channels: List of dicts with keys: channel_id, channel_name
    """
    if not channels:
        return

    now_serial = datetime_to_serial(localnow())

    _d_channel_tm.refresh_table()
    _update_lock_status("D_Channel", _d_channel_tm.actual_columns)
    all_ids = [c["channel_id"] for c in channels]
    existing = _d_channel_tm.find_rows_in_cache("D_ChannelID", all_ids)

    updates: list[tuple[int, dict]] = []
    inserts: list[dict] = []

    for c in channels:
        cid = str(c["channel_id"])
        if cid in existing:
            row_num, _ = existing[cid]
            updates.append((row_num, {
                "D_ChannelName": c["channel_name"],
                "D_LastFound":   now_serial,
            }))
        else:
            inserts.append(c)

    if updates:
        _d_channel_tm.batch_update_rows(updates)
        logger.info(f"Updated {len(updates)} D_Channel rows")
        for row_num, row_dict in updates:
            rec_idx = row_num - 2
            if 0 <= rec_idx < len(_d_channel_tm.records):
                _d_channel_tm.records[rec_idx].update(row_dict)

    if inserts:
        n = len(inserts)
        _d_channel_tm.insert_rows_at_top([], n)
        new_rows_data = []
        for i, c in enumerate(inserts):
            row_dict = {
                "D_ChannelID":   c["channel_id"],
                "D_ChannelName": c["channel_name"],
                "D_LastFound":   now_serial,
            }
            new_rows_data.append((2 + i, row_dict))
            _d_channel_tm.records[i].update(row_dict)
            logger.info(
                f"Inserting new D_Channel row for {c['channel_id']} ({c['channel_name']})"
            )
        _d_channel_tm.batch_update_rows(new_rows_data)
        logger.info(f"Inserted {n} new D_Channel rows")


async def upsert_d_channel(channel_id: str, channel_name: str) -> None:
    """Async wrapper for _upsert_d_channel_sync (single record)."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _upsert_d_channel_sync, channel_id, channel_name)


async def batch_upsert_d_channels(channels: list[dict]) -> None:
    """Async wrapper for batch_upsert_d_channels_sync."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, batch_upsert_d_channels_sync, channels)


def _upsert_t_group_sync(
    tg_group_id: str,
    title: str,
    group_type: str,
) -> None:
    """Insert or update a T_Group row.  Blocking — run in executor from async."""
    now_serial = datetime_to_serial(localnow())

    _t_group_tm.refresh_table()
    existing = _t_group_tm.find_rows_in_cache("T_GroupID", [tg_group_id])

    if existing:
        row_num, _ = existing[str(tg_group_id)]
        _t_group_tm.batch_update_rows([(row_num, {
            "T_Title":     title,
            "T_Type":      group_type,
            "T_LastFound": now_serial,
        })])
        logger.info(f"Updated T_Group row for group {tg_group_id} ({title})")
    else:
        _t_group_tm.insert_rows_at_top([], 1)
        _t_group_tm.batch_update_rows([(2, {
            "T_GroupID":   tg_group_id,
            "T_Title":     title,
            "T_Type":      group_type,
            "T_LastFound": now_serial,
            # T_Status left empty for user to set
        })])
        logger.info(f"Inserted new T_Group row for {tg_group_id} ({title})")


async def upsert_t_group(tg_group_id: str, title: str, group_type: str) -> None:
    """Async wrapper for _upsert_t_group_sync."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _upsert_t_group_sync, tg_group_id, title, group_type)


# ---------------------------------------------------------------------------
# Status summary (used by bot status commands / logging)
# ---------------------------------------------------------------------------

def status_summary() -> str:
    """Return a one-line human-readable cache status string."""
    with _lock:
        ts = _last_refresh_time.strftime("%H:%M:%S %Z") if _last_refresh_time else "never"
        active_users = sum(
            1 for r in user_by_discord_id.values()
            if _is_active(r.get("D_UserStatus", ""))
        )
        active_groups = len(user_by_tg_group_id)
        active_channels = len(get_active_channels())
        return (
            f"Last refresh: {ts} | "
            f"Active users: {active_users} | "
            f"Active TG groups: {active_groups} | "
            f"Active DC channels: {active_channels}"
        )
