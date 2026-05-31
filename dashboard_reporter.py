"""
dashboard_reporter.py — Periodic health-status heartbeat for the Manager Dashboard.

Emits one INFO-level log line every REPORT_INTERVAL_SECONDS (default 1800 = 30 min)
in a fixed pipe-delimited format that dashboard.sh can grep and parse.  Also emits
on startup and just before shutdown.

Log line format (one line, wrapped here for readability):
    <timestamp> - INFO - <bot_name>: Status Report | env=<env> | status=<OK|WARN|ERROR>
        | dc=<connected|disconnected> | tg_idle_min=<n> | sheets=<ok|error>
        | locked_min=<n> | bridged_30m=<n> | summary=<text>

Field definitions
-----------------
dc              "connected" or "disconnected" — Discord gateway state
tg_idle_min     Integer minutes since the last Telegram update was received
                (webhook or polling).  Dashboard decides what threshold is too high.
sheets          "ok" or "error" — whether the last Sheets API operation succeeded
locked_min      Integer minutes the longest-locked table has been locked.
                0 means no table is currently locked.  See lock detection below.
bridged_30m     Integer count of messages successfully bridged (both directions)
                since the last Status Report line was emitted.
summary         Free-form human-readable summary, chosen to be meaningful at a glance.

Status derivation
-----------------
OK    — dc connected, sheets ok, locked_min == 0
WARN  — sheets had a transient error, OR any table locked (locked_min > 0)
ERROR — dc disconnected

(tg_idle_min is reported raw; the dashboard decides the threshold.)

Lock detection
--------------
A table is considered "user-locked" if any column header begins with the string
"lock" (case-insensitive).  This is a convention from the Google_Sheets_Table_Layout
spec: the user renames a column to e.g. "Lock — editing" to prevent TDbridge
from writing to the sheet while they rearrange rows or columns.

For each table we maintain two aware datetimes:
    last_checked          — updated every time the lock state is inspected
    last_checked_unlocked — updated only when the table is found to be unlocked

locked_minutes for one table = floor((last_checked - last_checked_unlocked) / 60)

If a table has never been found locked, both timestamps are equal → locked_minutes = 0.
The reported locked_min is the maximum across all three tables.

Integration
-----------
See bot.py — DashboardReporter is instantiated there and integrated into the
startup, background task, and shutdown sequences.  sheets_manager.py updates
the lock timestamps.  bot.py increments status.bridged_30m after each
successful bridge in both directions.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

REPORT_INTERVAL_SECONDS = 1800   # 30 minutes
LOG_TAG                 = "Status Report"


class BotStatus:
    """Global status object — single instance shared across all modules.

    All attributes are safe to read and write from any thread or coroutine
    because Python GIL protects individual attribute assignments, and the
    values here are simple ints/bools/strings that are set atomically.

    Attributes updated by bot.py:
        dc_connected        True while the Discord gateway is connected.
        tg_last_update      Aware datetime of the last Telegram update received
                            (message, reaction, my_chat_member, etc.).  None until
                            the first update arrives.
        bridged_30m         Count of messages successfully bridged since the last
                            Status Report.  Reset after each report is emitted.

    Attributes updated by sheets_manager.py (one set per table):
        sheets_last_ok      True if the last Sheets API operation succeeded.
        d_user_last_checked         Aware datetime: last time D_User lock state was read.
        d_user_last_unlocked        Aware datetime: last time D_User was found unlocked.
        d_channel_last_checked      Same for D_Channel.
        d_channel_last_unlocked     Same for D_Channel.
        t_group_last_checked        Same for T_Group.
        t_group_last_unlocked       Same for T_Group.
    """

    def __init__(self):
        _now = datetime.now(tz=timezone.utc)

        # Discord
        self.dc_connected: bool = False

        # Telegram — None until the first update arrives
        self.tg_last_update: Optional[datetime] = None

        # Google Sheets
        self.sheets_last_ok: bool = True

        # Per-table lock tracking — both timestamps start at "now" so
        # locked_minutes = 0 until a lock is actually detected.
        self.d_user_last_checked:    datetime = _now
        self.d_user_last_unlocked:   datetime = _now
        self.d_channel_last_checked:  datetime = _now
        self.d_channel_last_unlocked: datetime = _now
        self.t_group_last_checked:   datetime = _now
        self.t_group_last_unlocked:  datetime = _now

        # Bridging counter — reset after each Status Report
        self.bridged_30m: int = 0


# Module-level singleton — imported by bot.py and sheets_manager.py
status = BotStatus()


def _save_unlocked_timestamps() -> None:
    """Persist the three last-unlocked timestamps to SQLite.

    Called by sheets_manager._update_lock_status() whenever a table is
    found to be unlocked, so the timestamps are immediately durable.

    This is a module-level function (not a DashboardReporter method) to
    avoid a circular import: bot.py creates the DashboardReporter instance,
    and sheets_manager.py cannot import from bot.py without creating a cycle.
    Instead, sheets_manager imports this function directly from
    dashboard_reporter, which has no dependency on bot.py.
    """
    import db as _db
    s = status
    try:
        _db.set_status_value("d_user_last_unlocked",
                             s.d_user_last_unlocked.isoformat())
        _db.set_status_value("d_channel_last_unlocked",
                             s.d_channel_last_unlocked.isoformat())
        _db.set_status_value("t_group_last_unlocked",
                             s.t_group_last_unlocked.isoformat())
    except Exception as e:
        logger.warning(f"_save_unlocked_timestamps: DB write failed: {e}")


class DashboardReporter:
    """Manages periodic Status Report log lines for the Manager Dashboard.

    Instantiate once in bot.py, start with asyncio.create_task(run_loop()),
    stop with stop() during graceful shutdown.
    """

    def __init__(self, cfg, bot_status: BotStatus):
        """
        Args:
            cfg:        config singleton (provides cfg.env, cfg.bot_name)
            bot_status: the module-level status singleton
        """
        self._config = cfg
        self._status = bot_status
        self._stop   = asyncio.Event()

    # ── Public API ─────────────────────────────────────────────────────────────

    def emit_startup(self) -> None:
        """Emit a Status Report line indicating successful startup."""
        self._emit(override_summary="TDbridge started successfully",
                   override_status="OK",
                   reset_counters=False)

    def emit_shutdown(self) -> None:
        """Emit a Status Report line just before shutdown."""
        self._emit(override_summary="TDbridge shutdown in progress",
                   override_status="WARN",
                   reset_counters=False)

    def stop(self) -> None:
        """Signal run_loop() to exit.  Safe to call from sync or async context."""
        self._stop.set()

    async def run_loop(self) -> None:
        """Background asyncio task — emit a periodic report every 30 minutes."""
        logger.info(
            f"DashboardReporter: periodic Status Report started "
            f"(interval={REPORT_INTERVAL_SECONDS}s)"
        )
        try:
            while not self._stop.is_set():
                # Sleep in 5-second slices so stop() is honoured quickly.
                elapsed = 0
                while elapsed < REPORT_INTERVAL_SECONDS and not self._stop.is_set():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(self._stop.wait()),
                            timeout=5.0,
                        )
                        break  # _stop was set
                    except asyncio.TimeoutError:
                        elapsed += 5

                if self._stop.is_set():
                    break

                self._emit()

        except asyncio.CancelledError:
            pass
        finally:
            logger.info("DashboardReporter: periodic Status Report stopped")

    # ── Persistence ────────────────────────────────────────────────────────────

    def load_from_db(self) -> None:
        """Restore persisted status fields from SQLite.

        Call once during startup, after init_db() but before emit_startup().
        Fields that are not found in the DB (e.g. first-ever run) are left
        at their default values from BotStatus.__init__().

        Persisted fields:
            tg_last_update  — aware datetime of the last Telegram update
                              received before the previous shutdown.  Restored
                              so that tg_idle_min is accurate immediately after
                              restart rather than showing 9999.
            bridged_30m     — rolling 30-minute bridge count.  Restored so the
                              next Status Report reflects activity before the
                              restart as well as after.
        """
        import db as _db
        s = self._status

        def _restore_dt(key: str, attr: str) -> None:
            """Restore a single datetime field from the DB."""
            raw = _db.get_status_value(key, "")
            if raw:
                try:
                    setattr(s, attr, datetime.fromisoformat(raw))
                    logger.info(f"DashboardReporter: restored {attr}={getattr(s, attr)}")
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"DashboardReporter: could not parse {key}={raw!r}: {e}"
                    )

        raw_tg = _db.get_status_value("tg_last_update", "")
        if raw_tg:
            try:
                s.tg_last_update = datetime.fromisoformat(raw_tg)
                logger.info(
                    f"DashboardReporter: restored tg_last_update={s.tg_last_update}"
                )
            except (ValueError, TypeError) as e:
                logger.warning(f"DashboardReporter: could not parse tg_last_update {raw_tg!r}: {e}")

        raw_bridged = _db.get_status_value("bridged_30m", "")
        if raw_bridged:
            try:
                s.bridged_30m = int(raw_bridged)
                logger.info(
                    f"DashboardReporter: restored bridged_30m={s.bridged_30m}"
                )
            except (ValueError, TypeError) as e:
                logger.warning(f"DashboardReporter: could not parse bridged_30m {raw_bridged!r}: {e}")

        # Restore the last-unlocked timestamps for all three tables.
        # These must survive a restart so that if a table is still locked
        # when the bot restarts, locked_min continues from where it left off
        # rather than resetting to 0.
        _restore_dt("d_user_last_unlocked",    "d_user_last_unlocked")
        _restore_dt("d_channel_last_unlocked", "d_channel_last_unlocked")
        _restore_dt("t_group_last_unlocked",   "t_group_last_unlocked")

    def save_to_db(self) -> None:
        """Persist status fields that should survive a restart.

        Called automatically after each periodic report (so values are at
        most 30 minutes stale after a crash) and during graceful shutdown.
        Also called by bot.py whenever tg_last_update changes, so the value
        is written frequently and stays accurate.
        """
        import db as _db
        s = self._status

        if s.tg_last_update is not None:
            _db.set_status_value("tg_last_update", s.tg_last_update.isoformat())
        _db.set_status_value("bridged_30m", str(s.bridged_30m))

        # Persist the last-unlocked timestamps for all three tables so that
        # locked_min is accurate after a restart even if a table is still locked.
        _db.set_status_value("d_user_last_unlocked",
                             s.d_user_last_unlocked.isoformat())
        _db.set_status_value("d_channel_last_unlocked",
                             s.d_channel_last_unlocked.isoformat())
        _db.set_status_value("t_group_last_unlocked",
                             s.t_group_last_unlocked.isoformat())

    # ── Private helpers ────────────────────────────────────────────────────────

    def _locked_minutes(self) -> int:
        """Return the maximum locked_minutes across all three tables."""
        s = self._status
        now = datetime.now(tz=timezone.utc)

        def _mins(last_checked: datetime, last_unlocked: datetime) -> int:
            delta = (last_checked - last_unlocked).total_seconds()
            return max(0, int(delta // 60))

        return max(
            _mins(s.d_user_last_checked,    s.d_user_last_unlocked),
            _mins(s.d_channel_last_checked,  s.d_channel_last_unlocked),
            _mins(s.t_group_last_checked,   s.t_group_last_unlocked),
        )

    def _tg_idle_minutes(self) -> int:
        """Return integer minutes since the last Telegram update was received."""
        if self._status.tg_last_update is None:
            # No update ever received — report a large number so dashboard notices.
            return 9999
        delta = datetime.now(tz=timezone.utc) - self._status.tg_last_update
        return max(0, int(delta.total_seconds() // 60))

    def _compute_status(self, locked_min: int) -> str:
        """Derive OK / WARN / ERROR from current status."""
        s = self._status
        if not s.dc_connected:
            return "ERROR"
        if not s.sheets_last_ok or locked_min > 0:
            return "WARN"
        return "OK"

    def _build_summary(self, overall_status: str, locked_min: int) -> str:
        """Build a concise human-readable summary."""
        s = self._status
        if not s.dc_connected:
            return "Discord disconnected"
        if not s.sheets_last_ok:
            return "Google Sheets error"
        if locked_min > 0:
            return f"Sheet locked {locked_min} min"
        return f"{s.bridged_30m} bridged, all systems nominal"

    def _emit(
        self,
        override_summary: Optional[str] = None,
        override_status: Optional[str] = None,
        reset_counters: bool = True,
    ) -> None:
        """Assemble and log one Status Report line."""
        s           = self._status
        locked_min  = self._locked_minutes()
        tg_idle_min = self._tg_idle_minutes()
        overall     = override_status or self._compute_status(locked_min)
        summary     = override_summary or self._build_summary(overall, locked_min)

        dc_str     = "connected" if s.dc_connected else "disconnected"
        sheets_str = "ok" if s.sheets_last_ok else "error"

        logger.info(
            f"{LOG_TAG} | "
            f"env={self._config.env} | "
            f"status={overall} | "
            f"dc={dc_str} | "
            f"tg_idle_min={tg_idle_min} | "
            f"sheets={sheets_str} | "
            f"locked_min={locked_min} | "
            f"bridged_30m={s.bridged_30m} | "
            f"summary={summary}"
        )

        if reset_counters:
            s.bridged_30m = 0

        # Persist after every emit so values survive a crash or restart.
        # Use a try/except so a DB failure never silences the log line.
        try:
            self.save_to_db()
        except Exception as e:
            logger.warning(f"DashboardReporter: could not persist status to DB: {e}")
