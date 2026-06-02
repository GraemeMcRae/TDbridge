"""
TDbridge Configuration Module

Parses --env test|prod at import time, loads .env, and exposes a singleton
`config` object plus a standalone `localnow()` helper.

Design mirrors config_hcf.py from the HCF project so that table_manager.py
and google_sheets_connection.py can be shared without modification between
the two projects.  The only change required in those modules is that they
import from `config` (this file) rather than from `config_hcf`.

Usage in other modules:
    from config import config, localnow
"""

import argparse
import logging
import os
import platform as _platform_module
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Project root — the directory that contains this file.
# All relative file references (credentials, log file, SQLite DB) are
# resolved from here, so the bot runs correctly regardless of the current
# working directory (important for systemd service).
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.absolute()


# ---------------------------------------------------------------------------
# Standalone localnow() — defined BEFORE Config so it can be called during
# early logging setup, and so callers can do `from config import localnow`
# without importing the full config object.
# ---------------------------------------------------------------------------
def localnow() -> datetime:
    """Return the current time as a timezone-aware datetime in LOCAL_TIMEZONE.

    Falls back to UTC if LOCAL_TIMEZONE has not yet been loaded from .env.
    """
    tz_name = os.getenv("LOCAL_TIMEZONE", "UTC")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = timezone.utc
    return datetime.now(tz=tz)


# ---------------------------------------------------------------------------
# Date serial-number helpers (Google Sheets / Excel epoch)
# ---------------------------------------------------------------------------
_SHEETS_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)


def datetime_to_serial(dt: datetime) -> float:
    """Convert an aware datetime to a Google Sheets Date Serial Number.

    The serial number is calculated relative to LOCAL_TIMEZONE (not UTC),
    matching how Google Sheets interprets bare serial numbers when the
    spreadsheet's locale timezone is set to LOCAL_TIMEZONE.

    Returns a float (fractional days since 1899-12-30).
    """
    tz_name = os.getenv("LOCAL_TIMEZONE", "UTC")
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = timezone.utc
    # Convert to local time, then strip timezone to get a naive local datetime
    local_naive = dt.astimezone(local_tz).replace(tzinfo=None)
    # Calculate serial relative to epoch (also naive, so arithmetic is pure)
    epoch_naive = _SHEETS_EPOCH.replace(tzinfo=None)
    delta = local_naive - epoch_naive
    return delta.total_seconds() / 86400.0


def serial_to_datetime(serial: float) -> datetime:
    """Convert a Google Sheets Date Serial Number to an aware datetime.

    The serial is assumed to represent a local time in LOCAL_TIMEZONE
    (the same assumption used when writing serials via datetime_to_serial).
    Returns a timezone-aware datetime in LOCAL_TIMEZONE.

    NOTE: Do NOT add the UTC offset to the serial to get UTC — DST makes
    that incorrect for dates on the other side of the DST boundary.  Instead,
    interpret the serial as a naive local time and then attach the timezone.
    """
    tz_name = os.getenv("LOCAL_TIMEZONE", "UTC")
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = timezone.utc
    epoch_naive = _SHEETS_EPOCH.replace(tzinfo=None)
    from datetime import timedelta
    naive_local = epoch_naive + timedelta(days=serial)
    # Attach LOCAL_TIMEZONE WITHOUT offsetting the clock value
    return naive_local.replace(tzinfo=local_tz)


# ---------------------------------------------------------------------------
# Config class
# ---------------------------------------------------------------------------
class Config:
    """Singleton configuration object.

    Reads --env test|prod from sys.argv, loads the .env file, and exposes
    all parameters as plain attributes (no TEST_/PROD_ prefix).

    Parameter naming convention
    ---------------------------
    Environment-specific parameters use TEST_/PROD_ prefix in .env:
        TEST_DISCORD_BOT_TOKEN  →  config.discord_bot_token
        PROD_DISCORD_BOT_TOKEN  →  config.discord_bot_token

    Shared parameters have no prefix:
        LOCAL_TIMEZONE          →  config.local_timezone
        GOOGLE_CREDENTIALS_FILE →  config.google_credentials_file

    Accessing undefined parameters raises AttributeError (fail fast).
    """

    def __init__(self):
        # ------------------------------------------------------------------ #
        # 1. Parse --env argument                                             #
        # ------------------------------------------------------------------ #
        parser = argparse.ArgumentParser(
            description="TDbridge Telegram ↔ Discord message bridge",
            add_help=False,   # avoid conflict if called from other entry points
        )
        parser.add_argument(
            "--env",
            choices=["test", "prod"],
            required=True,
            help="Select configuration environment: test or prod",
        )
        # Parse only the known args so that unit-test runners (pytest etc.)
        # can pass additional arguments without breaking config init.
        args, _ = parser.parse_known_args()
        self.env: str = args.env                     # "test" or "prod"
        self.env_prefix: str = args.env.upper() + "_"  # "TEST_" or "PROD_"

        # ------------------------------------------------------------------ #
        # 2. Load .env from project root                                      #
        # ------------------------------------------------------------------ #
        env_file = PROJECT_ROOT / ".env"
        load_dotenv(dotenv_path=env_file)

        # ------------------------------------------------------------------ #
        # 3. Helper: read env var with TEST_/PROD_ prefix                    #
        # ------------------------------------------------------------------ #
        def get(suffix: str, required: bool = True) -> str:
            value = os.getenv(self.env_prefix + suffix, "")
            if required and not value:
                raise RuntimeError(
                    f"Missing required .env parameter: {self.env_prefix}{suffix}"
                )
            return value

        # ------------------------------------------------------------------ #
        # 4. Platform detection                                                #
        # ------------------------------------------------------------------ #
        # "Linux" on the Ubuntu VPS (production and test-on-server).
        # "Windows" on the developer workstation.
        # Used to select webhook vs. polling mode for Telegram, and to choose
        # the correct OS-level shutdown signal handling strategy.
        # Stored as the raw platform.system() string ("Linux" or "Windows")
        # so it matches the HCF project convention.
        self.platform: str = _platform_module.system()  # "Linux" or "Windows"

        # ------------------------------------------------------------------ #
        # 5. Shared (un-prefixed) parameters                                  #
        # ------------------------------------------------------------------ #
        self.local_timezone: str = os.getenv("LOCAL_TIMEZONE", "America/Los_Angeles")
        self.google_credentials_file: str = str(
            PROJECT_ROOT / os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
        )

        # ------------------------------------------------------------------ #
        # 6. Environment-specific parameters                                  #
        # ------------------------------------------------------------------ #
        # Discord
        self.discord_bot_token: str     = get("DISCORD_BOT_TOKEN")
        self.discord_bot_name: str      = get("DISCORD_BOT_NAME")
        self.discord_bot_nickname: str  = get("DISCORD_BOT_NICKNAME", required=False)
        self.discord_application_id: str = get("DISCORD_BOT_APPLICATION_ID")

        # Telegram
        self.telegram_bot_token: str    = get("TELEGRAM_BOT_TOKEN")
        self.telegram_bot_name: str     = get("TELEGRAM_BOT_NAME")
        self.telegram_bot_username: str = get("TELEGRAM_BOT_USERNAME")
        self.telegram_bot_url: str      = get("TELEGRAM_BOT_URL", required=False)

        # Telegram webhook
        # TELEGRAM_WEBHOOK_URL must be a publicly reachable HTTPS URL whose
        # path Telegram will POST updates to.
        # Example: https://myserver.example.com:8443/tgwebhook
        # On Windows dev with ngrok: https://<ngrok-id>.ngrok.io/tgwebhook
        self.telegram_webhook_url: str  = get("TELEGRAM_WEBHOOK_URL")
        self.telegram_webhook_port: int = int(
            os.getenv(self.env_prefix + "TELEGRAM_WEBHOOK_PORT", "8443")
        )
        # Secret token Telegram includes in X-Telegram-Bot-Api-Secret-Token header
        self.telegram_webhook_secret: str = get("TELEGRAM_WEBHOOK_SECRET", required=False)

        # Google Sheets
        self.google_spreadsheet_name: str = get("GOOGLE_SPREADSHEET_NAME")

        # SQLite message store
        db_filename = os.getenv(
            self.env_prefix + "SQLITE_DB_FILE",
            f"tdbridge_{self.env}.db"
        )
        self.sqlite_db_file: str = str(PROJECT_ROOT / db_filename)

        # Logging
        log_filename = os.getenv(
            self.env_prefix + "LOGFILENAME",
            f"tdbridge_{self.env}.log"
        )
        self.log_filename: str = str(PROJECT_ROOT / log_filename)

        # Sheets refresh interval (seconds). Default: 5 minutes.
        self.sheets_refresh_interval: int = int(
            os.getenv(self.env_prefix + "SHEETS_REFRESH_INTERVAL", "300")
        )

        # TLS certificate for the Telegram webhook HTTPS server.
        # These are shared (no TEST_/PROD_ prefix) because both instances run
        # on the same server and use the same Let's Encrypt certificate.
        # The cert files must be readable by the user running the bot.
        self.tls_cert_file: str = os.getenv(
            "TLS_CERT_FILE",
            "/etc/letsencrypt/live/hcf.squadrontrucking.com/fullchain.pem"
        )
        self.tls_key_file: str = os.getenv(
            "TLS_KEY_FILE",
            "/etc/letsencrypt/live/hcf.squadrontrucking.com/privkey.pem"
        )

        # How to bridge emoji reactions:
        #   "react"   — add the emoji as a native reaction on the target message
        #   "reply"   — post a short reply message describing the reaction
        #   "both"    — do both
        #   "neither" — do not bridge reactions at all
        self.reactions_ttod: str = os.getenv(
            self.env_prefix + "REACTIONS_TTOD", "reply"
        ).lower()
        self.reactions_dtot: str = os.getenv(
            self.env_prefix + "REACTIONS_DTOT", "reply"
        ).lower()

        # What to do in Telegram when a Discord message is deleted.
        # "delete" — attempt to delete the Telegram message
        # "ignore" — do nothing (log only)
        # Any other non-empty string — post that string as a Telegram reply
        #   (leading ! is converted to ⚠️)
        _raw_dc_delete = os.getenv(self.env_prefix + "DC_MSG_DELETE_BEHAVIOR", "delete")
        if _raw_dc_delete.startswith("!"):
            _raw_dc_delete = "⚠️" + _raw_dc_delete[1:]
        self.dc_msg_delete_behavior: str = _raw_dc_delete

        # Message posted on Telegram when TG deletion fails. Empty = silent.
        self.delete_fail_errmsg: str = _errmsg("DELETE_FAIL_ERRMSG", "")

        # Telegram-side delete command.
        # If non-empty, a Telegram reply whose full text matches this regex
        # (re.fullmatch — implicitly anchored to the entire message text)
        # triggers deletion of the parent TG message and its corresponding
        # Discord message.  Include \s* in the pattern to allow leading/trailing
        # whitespace, e.g. "(?i)delete\s*" to handle autocomplete trailing spaces.
        # Default "" disables the feature entirely.
        self.tg_msg_delete_regex: str = os.getenv(
            self.env_prefix + "TG_MSG_DELETE_REGEX", ""
        )

        # Message posted on Telegram when a TG delete command fails.
        # Empty string (the default) means no error message is posted.
        self.tg_msg_delete_errmsg: str = _errmsg("TG_MSG_DELETE_ERRMSG", "")

        # Convenience alias so table_manager.py can call config.bot_name
        self.bot_name: str = self.discord_bot_name

        # ------------------------------------------------------------------ #
        # 7. Set up logging                                                   #
        # ------------------------------------------------------------------ #
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure root logger with rotating file handler + console handler.

        Log format
        ----------
        Timestamps are in LOCAL_TIMEZONE with millisecond precision.
        Severity is rendered as a grep-friendly fixed-width token:
            " - INFO - "     for INFO
            " - WARNING - "  for WARNING
            " - ERROR - "    for ERROR
            " - DEBUG - "    for DEBUG
            " - CRITICAL - " for CRITICAL

        Example line:
            2026-05-30 14:23:07.412 PDT - INFO - TDbridgeTest: Discord bot ready

        Rotation
        --------
        Rotating file handler: 5 MiB per file, 5 backup files.
        At rotation: .log.5 is deleted, .log.4 → .log.5, … .log → .log.1,
        then the active log file is truncated and writing continues.

        Called once during __init__.  All modules obtain a child logger via
            logger = logging.getLogger(config.bot_name)
        which automatically inherits this configuration.
        """
        local_tz = ZoneInfo(self.local_timezone)

        # Custom formatter: LOCAL_TIMEZONE timestamps with milliseconds,
        # and grep-friendly " - LEVEL - " severity tokens.
        class LocalTimeFormatter(logging.Formatter):
            # Map stdlib level names to the fixed-width tokens we want.
            _LEVEL_TOKENS = {
                "DEBUG":    " - DEBUG - ",
                "INFO":     " - INFO - ",
                "WARNING":  " - WARNING - ",
                "ERROR":    " - ERROR - ",
                "CRITICAL": " - CRITICAL - ",
            }

            def formatTime(self, record, datefmt=None):  # noqa: N802
                dt = datetime.fromtimestamp(record.created, tz=local_tz)
                # Format base timestamp and append milliseconds + timezone
                base = dt.strftime("%Y-%m-%d %H:%M:%S")
                ms   = int(record.msecs)
                tz   = dt.strftime("%Z")
                return f"{base}.{ms:03d} {tz}"

            def format(self, record):
                # Replace the default levelname with our token so the full
                # formatted line looks like:
                #   <timestamp><token><name>: <message>
                token = self._LEVEL_TOKENS.get(record.levelname, f" - {record.levelname} - ")
                record = logging.makeLogRecord(record.__dict__)  # shallow copy
                record.levelname = token.strip(" -").strip()     # kept for compatibility
                self._style._fmt = "%(asctime)s" + token + "%(name)s: %(message)s"
                return super().format(record)

        fmt = LocalTimeFormatter()  # format string is set dynamically in format()

        root = logging.getLogger()
        root.setLevel(logging.INFO)

        # Rotating file handler: 5 MiB × 5 backups.
        # At rotation, Python's RotatingFileHandler renames:
        #   .log → .log.1 → .log.2 → … → .log.5 (oldest is deleted).
        fh = RotatingFileHandler(
            self.log_filename,
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)

        # Console handler — same format, useful during development
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        root.addHandler(ch)


# ---------------------------------------------------------------------------
# Singleton — instantiated once at module level so every import shares it
# ---------------------------------------------------------------------------
config = Config()
