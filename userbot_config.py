"""
TDbridge Userbot Configuration Module

Parses --env test|prod at import time, loads the SHARED .env (the same file
TDbridge uses), and exposes a singleton `userbot_config` object holding only
the userbot's parameter surface.

This is a deliberately separate module from `config.py`: the userbot is a
distinct program (Option E — a thin Telethon userbot that relays another bot's
Telegram messages to TDbridge over a loopback gateway). It shares two files with
TDbridge — the `.env` and `telegram_gateways.json` — but nothing else. Keeping
the config separate preserves the "TDbridge core is untouched" boundary.

All environment-specific parameters use the same TEST_/PROD_ prefix convention
as config.py, so a full test and a full prod userbot can run on one server at
once. Shared (un-prefixed) parameters — LOCAL_TIMEZONE, TELEGRAM_GATEWAYS — are
read exactly as TDbridge reads them.

Usage in other userbot modules:
    from userbot_config import userbot_config
"""

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

# The project root is the directory containing this file — identical to the
# resolution config.py uses, so relative paths (session file, SQLite DB, log)
# resolve the same way regardless of the process's working directory (important
# under systemd).
PROJECT_ROOT = Path(__file__).parent.absolute()


class UserbotConfig:
    """Singleton configuration for the TDbridge userbot.

    Exposes all parameters as plain attributes (no TEST_/PROD_ prefix). The
    environment is selected by --env, matching config.py.
    """

    def __init__(self) -> None:
        # ------------------------------------------------------------------ #
        # 1. Parse --env argument (same contract as config.py)               #
        # ------------------------------------------------------------------ #
        parser = argparse.ArgumentParser(
            description="TDbridge userbot (Telethon relay client)",
            add_help=False,   # avoid conflict when imported from other entry points
        )
        parser.add_argument(
            "--env",
            choices=["test", "prod"],
            required=True,
            help="Select configuration environment: test or prod",
        )
        # parse_known_args so test runners / the login script can pass extra
        # arguments without breaking config init.
        args, _ = parser.parse_known_args()
        self.env: str = args.env                        # "test" or "prod"
        self.env_prefix: str = args.env.upper() + "_"   # "TEST_" or "PROD_"

        # ------------------------------------------------------------------ #
        # 2. Load the SHARED .env from the project root                       #
        # ------------------------------------------------------------------ #
        env_file = PROJECT_ROOT / ".env"
        load_dotenv(dotenv_path=env_file)

        # ------------------------------------------------------------------ #
        # 3. Helper: read a TEST_/PROD_-prefixed value                        #
        # ------------------------------------------------------------------ #
        def get(suffix: str, required: bool = True, default: str = "") -> str:
            value = os.getenv(self.env_prefix + suffix, "")
            if not value:
                if required:
                    raise RuntimeError(
                        f"Missing required .env parameter: {self.env_prefix}{suffix}"
                    )
                return default
            return value

        # ------------------------------------------------------------------ #
        # 4. Telegram (Telethon) credentials and session                     #
        # ------------------------------------------------------------------ #
        # Telethon needs an api_id / api_hash (obtained once from
        # https://my.telegram.org, shared across environments is fine but we
        # keep them prefixed so test and prod can differ if ever needed).
        self.tg_api_id: int = int(get("USERBOT_API_ID"))
        self.tg_api_hash: str = get("USERBOT_API_HASH")

        # The dedicated account's phone number (used only at first login /
        # re-login; the saved session is used thereafter).
        self.phone: str = get("USERBOT_PHONE")

        # Optional two-step-verification password (only if the account has
        # two-step verification enabled). Distinct from the per-login code.
        self.two_fa_password: str = get("USERBOT_2FA_PASSWORD", required=False)

        # Session file — resolved from PROJECT_ROOT. Telethon appends .session
        # if the name has no extension; we store the path as given.
        session_name = get(
            "USERBOT_SESSION_FILE", required=False,
            default=f"tdbridge_{self.env}_userbot.session",
        )
        self.session_file: str = str(PROJECT_ROOT / session_name)

        # ------------------------------------------------------------------ #
        # 5. Relay rule: which bot's messages we DON'T relay                  #
        # ------------------------------------------------------------------ #
        # We relay messages from ANY bot in the group EXCEPT this one — our own
        # sibling TDbridge bot, whose messages are already on the bridge. Stated
        # as a denylist-of-one so the partner can add/rename their own bots
        # without notifying us. Matched against the sender's username.
        self.gw_bot_username: str = get("USERBOT_GW_BOT")

        # ------------------------------------------------------------------ #
        # 6. Outbound metering and FLOOD handling                            #
        # ------------------------------------------------------------------ #
        # Minimum spacing between OUTBOUND Telegram actions (posts/reactions),
        # to stay well under Telegram's user-account rate limits. Inbound
        # reading is never metered. Default 2000 ms.
        self.metering_ms: int = int(
            get("USERBOT_METERING_MILLISEC", required=False, default="2000")
        )

        # Behavior when a FLOOD_WAIT exceeds Telethon's auto-sleep threshold:
        #   "retry" — honor the wait Telegram specifies, then retry (bounded).
        #   "save"  — persist the pending action locally and move on.
        self.flood_action: str = get(
            "USERBOT_FLOOD", required=False, default="retry"
        ).strip().lower()
        if self.flood_action not in ("retry", "save"):
            self.flood_action = "retry"

        # Ceiling (seconds) for "retry": if Telegram asks for a longer wait than
        # this, fall through to the "save" behavior instead of blocking.
        self.flood_retry_max_sec: int = int(
            get("USERBOT_FLOOD_RETRY_MAX_SEC", required=False, default="120")
        )

        # ------------------------------------------------------------------ #
        # 7. Login code retry (terminal login)                                #
        # ------------------------------------------------------------------ #
        # How long ensure_logged_in waits for a code before re-requesting a
        # fresh one (Telegram codes expire in minutes), and how many times to
        # retry before giving up (restart to try again).
        self.login_code_wait_min: int = int(
            get("USERBOT_LOGIN_CODE_WAIT_MIN", required=False, default="15")
        )
        self.login_code_max_retries: int = int(
            get("USERBOT_LOGIN_CODE_MAX_RETRIES", required=False, default="3")
        )

        # ------------------------------------------------------------------ #
        # 8. The gateway this userbot connects to (as a CLIENT)              #
        # ------------------------------------------------------------------ #
        # Shared gateways JSON (same file TDbridge uses; un-prefixed path).
        self.gateways_file: str = os.getenv("TELEGRAM_GATEWAYS", "").strip()
        gateways_path = (
            str(PROJECT_ROOT / self.gateways_file) if self.gateways_file else ""
        )

        # Which gateway name is ours to connect to (e.g. "Userbot_gw_prod").
        self.gateway_name: str = get("USERBOT_GATEWAY")

        # Load the shared JSON and pull out our GatewayDef. load_gateways now
        # accepts a loopback http:// url (the surgical TDbridge change), so the
        # Userbot_gw_* entries can be plain-HTTP loopback endpoints.
        from gateway_config import load_gateways
        all_gateways = load_gateways(gateways_path) if gateways_path else {}
        if self.gateway_name not in all_gateways:
            raise RuntimeError(
                f"Userbot gateway '{self.gateway_name}' not found in "
                f"{self.gateways_file!r}. Define it (loopback http:// url, "
                f"echo=false) in the shared gateways JSON."
            )
        self.gateway = all_gateways[self.gateway_name]

        # ------------------------------------------------------------------ #
        # 9. Local SQLite state (small; for FLOOD-deferred actions and dedupe)#
        # ------------------------------------------------------------------ #
        db_filename = get(
            "USERBOT_SQLITE_DB_FILE", required=False,
            default=f"tdbridge_{self.env}_userbot.db",
        )
        self.sqlite_db_file: str = str(PROJECT_ROOT / db_filename)

        # ------------------------------------------------------------------ #
        # 10. Logging                                                         #
        # ------------------------------------------------------------------ #
        log_filename = get(
            "USERBOT_LOGFILENAME", required=False,
            default=f"tdbridge_{self.env}_userbot.log",
        )
        self.log_filename: str = str(PROJECT_ROOT / log_filename)

        # ------------------------------------------------------------------ #
        # 11. Shared (un-prefixed) parameters                                 #
        # ------------------------------------------------------------------ #
        self.local_timezone: str = os.getenv(
            "LOCAL_TIMEZONE", "America/Los_Angeles"
        )


# Module-level singleton, constructed on import (mirrors config.py).
userbot_config = UserbotConfig()
