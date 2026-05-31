# TDbridge Project Structure

## Overview

TDbridge is a Python-based bot service that transparently bridges messages, replies, reactions, attachments, and edits between Telegram groups and a Discord server.  It allows one set of users to interact exclusively on Discord while another party interacts exclusively on Telegram.  Both sides appear to be in a single conversation.

**Design model:** Discord messages are funneled into a small number of monitored channels, with @mentions used to identify the target Telegram group.  Telegram messages are dispersed across a large number of groups, with the group name used to identify the sender on the Discord side.  TDbridge makes both platforms appear as one conversation.

---

## Team

**Graeme McRae** — Lead developer and system designer.  Makes all major design decisions.  Works with Claude as the primary coder and documenter.

**Maclyn** — Co-developer with equal access.  Contributing ideas and feature guidance.

**Claude (Anthropic)** — Primary coder and documenter.

---

## Development Environment

### Developer Workstations

Windows PCs with Git Bash as the terminal environment.  Python development is done locally in a virtual environment.

```
python -m venv venv
source venv/Scripts/activate   # Git Bash on Windows
pip install -r requirements.txt
```

### Version Control

```
~/TDbridge on Graeme's Windows PC
       ↕ git push / pull
   GitHub (GraemeMcRae/TDbridge, private)
       ↕ git pull
~/TDbridge on Production Server (graeme@hcf.squadrontrucking.com)
```

### Production Server

Same VPS as HCF (`hcf.squadrontrucking.com`, Ubuntu 24.04, 6 GB RAM).  TDbridge runs as a separate systemd service alongside HCF.

---

## Architecture

```
Telegram Groups (one per mapped user, webhook) 
        ↕ python-telegram-bot (webhook)
   TDbridge bot.py — single Python process, single asyncio event loop
        ↕ discord.py (gateway)
Discord Server (monitored channels + webhooks)
        ↕
SQLite message store (tdbridge_test.db / tdbridge_prod.db)
        ↕
Google Sheets (D_User_Sheet / D_Channel_Sheet / T_Group_Sheet)
```

TDbridge runs as a **single Python process** with one asyncio event loop shared by both discord.py (gateway connection) and python-telegram-bot (webhook server).  The Telegram webhook server starts after Discord reports `on_ready`.

---

## File Structure

```
TDbridge/
├── bot.py                        # Entry point; Discord + Telegram event handlers
├── config.py                     # Config singleton; parses --env; loads .env
├── db.py                         # SQLite message ID store
├── sheets_manager.py             # Google Sheets cache; upsert helpers
├── google_sheets_connection.py   # gspread connection (shared with HCF pattern)
├── table_manager.py              # Generic Sheets table engine (shared with HCF)
├── requirements.txt
├── .env                          # NOT committed; copy manually to each env
├── .env.example                  # Template with all parameter names
├── google_credentials_TDbridge.json  # NOT committed
├── .gitignore
└── TDbridge_Project_Structure.md  # This file
```

### Shared Modules from HCF

`table_manager.py` and `google_sheets_connection.py` are copied from the HCF project.  The only difference from the HCF copies is the import line: they import `config` (TDbridge's `config.py`) rather than `config_hcf`.  When changes are needed, both projects' copies must be updated in sync.

---

## File Descriptions

### `config.py`

Parses `--env test|prod` at import time, loads `.env`, and exposes a singleton `config` object.  Mirrors the `config_hcf.py` pattern from HCF exactly so that `table_manager.py` and `google_sheets_connection.py` work without modification.

Key attributes:

| Attribute | Source in .env | Example |
|---|---|---|
| `config.env` | `--env` argument | `"test"` |
| `config.bot_name` | `TEST_DISCORD_BOT_NAME` | `"TDbridgeTest"` |
| `config.discord_bot_token` | `TEST_DISCORD_BOT_TOKEN` | `"…"` |
| `config.telegram_bot_token` | `TEST_TELEGRAM_BOT_TOKEN` | `"…"` |
| `config.telegram_webhook_url` | `TEST_TELEGRAM_WEBHOOK_URL` | `"https://…/tgwebhook"` |
| `config.telegram_webhook_port` | `TEST_TELEGRAM_WEBHOOK_PORT` | `8443` |
| `config.telegram_webhook_secret` | `TEST_TELEGRAM_WEBHOOK_SECRET` | `"…"` |
| `config.google_spreadsheet_name` | `TEST_GOOGLE_SPREADSHEET_NAME` | `"TDbridge Config Test"` |
| `config.sqlite_db_file` | `TEST_SQLITE_DB_FILE` | `"tdbridge_test.db"` |
| `config.log_filename` | `TEST_LOGFILENAME` | `"tdbridge_test.log"` |
| `config.local_timezone` | `LOCAL_TIMEZONE` | `"America/Los_Angeles"` |
| `config.google_credentials_file` | `GOOGLE_CREDENTIALS_FILE` | `"google_credentials_TDbridge.json"` |

Also exports:
- `localnow()` — returns `datetime.now()` in `LOCAL_TIMEZONE`
- `datetime_to_serial(dt)` — converts an aware datetime to a Google Sheets Date Serial Number
- `serial_to_datetime(serial)` — inverse of the above

### `db.py`

SQLite message store.  Every bridged message creates a record mapping:

| Column | Description |
|---|---|
| `tg_group_id` | Telegram chat ID (text, negative for groups) |
| `tg_message_id` | Telegram message ID |
| `dc_channel_id` | Discord channel snowflake |
| `dc_message_id` | Discord message snowflake |
| `root_tg_msg_id` | Telegram message ID of the reply-chain root |
| `dc_user_id` | Discord user ID for attribution |
| `created_at` | Unix timestamp |

Unique indexes on `(tg_group_id, tg_message_id)` and `(dc_channel_id, dc_message_id)`.

Public API: `init_db()`, `store_message()`, `find_by_tg()`, `find_by_dc()`, `find_root_by_tg()`, `delete_by_tg()`, `delete_by_dc()`, `purge_older_than()`.

Each function opens its own connection so it is safe to call from any thread.

### `sheets_manager.py`

Reads the three Google Sheets tables and maintains an in-memory cache.  Refreshes every `SHEETS_REFRESH_INTERVAL` seconds (default 300).

**Tables read:**

| Sheet | Table | Key columns used by TDbridge |
|---|---|---|
| D_User_Sheet | D_User | D_ID, D_ChannelID, D_UserStatus, T_GroupID |
| D_Channel_Sheet | D_Channel | D_ChannelID, D_ChannelStatus |
| T_Group_Sheet | T_Group | T_GroupID, T_Status |

**Cache dictionaries (module-level, protected by `threading.RLock`):**

| Name | Key | Value |
|---|---|---|
| `user_by_discord_id` | D_ID | D_User row dict |
| `user_by_tg_group_id` | T_GroupID | D_User row dict (Active only) |
| `channel_by_id` | D_ChannelID | D_Channel row dict |
| `group_by_id` | T_GroupID | T_Group row dict |

**Public async upsert functions:**
- `upsert_d_user(discord_id, username, nickname, display_name)` — insert or update a D_User row
- `upsert_d_channel(channel_id, channel_name)` — insert or update a D_Channel row
- `upsert_t_group(tg_group_id, title, group_type)` — insert or update a T_Group row

All upserts run in an executor to avoid blocking the event loop.

### `google_sheets_connection.py`

Identical in function to the HCF version.  Creates a global `sheet` (gspread.Spreadsheet) at import time.  Imported by `table_manager.py`.

### `table_manager.py`

Shared with HCF.  Generic Google Sheets table engine providing:
- Column-order-independent reads (reads header row first)
- Insert-at-top with empty rows (inherits formatting/formulas from row below)
- Batch update with exponential backoff retry
- Both sync and async versions of all write operations
- Thread-safe via a global `threading.RLock`

### `bot.py`

Main entry point.  Creates `TDbridgeDiscordClient` (subclass of `discord.Client`) and the python-telegram-bot `Application`.  Both share the same asyncio event loop.

**Startup sequence:**
1. `asyncio.run(runner())` starts the Discord client
2. Discord fires `on_ready`
3. `_startup()` runs: SQLite init → Sheets load → Telegram webhook start → background tasks

**Background tasks:**
- `_sheets_refresh_loop()` — refreshes Sheets cache every `SHEETS_REFRESH_INTERVAL` seconds
- `_db_purge_loop()` — purges SQLite records older than 30 days, once per day

---

## Google Sheets Tables

See `Google_Sheets_Table_Layout.txt` for the authoritative column definitions.

### D_User (D_User_Sheet)

| Column | Written by | Used by TDbridge |
|---|---|---|
| D_ID | TDbridge | Yes — lookup key |
| D_UserName | TDbridge | No |
| D_Nickname | TDbridge | No |
| D_DisplayName | TDbridge | No |
| D_LastFound | TDbridge | No |
| D_ChannelID | User | Yes — target Discord channel |
| D_ChannelName | User/formula | No |
| D_UserStatus | User | Yes — Active/Inactive |
| T_GroupID | User/formula | Yes — source Telegram group |
| T_Title | User/formula | No |
| T_LastFound | User/formula | No |

On insert, all user-maintained columns are left empty.  TDbridge never overwrites user-maintained columns on update.

If `D_UserStatus` is blank or `"Inactive"`, the user is not routed.  If a permanent API error occurs for a user, `"Error"` is appended to `D_UserStatus` (e.g. `"Active, Error"`).

### D_Channel (D_Channel_Sheet)

| Column | Written by | Used by TDbridge |
|---|---|---|
| D_ChannelID | TDbridge | Yes — lookup key |
| D_ChannelName | TDbridge | No |
| D_LastFound | TDbridge | No |
| D_ChannelStatus | User | Yes — Active/Inactive |

### T_Group (T_Group_Sheet)

| Column | Written by | Used by TDbridge |
|---|---|---|
| T_GroupID | TDbridge | Yes — lookup key |
| T_Title | TDbridge | No |
| T_Type | TDbridge | No |
| T_LastFound | TDbridge | No |
| T_Status | User | Yes — Active/Inactive |

---

## Message Routing Rules

### Telegram → Discord

| Event | Action |
|---|---|
| New message in a mapped Active group | Post to mapped Discord channel via webhook. Display name: `[SenderName] [TG]`. Tag mapped Discord user (`<@id>`). Store TG↔DC ID mapping. |
| New message in an unmapped group | Post to first Active DC channel. Use `"@ GroupName"` (space prevents real Discord mention). |
| Reply to a bridged message | Post as a Discord reply to the previously bridged message. |
| Forwarded message | Post as a new message (not a reply) with `↪️ Forwarded from …` prefix. |
| Edited message | Attempt to edit the Discord message; fall back to a new reply with `✏️ EDIT —` prefix. |
| Reaction | Post as a reply: `{emoji} SenderName reacted to this message`. |
| Photo / video / voice / file | Re-downloaded and re-uploaded to Discord. Files >25 MB replaced with text notice. |
| Sticker (static) | Bridged as .webp image. |
| Sticker (animated/video) | Bridged as text `[Sticker: emoji]`. |
| Poll | Bridged as text summary: question + options. |

### Discord → Telegram

| Event | Action |
|---|---|
| Reply to a bridged message | Send to the Telegram group of the reply-chain root. |
| New message tagging a mapped user | Send to that user's Telegram group. Multiple tags → first mapped user's group. |
| New message, sender is a mapped user, no tag | Send to sender's own Telegram group. |
| New message, no tag, sender not mapped or not Active | Behaviour controlled by `UNROUTABLE_BEHAVIOR`: `warn` posts a reply in Discord; `ignore` logs only. Message not forwarded. |
| Edited message | Attempt to edit TG message; fall back to a new reply with edit prefix. |
| Deleted message | Configurable via `DELETE_BEHAVIOR` (.env): `"delete"` (default) / `"notify"` / `"ignore"`. |
| Reaction | Post as a Telegram reply: `{emoji} UserName (Discord) reacted to this message`. |

---

## Configuration (.env)

All parameters follow the `TEST_`/`PROD_` prefix pattern.  The `--env` argument selects the active prefix.

See `envexample.txt` for the canonical list of all parameters with inline
comments.  See `TDbridge_Guide.md` § Configuration for usage guidance.
The full parameter reference is reproduced here for documentation purposes.

```
# Telegram webhook (Linux only; polling is used automatically on Windows)
TEST_TELEGRAM_WEBHOOK_URL="https://hcf.squadrontrucking.com:88/tgwebhook"
TEST_TELEGRAM_WEBHOOK_PORT=88
TEST_TELEGRAM_WEBHOOK_SECRET="<random-hex-32>"   # authenticate Telegram POSTs

PROD_TELEGRAM_WEBHOOK_URL="https://hcf.squadrontrucking.com:8443/tgwebhook"
PROD_TELEGRAM_WEBHOOK_PORT=8443
PROD_TELEGRAM_WEBHOOK_SECRET="<random-hex-32>"

# SQLite message store
TEST_SQLITE_DB_FILE=TDbridge_test.db
PROD_SQLITE_DB_FILE=TDbridge_prod.db

# Log file
TEST_LOGFILENAME=TDbridge_test.log
PROD_LOGFILENAME=TDbridge_prod.log

# Sheets mapping cache refresh interval (seconds)
TEST_SHEETS_REFRESH_INTERVAL=300
PROD_SHEETS_REFRESH_INTERVAL=300

# Unroutable Discord message behavior: "warn" | "ignore"
#   warn   — log WARNING + post a reply in Discord
#   ignore — log INFO only, no Discord message
# Use "warn" during setup; switch to "ignore" once stable.
TEST_UNROUTABLE_BEHAVIOR=warn
PROD_UNROUTABLE_BEHAVIOR=warn

# Message deletion behavior: "delete" | "notify" | "ignore"
#   delete — attempt to delete the Telegram message
#   notify — post a Telegram reply noting the deletion
#   ignore — do nothing (log only)
TEST_DELETE_BEHAVIOR=delete
PROD_DELETE_BEHAVIOR=delete

# If Telegram deletion fails, post a notification?
TEST_DELETE_FAIL_NOTIFY=true
PROD_DELETE_FAIL_NOTIFY=true

# Reaction bridging: "react" | "reply" | "both" | "neither"
#   react   — add emoji as a native platform reaction
#   reply   — post a short reply message describing the reaction
#   both    — do both (native reaction falls back gracefully if unsupported)
#   neither — do not bridge reactions
# Note: Telegram native reactions support only a limited emoji set.
TEST_REACTIONS_TTOD=reply    # Telegram → Discord
TEST_REACTIONS_DTOT=reply    # Discord → Telegram
PROD_REACTIONS_TTOD=reply
PROD_REACTIONS_DTOT=reply

# TLS certificate (shared; both instances on the same server use the same cert)
TLS_CERT_FILE=/etc/letsencrypt/live/hcf.squadrontrucking.com/fullchain.pem
TLS_KEY_FILE=/etc/letsencrypt/live/hcf.squadrontrucking.com/privkey.pem
```

---

## Platform Differences

TDbridge detects the operating system at startup via `platform.system()` and
stores the result as `config.platform` (`"Linux"` or `"Windows"`).  This value
drives two platform-specific behaviours:

### Telegram transport: webhook vs. polling

| Platform | Mode | How it works |
|---|---|---|
| Linux (server) | Webhook | Bot runs its own HTTPS server; Telegram POSTs updates to it immediately |
| Windows (dev) | Polling | Bot asks Telegram for new updates every few seconds; no port or cert needed |

The switch happens entirely inside `_start_telegram_app()` in `bot.py`.  All
handler functions (`route_tg_to_discord`, `route_tg_edit_to_discord`, etc.) are
identical in both modes — only the transport layer differs.  This means Windows
testing gives a valid indication of Linux production behaviour.

On Windows startup, any previously registered webhook is deleted first
(`delete_webhook()`) so that polling and webhooking cannot conflict.

The `.env` webhook parameters (`TEST_TELEGRAM_WEBHOOK_URL`, etc.) are read on
Linux only; they are ignored on Windows.  No env var toggle is needed — platform
detection is fully automatic.

### Shutdown signal handling

| Platform | Shutdown mechanism |
|---|---|
| Linux | `SIGTERM` from systemd (or `SIGINT` from Ctrl-C); handled via `asyncio` signal handlers registered at startup |
| Windows | `KeyboardInterrupt` (Ctrl-C only); `asyncio` signal handlers are not supported on Windows |

Both paths call the same `_shutdown()` coroutine, which cancels background
tasks and stops the Telegram application cleanly before exiting.

---

## Webhook Setup

### Production (Ubuntu VPS)

The Telegram bot API delivers updates to `https://hcf.squadrontrucking.com:8443/tgwebhook`.  Port 8443 is one of Telegram's allowed webhook ports.  Open it in the firewall:

```bash
sudo ufw allow 8443/tcp
```

The TDbridge process listens on `0.0.0.0:8443` internally (no nginx proxy required for webhook-only traffic on a non-standard port).

### Development (Windows + ngrok)

Install ngrok and expose the webhook port:

```bash
ngrok http 8443
```

Set `TEST_TELEGRAM_WEBHOOK_URL` to the ngrok HTTPS URL, e.g.:
```
TEST_TELEGRAM_WEBHOOK_URL="https://abc123.ngrok.io/tgwebhook"
```

The webhook URL is re-registered with Telegram each time the bot starts, so changing the ngrok URL just requires restarting the bot.

---

## systemd Services

```ini
# /etc/systemd/system/tdbridge.service
[Unit]
Description=TDbridge Telegram-Discord bridge (prod)
After=network.target

[Service]
TimeoutStopSec=60
ExecStart=/home/graeme/TDbridge/venv/bin/python -u bot.py --env prod
WorkingDirectory=/home/graeme/TDbridge
User=graeme
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/tdbridge-test.service
[Unit]
Description=TDbridge Telegram-Discord bridge (test)
After=network.target

[Service]
TimeoutStopSec=60
ExecStart=/home/graeme/TDbridge/venv/bin/python -u bot.py --env test
WorkingDirectory=/home/graeme/TDbridge
User=graeme
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

---

## Import Hierarchy

**Level 1 (no TDbridge dependencies):**
- `config.py`

**Level 2:**
- `google_sheets_connection.py` → config
- `db.py` → config

**Level 3:**
- `table_manager.py` → config, google_sheets_connection

**Level 4:**
- `sheets_manager.py` → config, table_manager

**Level 5:**
- `bot.py` → config, db, sheets_manager

---

## Guiding Principles

These principles are inherited from HCF and extended for TDbridge.

### General-Purpose Design

Use existing engines before building new ones.  `table_manager.py` handles all Google Sheets I/O.  `db.py` handles all SQLite I/O.  `sheets_manager.py` is the single source of truth for mapping data.  `bot.py` handles routing logic only.

### Non-Blocking Async

The Discord heartbeat must never be blocked.  All Google Sheets API calls run in `loop.run_in_executor(None, ...)`.  All SQLite calls also run in executor (SQLite is fast but can block on disk I/O).

### Aware Datetimes Always

Every datetime variable is timezone-aware.  Unaware datetimes are created only transiently during Google Sheets serial number conversion and are immediately discarded.

### Configuration: TEST_/PROD_ Prefix Pattern

Inherited from HCF.  All environment-specific values in `.env` use the `TEST_` or `PROD_` prefix.  `config.py` selects the active prefix from `--env`.

### Webhook on Linux, Polling on Windows

On Linux (the server), TDbridge uses Telegram webhook mode.  The bot runs its
own HTTPS server using the Let's Encrypt certificate and registers the public
URL with Telegram at every startup.

On Windows (development), TDbridge automatically switches to Telegram polling
mode.  No certificate, no public URL, and no open port are required on the dev
machine.  The platform is detected automatically via `platform.system()` — no
env var or command-line flag is needed.

This design keeps platform-specific code isolated to a single function
(`_start_telegram_app()` in `bot.py`), so Windows testing is a valid proxy for
Linux production behaviour.

### No Platform-Specific Code

All code runs on both Windows and Ubuntu without modification.  Path handling uses `pathlib.Path`.  No POSIX-only system calls.

---

## Confidential Files (not in GitHub)

| File | Description |
|---|---|
| `.env` | Bot tokens, webhook URLs, spreadsheet names |
| `google_credentials_TDbridge.json` | Google service account key |
| `tdbridge_test.db` | SQLite message store (test) |
| `tdbridge_prod.db` | SQLite message store (prod) |

Both `.env` and `google_credentials_*.json` are listed in `.gitignore`.  Copy manually to each environment — never via git.

---

## Phase Roadmap

### Phase 1A — Proof of Concept (current)
- Bidirectional text, reply, reaction, attachment, edit, deletion bridging
- Single monitored Discord channel, single Telegram group per mapped user
- SQLite message store with reply-chain root tracking
- Google Sheets mapping (D_User / D_Channel / T_Group tables)
- Webhook-only Telegram mode; ngrok for Windows dev

### Phase 1B — Full Rollout
- Multi-attachment messages (currently bridges first attachment only)
- Expand to all active users after PoC validation

### Phase 2
- Multi-channel Discord routing (architecture already supports it via D_ChannelID)
- Squadron Admin Bot (separate Telegram bot proxy for management)
- Admin slash commands for TDbridge configuration

---

## Version History

- **Creation Date:** 2026-05-29
- **Last Modified:** 2026-05-29
