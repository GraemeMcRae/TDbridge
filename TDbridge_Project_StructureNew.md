# TDbridge Project Structure

## Overview

TDbridge is a Python-based bot service that transparently bridges messages,
replies, reactions, attachments, and edits between Telegram groups and a
Discord server.  It allows one set of users to interact exclusively on Discord
while another party interacts exclusively on Telegram.  Both sides appear to
be in a single conversation.

**Design model:** Discord messages are funneled into a small number of monitored
channels, with @mentions (users or roles) used to identify the target Telegram
group.  Telegram messages are dispersed across a large number of groups, with
the group's T_GroupID used to look up the corresponding Discord user or role.
TDbridge makes both platforms appear as one conversation.

**Status:** In production at Squadron Trucking as of 2026-06-03, with 95 active Telegram groups.

---

## Team

**Graeme McRae** — Lead developer and system designer.  Makes all major design
decisions.  Works with Claude as the primary coder and documenter.

**Maclyn** — Co-developer with equal access.  Contributing ideas and feature guidance.

**Claude (Anthropic)** — Primary coder and documenter.

---

## Development Environment

### Developer Workstations

Windows PCs with Git Bash as the terminal environment.  Python development is
done locally in a virtual environment.

```
python -m venv venv
source venv/Scripts/activate   # Git Bash on Windows
pip install -r requirements.txt
python bot.py --env test       # Windows uses polling mode automatically
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

Same VPS as HCF (`hcf.squadrontrucking.com`, Ubuntu 24.04, Namecheap hosting).
TDbridge runs as two separate systemd services (`TDbridge.service` and
`TDbridgetest.service`) alongside HCF.  stunnel4 handles TLS termination for
both instances.

---

## Architecture

```
Telegram Groups (one per mapped user/role)
        ↕ HTTPS → stunnel → HTTP → python-telegram-bot webhook
   TDbridge bot.py — single Python process, single asyncio event loop
        ↕ discord.py gateway
Discord Server (monitored channels + webhooks)
        ↕
SQLite message store (TDbridge_test.db / TDbridge_prod.db)
        ↕
Google Sheets (D_User_Sheet / D_Channel_Sheet / T_Group_Sheet)
```

TDbridge runs as a **single Python process** with one asyncio event loop shared
by both discord.py (gateway) and python-telegram-bot (webhook server).
The Telegram webhook server starts after Discord reports `on_ready`.

### TLS termination

python-telegram-bot v22's `start_webhook()` does not correctly present the full
Let's Encrypt certificate chain.  stunnel4 sits in front of the bot and handles
TLS correctly:

```
Telegram → port 88   → stunnel → 127.0.0.1:8088  (test bot)
Telegram → port 8443 → stunnel → 127.0.0.1:8444  (prod bot)
```

stunnel config: `/etc/stunnel/tdbridge.conf`

---

## File Structure

```
TDbridge/
├── bot.py                          # Entry point; all event handlers and routing
├── config.py                       # Config singleton; parses --env; loads .env
├── db.py                           # SQLite message ID store and bot_status table
├── sheets_manager.py               # Google Sheets cache; upsert helpers; T_Group buffer
├── dashboard_reporter.py           # 30-minute health Status Report logger
├── google_sheets_connection.py     # gspread connection (shared with HCF pattern)
├── table_manager.py                # Generic Sheets table engine (shared with HCF)
├── requirements.txt
├── .env                            # NOT committed; copy manually to each env
├── envexample.txt                  # Template with all parameter names and comments
├── google_credentials_TDbridge.json  # NOT committed
├── .gitignore
├── TDbridge_Guide.md               # User-facing setup and usage guide
└── TDbridge_Project_Structure.md   # This file
```

### Shared modules from HCF

`table_manager.py` and `google_sheets_connection.py` are copied from the HCF
project with one change: they import `config` (TDbridge's `config.py`) rather
than `config_hcf`.  When changes are needed, both projects' copies must be
updated in sync.

---

## Module Descriptions

### `config.py`

Parses `--env test|prod` at import time, loads `.env`, and exposes a singleton
`config` object.  Also defines `localnow()` and date serial conversion helpers.

The `_errmsg(suffix, default)` helper (defined inside `__init__`) reads an env
var and converts a leading `!` to `⚠️`, allowing `.env` files to stay ASCII.

Key behavioural attributes:

| Attribute | .env parameter | Description |
|---|---|---|
| `dc_msg_delete_behavior` | `DC_MSG_DELETE_BEHAVIOR` | `"delete"` / `"ignore"` / custom string |
| `delete_fail_errmsg` | `DELETE_FAIL_ERRMSG` | Posted on TG if deletion fails |
| `unroutable_dtot_errmsg` | `UNROUTABLE_DTOT_ERRMSG` | Posted on DC if DC→TG unroutable |
| `unroutable_ttod_errmsg` | `UNROUTABLE_TTOD_ERRMSG` | Posted on TG if TG→DC fully unroutable |
| `routed_inactive_ttod_errmsg` | `ROUTED_INACTIVE_TTOD_ERRMSG` | Posted on TG if routed via Inactive row |
| `tg_msg_delete_regex` | `TG_MSG_DELETE_REGEX` | Regex for TG delete command replies |
| `tg_msg_delete_errmsg` | `TG_MSG_DELETE_ERRMSG` | Posted on TG if delete command fails |
| `reactions_ttod` | `REACTIONS_TTOD` | `react`/`reply`/`both`/`neither` |
| `reactions_dtot` | `REACTIONS_DTOT` | `react`/`reply`/`both`/`neither` |
| `dc_msg_delete_behavior` | `DC_MSG_DELETE_BEHAVIOR` | What to do on Discord message deletion |

### `db.py`

SQLite message store.  Two tables:

**`message_map`** — Every bridged message creates a record:

| Column | Description |
|---|---|
| `tg_group_id` | Telegram chat ID |
| `tg_message_id` | Telegram message ID |
| `dc_channel_id` | Discord channel snowflake |
| `dc_message_id` | Discord message snowflake |
| `root_tg_msg_id` | TG message ID of the reply-chain root |
| `dc_user_id` | Discord user ID for attribution |
| `created_at` | Unix timestamp |

The TG index is unique on `(tg_group_id, tg_message_id)`.  The DC index is
**non-unique** on `(dc_channel_id, dc_message_id)` because a single Discord
message (e.g. a 10-photo attachment) can produce multiple Telegram messages,
all mapping back to the same Discord message ID.

**`bot_status`** — Key-value persistence for the dashboard reporter:
`tg_last_update`, `bridged_30m`, and the three `*_last_unlocked` timestamps.

Public API: `init_db()`, `store_message()`, `find_by_tg()`, `find_by_dc()`,
`find_all_by_dc()`, `find_root_by_tg()`, `delete_by_tg()`, `delete_by_dc()`,
`purge_older_than()`, `set_status_value()`, `get_status_value()`.

### `sheets_manager.py`

Reads the three Google Sheets tables and maintains an in-memory routing cache.
Refreshes every `SHEETS_REFRESH_INTERVAL` seconds.

**Cache dictionaries (module-level, protected by `threading.RLock`):**

| Name | Key | Value |
|---|---|---|
| `user_by_discord_id` | D_ID (user or `&role_id`) | D_User row dict |
| `user_by_tg_group_id` | T_GroupID | D_User row dict (Active rows only) |
| `channel_by_id` | D_ChannelID | D_Channel row dict |
| `group_by_id` | T_GroupID | T_Group row dict |

`user_by_discord_id` contains both user rows and role rows.  Role rows have
`D_ID = "&<role_id>"`.

**T_Group write-behind buffer:**  To avoid 3 Sheets API calls per incoming
Telegram message, `upsert_t_group_buffered()` queues updates in memory.
A background task flushes the buffer to Sheets every 60 seconds (or on
shutdown).  The buffer is persisted to SQLite so pending writes survive
a restart.

**Public lookup functions:**
- `get_user_by_discord_id(id)` — returns the D_User row for a user or role ID
- `get_user_by_tg_group(tg_group_id)` — returns the Active D_User row for a TG group
- `get_user_by_tg_group_inactive(tg_group_id)` — returns the first Inactive D_User row for a TG group
- `get_all_user_rows_in_table_order()` — all D_User rows in sheet row order (used for role-based DC→TG routing)
- `get_channel(channel_id)` — returns the D_Channel row
- `get_active_channels()` — returns all Active D_Channel rows in table order
- `get_tg_group(tg_group_id)` — returns the T_Group row

**User-lock detection:** After every `refresh_table()` call, `_update_lock_status()`
checks whether any column header starts with "lock" (case-insensitive).  If
locked, the `last_unlocked` timestamp for that table is frozen.  The dashboard
reporter computes `locked_min = (last_checked - last_unlocked) / 60`.

### `dashboard_reporter.py`

Emits one INFO-level log line every 30 minutes (and at startup and shutdown)
for the Manager Dashboard script to parse.

Log line format:
```
<timestamp> - INFO - <bot_name>: Status Report | env=<env> | status=<OK|WARN|ERROR>
    | dc=<connected|disconnected> | tg_idle_min=<n> | sheets=<ok|error>
    | locked_min=<n> | bridged_30m=<n> | summary=<text>
```

Status derivation: `ERROR` if Discord disconnected; `WARN` if any table locked
or last Sheets op failed; `OK` otherwise.  `tg_idle_min` and `locked_min` are
raw numbers — the dashboard script applies thresholds.

Persisted fields (in `bot_status` SQLite table): `tg_last_update`, `bridged_30m`,
and the three `*_last_unlocked` lock timestamps.  These survive a restart so
`tg_idle_min` doesn't show 9999 after every restart.

### `bot.py`

Main entry point.  Creates `TDbridgeDiscordClient` (subclass of `discord.Client`)
and the python-telegram-bot `Application`.

**Startup sequence:**
1. `asyncio.run(runner())` starts the Discord client
2. Discord fires `on_ready` → `_startup()` runs
3. `_startup()`: `dc_connected = True` → nickname set → SQLite init → buffer restore →
   dashboard restore → Sheets load → Telegram app start → Discord refresh →
   background tasks launched → dashboard `emit_startup()`

**Background tasks:**
- `_sheets_refresh_loop()` — refreshes Sheets cache every `SHEETS_REFRESH_INTERVAL` seconds
- `_db_purge_loop()` — purges SQLite records older than 30 days, once per day
- `_discord_refresh_loop()` — re-scans guild members, roles, and channels every 24 hours
- `t_group_flush_loop()` (in sheets_manager) — flushes the T_Group write buffer every 60 seconds
- `_dashboard_reporter.run_loop()` — emits Status Report every 30 minutes

---

## Google Sheets Tables

### D_User (D_User_Sheet)

Stores both Discord **users** and Discord **roles**.  TDbridge never deletes
rows — it only inserts new rows and updates `D_LastFound` and identity columns.
User-maintained columns (`D_ChannelID`, `D_UserStatus`, `T_GroupID`, etc.) are
never overwritten by TDbridge on update.

| Column | Written by | Notes |
|---|---|---|
| D_ID | TDbridge | Numeric for users; `&<role_id>` for roles |
| D_UserName | TDbridge | Empty for roles |
| D_Nickname | TDbridge | Server nickname for users; role name for roles |
| D_DisplayName | TDbridge | Empty for roles |
| D_LastFound | TDbridge | Updated on each 24-hour refresh |
| D_ChannelID | User | Target Discord channel for routing |
| D_ChannelName | User/formula | Informational |
| D_UserStatus | User | `Active` = routed; anything else = not routed |
| T_GroupID | User | Source Telegram group for routing |
| T_Title | User/formula | Informational |
| T_LastFound | User/formula | Informational |

The table is a **superset** shared by test and production environments.
Old/deleted users and roles are never removed — use `D_LastFound` to identify
rows that are no longer seen in Discord.

### D_Channel (D_Channel_Sheet)

| Column | Written by | Notes |
|---|---|---|
| D_ChannelID | TDbridge | Discord channel snowflake |
| D_ChannelName | TDbridge | Channel name at time of last discovery |
| D_LastFound | TDbridge | Updated on each 24-hour refresh |
| D_ChannelStatus | User | `Active` = monitored; anything else = ignored |

The **first Active channel** in table order is used as the fallback destination
for Telegram messages that cannot be matched to any specific user or role.

### T_Group (T_Group_Sheet)

| Column | Written by | Notes |
|---|---|---|
| T_GroupID | TDbridge | Telegram chat ID (negative for groups) |
| T_Title | TDbridge | Group title at time of last message |
| T_Type | TDbridge | `group` or `supergroup` |
| T_LastFound | TDbridge | Updated on each incoming message (buffered) |
| T_Status | User | `Active` = monitored; anything else = not routed to Discord |

---

## Message Routing Rules

### Telegram → Discord

| Case | Condition | Action |
|---|---|---|
| 1 | Active D_User row matching T_GroupID | Route to that row's D_ChannelID; tag `<@D_ID>` |
| 2 | Inactive D_User row matching T_GroupID | Route to that row's D_ChannelID; tag `<@D_ID>`; post `ROUTED_INACTIVE_TTOD_ERRMSG` |
| 3 | No D_User row at all | Route to first Active D_Channel; pseudo-tag with group title; post `UNROUTABLE_TTOD_ERRMSG` |

Reply chains, edits, and deletions all use the stored TG↔DC message ID mapping
in SQLite to route to the correct Discord message.

### Discord → Telegram

| Rule | Condition | Action |
|---|---|---|
| 1 | Reply to a previously bridged message | Route to that message's TG group |
| 2 | First `<@user>` or `<@&role>` in message text (L→R) that is Active + has T_GroupID + D_ChannelID matches incoming channel | Route to that user/role's TG group |
| 3 | Sender's user ID matches Active D_User row with same conditions | Route to sender's TG group |
| 4 | Sender's roles, searched in D_User table row order, matching Active row with same conditions | Route via that role's TG group |
| — | None of the above | Post `UNROUTABLE_DTOT_ERRMSG` in Discord (if configured); log WARNING or INFO |

---

## Configuration (.env)

All parameters use `TEST_`/`PROD_` prefix.  `--env` selects the active prefix.
See `envexample.txt` for the canonical reference with inline comments.
See `TDbridge_Guide.md` § "Step 5" for detailed descriptions.

Key parameter groups:
- **Identity:** `DISCORD_BOT_TOKEN`, `TELEGRAM_BOT_TOKEN`, `GOOGLE_SPREADSHEET_NAME`
- **Webhook:** `TELEGRAM_WEBHOOK_URL`, `TELEGRAM_WEBHOOK_PORT`, `TELEGRAM_WEBHOOK_SECRET`
- **Routing ERRMSGs:** `UNROUTABLE_DTOT_ERRMSG`, `UNROUTABLE_TTOD_ERRMSG`, `ROUTED_INACTIVE_TTOD_ERRMSG`
- **Deletion:** `DC_MSG_DELETE_BEHAVIOR`, `DELETE_FAIL_ERRMSG`, `TG_MSG_DELETE_REGEX`, `TG_MSG_DELETE_ERRMSG`
- **Reactions:** `REACTIONS_TTOD`, `REACTIONS_DTOT`
- **Infrastructure:** `SQLITE_DB_FILE`, `LOGFILENAME`, `SHEETS_REFRESH_INTERVAL`
- **Shared:** `LOCAL_TIMEZONE`, `GOOGLE_CREDENTIALS_FILE`, `TLS_CERT_FILE`, `TLS_KEY_FILE`

---

## Platform Differences

| Aspect | Linux (server) | Windows (dev) |
|---|---|---|
| Telegram transport | Webhook (stunnel → bot on localhost) | Polling (getUpdates every 10s) |
| Shutdown signal | SIGTERM / SIGINT via asyncio signal handlers | KeyboardInterrupt |
| Port binding | Bot binds to 127.0.0.1:8088 / 8444 | Not applicable |

Platform is detected automatically via `platform.system()` — no env var needed.

---

## systemd Services

Both services have `After=stunnel4.service` and `Requires=stunnel4.service`
so stunnel is guaranteed running before the bot starts.

```ini
# /etc/systemd/system/TDbridgetest.service (added by Graeme)
[Unit]
Description=TDbridge Telegram-Discord bridge (test) (added by Graeme)
After=network.target stunnel4.service
Requires=stunnel4.service

[Service]
TimeoutStopSec=60
ExecStart=/home/graeme/TDbridge/venv/bin/python -u bot.py --env test
WorkingDirectory=/home/graeme/TDbridge
User=graeme
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/TDbridge.service (added by Graeme)
[Unit]
Description=TDbridge Telegram-Discord bridge (prod) (added by Graeme)
After=network.target stunnel4.service
Requires=stunnel4.service

[Service]
TimeoutStopSec=60
ExecStart=/home/graeme/TDbridge/venv/bin/python -u bot.py --env prod
WorkingDirectory=/home/graeme/TDbridge
User=graeme
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

---

## Import Hierarchy

```
config.py                           (Level 1 — no TDbridge deps)
  ↓
google_sheets_connection.py         (Level 2)
db.py                               (Level 2)
  ↓
table_manager.py                    (Level 3)
  ↓
sheets_manager.py                   (Level 4)
dashboard_reporter.py               (Level 4 — imports db lazily)
  ↓
bot.py                              (Level 5)
```

`dashboard_reporter.py` imports `db` lazily (inside functions) to avoid
circular imports with `sheets_manager.py`.

---

## Guiding Principles

**General-purpose design:** TDbridge is not a driver-dispatcher tool — it is a
general-purpose Telegram↔Discord bridge that happens to work well for that
use case.  The D_User table supports both users and roles, enabling flexible
routing scenarios.

**Non-blocking async:** The Discord heartbeat must never be blocked.  All
Google Sheets and SQLite calls run in `loop.run_in_executor(None, ...)`.

**Write-behind buffering:** T_Group upserts are batched in memory and flushed
to Sheets every 60 seconds, reducing API calls from O(messages) to O(active groups).

**Superset table:** D_User_Sheet is shared by test and production.  TDbridge
never deletes rows — old rows are identified by `D_LastFound` and managed
manually.  This prevents accidental data loss from misconfiguration.

**Aware datetimes always:** Every datetime is timezone-aware.  Unaware datetimes
exist only transiently during Google Sheets serial number conversion.

**Structured log lines:** All bridge events, routing decisions, and errors are
logged in `key=value | key=value` format for easy grep and parsing by the
Manager Dashboard shell script.

---

## Confidential Files (not in GitHub)

| File | Description |
|---|---|
| `.env` | Bot tokens, webhook URLs, spreadsheet names, secrets |
| `google_credentials_TDbridge.json` | Google service account key |
| `TDbridge_test.db` | SQLite message store (test) |
| `TDbridge_prod.db` | SQLite message store (prod) |

---

## Version History

- **Creation:** 2026-05-23
- **Production deployment:** 2026-06-02 (51+ active Telegram groups)
- **Last updated:** 2026-06-03
