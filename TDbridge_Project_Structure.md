# TDbridge Project Structure

> Copyright (c) 2026 Squadron Trucking. Released under the MIT License. The
> copyright notice and permission notice shall be included in all copies or
> substantial portions. For the full license text, see `LICENSE_TDbridge.md`.

## Overview

TDbridge is a Python service that transparently bridges messages, replies,
reactions, attachments, edits, and deletions between Telegram groups and a
Discord server. One set of participants interacts exclusively on Discord while
another interacts exclusively on Telegram, and both sides see a single
continuous conversation.

**Design model.** Discord messages are funneled into a small number of
monitored channels, with @mentions (users or roles) identifying the target
Telegram group. Telegram messages are dispersed across a large number of
groups, with each group's `T_GroupID` used to look up the corresponding Discord
destination. This many-to-few / few-to-many asymmetry is the defining shape of
the system.

**The Gateway.** Beyond direct bridging, TDbridge now includes a **Gateway** — an
authenticated HTTPS side channel that lets two cooperating systems exchange
Telegram-group messages *without either system needing to observe the other's
Telegram bot messages*. This solves a hard Telegram limitation: a bot never
receives messages authored by another bot. When two organizations each run a
bot in the same group, neither can see the other's messages; the Gateway carries
them directly. A single TDbridge instance can act as a gateway **server** (owning
an endpoint and a queue), a gateway **client** (polling and sending to another
instance), or both at once. The wire protocol is specified in
`TDbridge_Gateway_Protocol.md`; integration guidance for a partner programmer is
in `TDbridge_Gateway_Integration_Guide.md`.

**Status.** In production at Squadron Trucking since 2026-06-03, with ~95+ active
Telegram groups. The full feature set — messages, replies, reply-to-replies,
attachments (single and multi-attachment media groups), reactions, edits, and
deletions — has been validated bidirectionally both natively and through the
Gateway.

---

## Team

**Graeme McRae** — Lead developer and system designer. Makes all major design
decisions. Works with Claude as the primary coder and documenter.

**Maclyn** — Co-developer with equal access. Contributing ideas and feature
guidance.

**Claude (Anthropic)** — Primary coder and documenter.

---

## Development Environment

### Developer Workstations

Windows PCs with Git Bash as the terminal environment. Python development is
done locally in a virtual environment (Python 3.13 via pyenv on the primary
workstation; 3.11+ supported).

```bash
python -m venv venv
source venv/Scripts/activate   # Git Bash on Windows
pip install -r requirements.txt
python bot.py --env test       # dev uses polling mode automatically
```

The test instance can be run as a **gateway client of the production server**,
so the full client-server round trip can be exercised end to end from a laptop
(the test instance impersonates a partner dispatch bot; production acts as the
server).

### Version Control

```
~/TDbridge on Graeme's Windows PC
       up/down git push / pull
   GitHub (git@github.com:GraemeMcRae/TDbridge.git, private)
       up/down git pull
~/TDbridge on Production Server (graeme@hcf.squadrontrucking.com)
```

### Production Server

Same VPS as HCF (`hcf.squadrontrucking.com`, Ubuntu 24.04, Namecheap hosting).
TDbridge runs as two systemd services (`TDbridge.service` and
`TDbridgetest.service`) alongside HCF. stunnel4 handles TLS termination for both
instances and for the gateway endpoint.

---

## Architecture

```
Telegram Groups (one per mapped participant)
        up/down Telegram Bot API  (polling or webhook -- see "Transport" below)
   TDbridge bot.py -- single Python process, single asyncio event loop
        up/down discord.py gateway
Discord Server (monitored channels + webhooks)
        up/down
SQLite message store (TDbridge_test.db / TDbridge_prod.db)
        up/down
Google Sheets (D_User_Sheet / D_Channel_Sheet / T_Group_Sheet)

   -- plus, when the Gateway is in use --

   TDbridge (server role) <-- HTTPS --> TDbridge or partner bot (client role)
        via stunnel-terminated /gateway endpoints
        (send / poll / ack / upload / getfile)
```

TDbridge runs as a **single Python process** with one asyncio event loop shared
by discord.py, the Telegram Bot API integration, and — when enabled — the
gateway server (aiohttp) and any gateway client poll loops. The Telegram side
starts after Discord reports `on_ready`.

### Transport: polling vs. webhook

The Telegram transport is selected automatically but can be overridden:

- **Platform default:** webhook on Linux, polling on Windows/WSL2.
- **`TELEGRAM_USE_POLLING` override** (per-env): `true` forces polling even on
  Linux; `false` forces webhook.

**Production currently runs in polling mode.** A change in the inbound network
path to the VPS (an MTU / black-hole issue affecting larger inbound packets,
suspected upstream, tracked in the VPS Spin-up effort) made webhook delivery
unreliable, so `PROD_TELEGRAM_USE_POLLING=true` is set. This is an
infrastructure workaround, not a code limitation; webhook mode remains fully
supported and is the intended mode once the network path is sound.

One consequence of polling on a fresh connection: the first Telegram media send
after a cold start can incur a slow TLS handshake (tens of seconds). Media sends
therefore use generous timeouts to avoid a timeout-then-retry that would post a
duplicate.

### TLS termination

python-telegram-bot's built-in webhook server does not present the full Let's
Encrypt certificate chain, so **stunnel4** sits in front and terminates TLS
correctly. The same stunnel pattern fronts the gateway endpoint.

```
Telegram -> port 88   -> stunnel -> 127.0.0.1:8088  (test webhook, in webhook mode)
Telegram -> port 8443 -> stunnel -> 127.0.0.1:8444  (prod webhook, in webhook mode)
Gateway  -> public    -> stunnel -> 127.0.0.1:<GATEWAY_LISTEN_PORT>  (gateway server)
```

Full detail — certificate acquisition, renewal automation, and troubleshooting —
is in `TDbridge_TLS.md`. stunnel config: `/etc/stunnel/tdbridge.conf`.

---

## File Structure

```
TDbridge/
├── bot.py                          # Entry point; event handlers, routing, gateway hooks
├── config.py                       # Config singleton; --env; .env; platform detection
├── db.py                           # SQLite message-id store, bot_status, gateway_queue
├── sheets_manager.py               # Sheets cache; composite-key lookups; T_Group buffer
├── dashboard_reporter.py           # Periodic health Status Report logger
│
│   # -- Gateway subsystem --
├── gateway_config.py               # Load/validate gateway defs (telegram_gateways.json)
├── gateway_protocol.py             # Wire format: envelope + payloads; make_* builders
├── gateway_server.py               # aiohttp server: /send /poll /ack /upload /getfile
├── gateway_client.py               # GatewayClient: send/poll/ack/upload/getfile
├── gateway_files.py                # Disk-backed attachment store (two-hop); 24h sweep
├── gateway_retry.py                # Heartbeat-safe retry helper for Discord/Telegram
├── gateway_ratelimit.py            # Per-Telegram-group burst circuit breaker
├── gateway_client_test.py          # Standalone validation harness for GatewayClient
│
│   # -- Shared with HCF --
├── google_sheets_connection.py     # gspread connection (shared pattern)
├── table_manager.py                # Generic Sheets table engine (shared)
│
│   # -- Ops / config / assets --
├── cert_renew.sh                   # Daily certificate health check and renewal
├── stunnel_tdbridge.conf           # Reference copy of the stunnel config
├── telegram_gateways.json          # Gateway definitions (NOT committed)
├── telegram_gateways.example.json  # Template for gateway definitions
├── requirements.txt
├── .env                            # NOT committed; per-environment
├── envexample.txt                  # Template with all parameters and comments
├── google_credentials_TDbridge.json  # NOT committed
├── TDbridge_icon.png               # Bot icon (512x512) for Discord and Telegram
├── .gitignore
│
│   # -- Documentation --
├── TDbridge_Guide.md               # Operator setup and usage guide
├── TDbridge_Gateway_Protocol.md    # Gateway wire specification
├── TDbridge_Gateway_Integration_Guide.md  # Partner-programmer integration guide
├── TDbridge_TLS.md                 # TLS, stunnel, and certificate management
├── LICENSE_TDbridge.md             # MIT license + per-file notice guidance
└── TDbridge_Project_Structure.md   # This file
```

### Shared modules from HCF

`table_manager.py` and `google_sheets_connection.py` are copied from the HCF
project with one change: they import `config` (TDbridge's) rather than
`config_hcf`. When changes are needed, both projects' copies must be updated in
sync.

---

## Module Descriptions

### `config.py`

Parses `--env test|prod` at import time, loads `.env`, and exposes a singleton
`config`. Also defines `localnow()` and date-serial helpers, and performs
platform detection (including WSL2, see "Platform Differences"). The
`_errmsg(suffix, default)` helper converts a leading `!` to a warning emoji so
`.env` files can stay ASCII.

Selected behavioural attributes (see `envexample.txt` for the full list):

| Attribute | .env parameter | Description |
|---|---|---|
| `dc_msg_delete_behavior` | `DC_MSG_DELETE_BEHAVIOR` | `"delete"` / `"ignore"` / custom reply string |
| `delete_fail_errmsg` | `DELETE_FAIL_ERRMSG` | Posted on TG if a deletion fails |
| `unroutable_dtot_errmsg` | `UNROUTABLE_DTOT_ERRMSG` | Posted on DC if a DC->TG message is unroutable |
| `unroutable_ttod_errmsg` | `UNROUTABLE_TTOD_ERRMSG` | Posted on TG if a TG->DC message is fully unroutable |
| `routed_inactive_ttod_errmsg` | `ROUTED_INACTIVE_TTOD_ERRMSG` | Posted on TG if routed via an Inactive row |
| `tg_msg_delete_regex` | `TG_MSG_DELETE_REGEX` | Regex matching the TG "delete" reply command |
| `tg_msg_delete_errmsg` | `TG_MSG_DELETE_ERRMSG` | Posted on TG if the delete command fails |
| `reactions_ttod` | `REACTIONS_TTOD` | `react`/`reply`/`both`/`neither` |
| `reactions_dtot` | `REACTIONS_DTOT` | `react`/`reply`/`both`/`neither` |
| `platform` | (auto) | `"Linux"` or `"Windows"` (WSL2 -> `"Windows"`) |
| `telegram_use_polling` | `TELEGRAM_USE_POLLING` | Force polling/webhook, overriding platform default |
| `own_gateway` | `OWN_GATEWAY` | The gateway this instance **serves** (`""` = none owned) |
| `gateway_listen_port` | `GATEWAY_LISTEN_PORT` | Local port the gateway server binds (behind stunnel) |
| `gateway_debug_endpoints` | `GATEWAY_DEBUG_ENDPOINTS` | Enable extra gateway debug endpoints (default off) |
| `gateway_filesize_mb` | `GATEWAY_FILESIZE` | Max attachment size accepted over the gateway |
| `dc_filesize_mb` | `DC_FILESIZE` | Max attachment size for Discord bot/webhook uploads (see note) |
| `telegram_burstrate` | `TELEGRAM_BURSTRATE` | Per-group burst circuit-breaker threshold (`0` disables) |
| `relay_user_messages` | (derived) | True iff this instance owns a gateway with the relay flag set |

> **`DC_FILESIZE` note.** Discord's widely-quoted 25 MB limit is for *direct
> user* uploads. **Bot/webhook** uploads cap at **10 MB** on a non-boosted server
> (raise to 50/100 MB for boost levels 2/3). `DC_FILESIZE` defaults to 10 MB
> accordingly. Telegram photo sends cap at 10 MB (larger images route to
> `sendDocument`, up to 50 MB). These limits are applied *independently per
> destination* to a single fetched copy — an attachment may reach some
> destinations and not others.

`OWN_GATEWAY=""` means "this instance owns no gateway," **not** "the Telegram
gateway." No instance can own Telegram; a blank gateway value elsewhere (in the
routing tables) denotes native Telegram reach, which is a distinct concept.

### `db.py`

SQLite store. Three tables:

**`message_map`** — every bridged message creates a record:

| Column | Description |
|---|---|
| `tg_group_id` | Telegram chat ID |
| `tg_message_id` | Telegram message ID |
| `dc_channel_id` | Discord channel snowflake |
| `dc_message_id` | Discord message snowflake |
| `root_tg_msg_id` | TG message ID of the reply-chain root |
| `dc_user_id` | Discord user ID for attribution |
| `origin_gateway` | Gateway the message originated through (`""` = native Telegram) |
| `created_at` | Unix timestamp |

The TG index is **unique** on `(tg_group_id, tg_message_id)`. The DC index is
**non-unique** on `(dc_channel_id, dc_message_id)` because a single Discord
message (e.g. a multi-photo media group) maps to multiple Telegram messages, all
pointing back to the one Discord message. The `origin_gateway` column is what
lets a reply/edit/reaction/deletion concerning a gateway-origin message be
relayed back out the correct gateway; a schema migration adds it to older DBs.

**`bot_status`** — key/value persistence for the dashboard reporter
(`tg_last_update`, `bridged_30m`, the three `*_last_unlocked` timestamps).

**`gateway_queue`** — persisted outbound events awaiting delivery to a polling
gateway peer. Persistence means queued events survive a server restart; the
dashboard reports the count of undelivered events.

Public API includes `init_db()`, `store_message()`, `find_by_tg()`,
`find_by_dc()`, `find_all_by_dc()`, `find_root_by_tg()`, `delete_by_tg()`,
`delete_by_dc()`, `purge_older_than()`, the `bot_status` accessors, and the
`gateway_queue` helpers.

### `sheets_manager.py`

Reads the three Sheets tables and maintains an in-memory routing cache, refreshed
every `SHEETS_REFRESH_INTERVAL` seconds. All caches are module-level and guarded
by a `threading.RLock`.

| Cache | Key | Value |
|---|---|---|
| `user_by_discord_id` | D_ID (user or `&role_id`) | D_User row |
| `user_by_tg_group_id` | T_GroupID | Active D_User row (legacy single-key view) |
| `user_by_group_gateway` | **(T_GroupID, T_Gateway)** | Active D_User row -- canonical routing key |
| `channel_by_id` | D_ChannelID | D_Channel row |
| `group_by_id` | T_GroupID | T_Group row |
| `group_by_id_gateway` | **(T_GroupID, T_Gateway)** | T_Group row |

**Composite-key routing.** Every routing-to-Discord lookup uses the pair
**(T_Gateway, T_GroupID)** — there is no T_GroupID-only special case. A blank
`T_Gateway` (native Telegram) is simply one value of the key; a gateway name is
another. The same real `T_GroupID` may appear with a blank gateway *and* with one
or more named gateways as separate rows, resolving independently. The canonical
lookup is `get_user_by_gateway_and_group(gateway, tg_group_id)` (pass `""` for
native sources, the gateway name for gateway sources). Attribution names are read
from the cached row (D_Nickname -> D_DisplayName -> D_UserName) rather than by
live Discord queries.

**T_Group write-behind buffer.** To avoid Sheets API calls per incoming Telegram
message, T_Group updates are queued in memory and flushed every 60 seconds (and
on shutdown). The buffer is persisted to SQLite so pending writes survive a
restart.

**User-lock detection.** After each refresh, `_update_lock_status()` checks
whether any column header starts with "lock" (case-insensitive). If a table is
locked, its `last_unlocked` timestamp is frozen, and the dashboard reports
`locked_min`.

### `dashboard_reporter.py`

Emits one INFO Status Report line periodically (and at startup and shutdown) for
the Manager Dashboard shell script to parse:

```
<ts> - INFO - <bot>: Status Report | env=<env> | status=<OK|WARN|ERROR>
    | dc=<connected|disconnected> | tg_idle_min=<n> | sheets=<ok|error>
    | locked_min=<n> | bridged_30m=<n> | poll_ok=<...> | gw=<...>
    | cb_trips=<n> | summary=<text>
```

`status` is `ERROR` if Discord is disconnected, `WARN` if a table is locked or
the last Sheets op failed, else `OK`. Gateway-related fields (`poll_ok`, `gw`,
`cb_trips`) report gateway health and circuit-breaker trips. Persisted fields
survive restarts so `tg_idle_min` doesn't spike after every restart.

### `bot.py`

Main entry point. Creates `TDbridgeDiscordClient` (a `discord.Client` subclass)
and the Telegram `Application`, wires all event handlers, and — when configured —
starts the gateway server and any gateway client poll loops.

**Startup sequence:** `on_ready` -> `_startup()`: set nickname -> init SQLite ->
restore T_Group buffer and dashboard state -> load Sheets -> start Telegram
(polling or webhook) -> run initial Discord->Sheets refresh -> start the gateway
server (if `OWN_GATEWAY` set) and register its bridge/reaction/deletion hooks ->
build and start gateway client poll loops (for gateways named in active D_User
rows that this instance does not itself own) -> launch background loops -> emit
startup Status Report. Startup is fail-fast: an exception exits the process
rather than running half-initialized.

**Background tasks:** Sheets refresh, daily SQLite purge, 24-hour Discord
re-scan, T_Group flush (60 s), dashboard Status Report, gateway file sweep, and
one poll loop per client gateway.

**Gateway hooks (server role):** the server injects three async hooks defined in
`bot.py` — a bridge hook (inbound `message`/`edited_message`, incl. attachments
and the `edited` flag), a reaction hook, and a deletion hook — each of which
applies the event to real Telegram and bridges it to Discord.

### Gateway subsystem modules

- **`gateway_config.py`** — loads and validates gateway definitions from
  `telegram_gateways.json`; validates the instance's `OWN_GATEWAY`.
- **`gateway_protocol.py`** — the wire format: the envelope plus the message /
  reaction / ids / attachment / user dataclasses, the `make_message` /
  `make_reaction` / `make_deletion` / `make_ack` builders, and the event-type
  constants. Stdlib-only. Note that `from_user` serializes under the JSON key
  `"from"` (Telegram's shape).
- **`gateway_server.py`** — aiohttp server bound to
  `127.0.0.1:GATEWAY_LISTEN_PORT` behind stunnel. Endpoints: `/send`, `/poll`
  (~10 s long-poll), `/ack`, `/upload`, `/getfile`. Dispatches inbound events to
  the injected hooks; manages the persisted outbound queue and `RequireACK`
  redelivery.
- **`gateway_client.py`** — `GatewayClient`: `send_message` (incl. `edited`),
  `send_reaction`, `send_deletion`, `ack`, `upload_file`, `getfile`,
  `download_file`, and the poll loop. Makes only outbound requests (works behind
  NAT).
- **`gateway_files.py`** — disk-backed attachment store for the two-hop transfer
  (`store_upload`, `read_file_by_ref`, `delete_file`, and a sweep that expires
  files older than 24 hours, rewriting any still-queued events that referenced
  them).
- **`gateway_retry.py`** — heartbeat-safe retry wrapper for Discord/Telegram
  calls, so retries never block the asyncio heartbeat.
- **`gateway_ratelimit.py`** — a per-group burst circuit breaker that suppresses
  runaway sends and records a status the dashboard surfaces.
- **`gateway_client_test.py`** — a standalone harness that exercises the client
  against a server, used during development.

---

## Google Sheets Tables

### D_User (D_User_Sheet)

Stores Discord **users** and **roles** (role rows have `D_ID = "&<role_id>"`).
TDbridge never deletes rows; it inserts and updates identity columns and
`D_LastFound`, and never overwrites the user-maintained routing columns.

| Column | Written by | Notes |
|---|---|---|
| D_ID | TDbridge | Numeric for users; `&<role_id>` for roles |
| D_UserName | TDbridge | Username (empty for roles) |
| D_Nickname | TDbridge | Server nickname (role name for roles) |
| D_DisplayName | TDbridge | Display name (empty for roles) |
| D_LastFound | TDbridge | Updated on each 24-hour refresh |
| D_ChannelID | User | Target Discord channel for routing |
| D_ChannelName | User/formula | Informational |
| D_UserStatus | User | `Active` = routed; anything else = not routed |
| **T_Gateway** | User | The gateway used to reach the group (`""` = native Telegram) |
| T_GroupID | User | Target Telegram group for routing |
| T_Title | User/formula | Informational |
| T_LastFound | User/formula | Informational |

**Attribution order:** server nickname -> display name -> username (D_Nickname ->
D_DisplayName -> D_UserName), read from the cache. The **(T_Gateway, T_GroupID)**
pair — not T_GroupID alone — is the routing key, so a user may have multiple rows
for the same group reached by different gateways (or natively).

The table is a **superset** shared by test and production. Old/deleted rows are
never removed; use `D_LastFound` to identify stale ones.

### D_Channel (D_Channel_Sheet)

| Column | Written by | Notes |
|---|---|---|
| D_ChannelID | TDbridge | Discord channel snowflake |
| D_ChannelName | TDbridge | Channel name at last discovery |
| D_LastFound | TDbridge | Updated on each 24-hour refresh |
| D_ChannelStatus | User | `Active` = monitored; anything else = ignored |

The **first Active channel** in table order is the fallback destination for
Telegram messages that match no specific user or role.

### T_Group (T_Group_Sheet)

| Column | Written by | Notes |
|---|---|---|
| T_GroupID | TDbridge | Telegram chat ID (negative for groups) |
| T_Title | TDbridge | Group title at last message |
| T_Type | TDbridge | `group` or `supergroup` |
| **T_Gateway** | User | Gateway through which the group is reached (`""` = native) |
| T_LastFound | TDbridge | Updated on each incoming message (buffered) |
| T_Status | User | `Active` = monitored; anything else = not routed |

The unique key is **(T_GroupID, T_Gateway)**. The same real `T_GroupID` may
appear with different gateways (or none), reflecting that multiple bots may be
members of one Telegram group. On a duplicate (T_GroupID, T_Gateway), the first
row wins. A message arriving over gateway `G` for a group requires a matching
**Active** T_Group row for (T_GroupID, `G`), or it is treated as unroutable.

---

## Message Routing Rules

### Telegram to Discord

| Case | Condition | Action |
|---|---|---|
| 1 | Active D_User row for (T_Gateway, T_GroupID) | Route to that row's D_ChannelID; tag `<@D_ID>` |
| 2 | Inactive D_User row matching the group | Route there; tag; post `ROUTED_INACTIVE_TTOD_ERRMSG` |
| 3 | No matching D_User row | Route to first Active D_Channel; pseudo-tag with group title; post `UNROUTABLE_TTOD_ERRMSG` |

Replies, edits, reactions, and deletions use the stored TG<->DC mapping in
SQLite. For a media group, all member TG ids map to the one Discord message, so a
deletion removes the Discord message only when its **last** Telegram sibling is
gone (siblings disassociate one at a time).

Telegram UI deletions are invisible to bots; a Telegram user deletes a bridged
message by **replying with the delete command** (`TG_MSG_DELETE_REGEX`), which
TDbridge treats as a deletion and bridges/relays accordingly.

### Discord to Telegram

| Rule | Condition | Action |
|---|---|---|
| 1 | Reply to a previously bridged message | Route to that message's TG group (hop 1); if gateway-origin, also relay out that gateway (hop 2) |
| 2 | First `<@user>`/`<@&role>` (L->R) that is Active, has T_GroupID, and whose D_ChannelID matches the incoming channel | Route to that user/role's group |
| 3 | Sender's user ID matches an Active row under the same conditions | Route to the sender's group |
| 4 | Sender's roles, in D_User table order, matching an Active row | Route via that role's group |
| -- | None of the above | Post `UNROUTABLE_DTOT_ERRMSG` (if configured); log |

**Two-hop routing.** A reply to a gateway-origin message has two *independent*
hops: hop 1 is governed by *this* message's own D_User row (native delivery to
Telegram for a blank-gateway row); hop 2 is the outbound relay out the
`origin_gateway`, performed only by the instance that **serves** that gateway.
The two must not be conflated — hop 1's routing is never taken from the inherited
origin gateway.

**Loop prevention.** A message whose Telegram sender is this instance's own bot
is not bridged (stateless identity check). Additionally, a deletion the bot
performs *because it received one* is marked so it is not re-relayed (echo
suppression), and an inbound message whose id is already mapped is skipped
(dedupe), preventing the client from re-bridging its own echoes.

---

## Configuration (.env)

All parameters use a `TEST_`/`PROD_` prefix; `--env` selects the active prefix.
See `envexample.txt` for the canonical, commented reference. Parameter groups:

- **Identity:** `DISCORD_BOT_TOKEN`, `TELEGRAM_BOT_TOKEN`, `GOOGLE_SPREADSHEET_NAME`
- **Transport:** `TELEGRAM_WEBHOOK_URL`, `TELEGRAM_WEBHOOK_PORT`,
  `TELEGRAM_WEBHOOK_SECRET`, `TELEGRAM_USE_POLLING`
- **Gateway:** `OWN_GATEWAY`, `GATEWAY_LISTEN_PORT`, `GATEWAY_DEBUG_ENDPOINTS`,
  `GATEWAY_FILESIZE`, `TELEGRAM_GATEWAYS` (path to the JSON)
- **Attachments/limits:** `DC_FILESIZE`, `TELEGRAM_BURSTRATE`
- **Routing ERRMSGs:** `UNROUTABLE_DTOT_ERRMSG`, `UNROUTABLE_TTOD_ERRMSG`,
  `ROUTED_INACTIVE_TTOD_ERRMSG`
- **Deletion:** `DC_MSG_DELETE_BEHAVIOR`, `DELETE_FAIL_ERRMSG`,
  `TG_MSG_DELETE_REGEX`, `TG_MSG_DELETE_ERRMSG`
- **Reactions:** `REACTIONS_TTOD`, `REACTIONS_DTOT`
- **Infrastructure:** `SQLITE_DB_FILE`, `LOGFILENAME`, `SHEETS_REFRESH_INTERVAL`
- **Shared:** `LOCAL_TIMEZONE`, `GOOGLE_CREDENTIALS_FILE`, `TLS_CERT_FILE`,
  `TLS_KEY_FILE`

Gateway definitions live in a separate JSON file (default
`telegram_gateways.json`; template `telegram_gateways.example.json`). Each entry
carries the gateway `url`, `secret`, and the server-side behavior flags `echo`,
`require_ack`, and `relay_user_messages` (see `TDbridge_Gateway_Protocol.md` sec.
9, sec. 10).

---

## Platform Differences

| Aspect | Linux (server) | Windows or WSL2 (dev) |
|---|---|---|
| Telegram transport | Webhook by default; polling if forced (prod currently forces polling) | Polling |
| Shutdown signal | SIGTERM/SIGINT via asyncio handlers | KeyboardInterrupt |
| Webhook port binding | Bot binds `127.0.0.1:8088`/`8444` (webhook mode only) | Not applicable |

Platform is detected automatically — no env var needed for detection itself.

**WSL2 detection.** WSL2 reports `"Linux"` from `platform.system()` but is a
developer environment behind NAT, where webhook mode cannot work. TDbridge reads
`/proc/version` for `"microsoft"`/`"wsl"` and, if found, overrides
`config.platform` to `"Windows"` so polling is used. `TELEGRAM_USE_POLLING`
overrides the platform default in either direction.

---

## systemd Services

Both services declare `After=` and `Requires=stunnel4.service` so stunnel is up
before the bot starts. Restarting stunnel cascades a restart of the bot
services, which is the standard way to apply certificate renewals.

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
config.py                           (Level 1 -- no TDbridge deps)
  |
google_sheets_connection.py         (Level 2)
db.py                               (Level 2)
gateway_protocol.py                 (Level 2 -- stdlib only)
gateway_config.py                   (Level 2)
gateway_files.py                    (Level 2)
gateway_ratelimit.py                (Level 2)
gateway_retry.py                    (Level 2)
  |
table_manager.py                    (Level 3)
  |
sheets_manager.py                   (Level 4)
dashboard_reporter.py               (Level 4 -- imports db lazily)
gateway_server.py                   (Level 4 -- uses protocol, files, db queue)
gateway_client.py                   (Level 4 -- uses protocol)
  |
bot.py                              (Level 5 -- wires everything; defines gateway hooks)
```

`dashboard_reporter.py` imports `db` lazily (inside functions) to avoid a
circular import with `sheets_manager.py`.

---

## Guiding Principles

**General-purpose design.** TDbridge is a general Telegram<->Discord bridge that
happens to suit dispatch well, not a dispatch-specific tool. The routing tables
support users and roles, native and gateway reach, and the many-to-few /
few-to-many mapping in either direction.

**Composite-key discipline.** (T_Gateway, T_GroupID) is the universal routing
key, everywhere, with no special cases. A blank gateway is one value of the key.

**Two-hop supervision by the gateway owner only.** The second (relay) hop of a
gateway-origin message is performed solely by the instance that serves that
gateway; the native delivery hop is governed independently by the message's own
row.

**Non-blocking async.** The Discord heartbeat must never block. All Sheets and
SQLite work runs via `loop.run_in_executor`; gateway/Telegram retries use the
heartbeat-safe retry helper.

**Fetch-once, limit-per-consumer.** An attachment's bytes are fetched once; each
destination (Discord, Telegram, gateway) applies its own size limit to that one
copy. No bridge outcome gates the gateway relay — the paths degrade
independently.

**Write-behind buffering.** T_Group upserts are batched and flushed every 60 s,
reducing Sheets calls from O(messages) to O(active groups).

**Superset tables, never auto-deleted.** Shared test/prod tables; stale rows are
identified by `D_LastFound` and managed by hand, preventing accidental data loss.

**Aware datetimes always.** Every datetime is timezone-aware except transiently
during Sheets serial conversion.

**Structured log lines.** Bridge events, routing decisions, and errors are logged
`key=value | key=value` for grep and for the Manager Dashboard script.

**Don't over-engineer loop protection.** The identity check, echo-suppression,
dedupe, and the burst circuit breaker are the backstops; benign transients (e.g.
occasional poll timeouts, a harmless deletion no-op) are logged, not fought.

---

## Confidential Files (not in GitHub)

| File | Description |
|---|---|
| `.env` | Bot tokens, webhook URLs, spreadsheet names, gateway secrets |
| `google_credentials_TDbridge.json` | Google service-account key |
| `telegram_gateways.json` | Gateway URLs and shared secrets |
| `TDbridge_test.db` / `TDbridge_prod.db` | SQLite message stores |

---

## Version History

- **Creation:** 2026-05-23
- **Production deployment:** 2026-06-02/03 (initial bridging; ~51 groups)
- **Gateway subsystem:** designed and validated 2026-06 -> 2026-07 (server +
  client roles, attachments, reactions, edits, deletions, media groups,
  composite-key routing, two-hop relay)
- **Documentation facelift:** 2026-07-03 (this revision) — reflects the gateway
  subsystem, composite-key routing, current transport reality, and lessons
  learned; merged from the two prior structure drafts
- **Scale:** ~95+ active Telegram groups in production
