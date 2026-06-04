# TDbridge Guide

## What is TDbridge?

TDbridge is a message bridge between Telegram groups and a Discord server.
People on Discord send messages in a small number of monitored channels,
using @mentions to indicate who on the Telegram side should receive them.
People on Telegram send messages in their own group, and TDbridge delivers
those messages to the appropriate Discord channel.

Both sides see a continuous conversation as if they were in the same chat.
Replies, edits, reactions, and attachments are all bridged in both directions.

---

## Using TDbridge at your organization

### Quick overview

Here is what another organization needs to do to set up TDbridge:

1. Get access to the TDbridge GitHub repository (contact Graeme)
2. Create a Discord bot and invite it to your Discord server
3. Create a Telegram bot and invite it into each Telegram group you want to bridge
4. Create a Google Cloud service account and a Google Sheet, and share the sheet with the service account
5. Set up a Linux server with a public hostname and HTTPS certificate
6. Clone the TDbridge repository, create a Python virtual environment, and install dependencies
7. Copy `envexample.txt` to `.env` and fill in your tokens and settings
8. Start TDbridge — it will automatically discover all Discord users, roles, channels, and Telegram groups
9. In the Google Sheet, associate each Discord user or role with its corresponding Telegram group and Discord channel, then set their status to Active

Once the associations are made, TDbridge bridges messages automatically.

You don't need a separate test environment. You can start with a single bot
pair and a single Google Sheet, test with a few Telegram groups, and gradually
bring more groups online. A second bot pair (test vs. production) is only
worthwhile if you want a persistent test environment alongside production.

### Getting help

Graeme is happy to get on a conference call with your programmer for an initial
chat, and then work with you via Discord, Telegram, WhatsApp, or plain old
texting to answer questions and offer troubleshooting guidance.

That said, the best first resource for technical questions is an AI assistant.
Free options like ChatGPT or Google AI can answer most Linux and Python
questions. If you need the AI to read and interpret these setup instructions
for your specific situation, a paid AI service (~$20–30/month) is well worth
it — an AI assistant who can read documents and reason about your exact setup
is genuinely the most efficient way to get a complex system like this running
smoothly. That's not Graeme ducking responsibility for what he's sharing —
it's honest advice about what works.

### What you need to know

You or your programmer should either be comfortable with Linux and Git, or be
willing to use an AI assistant to work out the exact commands for your
situation. TDbridge runs on a Linux server, is configured via text files, and
is deployed using standard Linux tools (systemd, git, pip). None of it is
exotic, but it does assume basic familiarity with a terminal.

---

## Prerequisites

- A Discord server where you have Administrator permissions
- A Telegram account
- A Google account (for Google Sheets)
- A Linux server reachable at a public hostname (for production)
- Python 3.11+ installed on your development machine and server
- Git installed on both machines

### Project directory

Create a project directory under your home directory on both your development
machine and your server. This directory will contain the TDbridge code, your
`.env` configuration, log files, SQLite database, and Google credentials file.
All configuration files and logs live here — nothing is scattered elsewhere.

```bash
mkdir ~/TDbridge
cd ~/TDbridge
```

---

## Step 1 — Get access to the repository

Contact Graeme and provide your GitHub username. He will give you read-only
access to the private `GraemeMcRae/TDbridge` repository using a personal
access token.

### Clone for the first time

```bash
git clone https://<your-token-here>@github.com/GraemeMcRae/TDbridge.git ~/TDbridge
```

### Update when Graeme releases a new version

Graeme will send you a new token along with the update notice.

```bash
cd ~/TDbridge
git remote set-url origin https://<NEW-TOKEN>@github.com/GraemeMcRae/TDbridge.git
git pull origin main
```

If `git pull` reports conflicts with local changes (e.g. you edited a file):

```bash
git stash          # temporarily set aside your local changes
git pull origin main
git stash pop      # re-apply your local changes on top of the update
```

The repository includes `bot.py`, `config.py`, `db.py`, `sheets_manager.py`,
`dashboard_reporter.py`, `google_sheets_connection.py`, `table_manager.py`,
`requirements.txt`, `envexample.txt`, `TDbridge_icon.png`, and this Guide.

---

## Step 2 — Create the Discord bot(s)

You need at least one bot. Create a second one only if you want a persistent
test environment alongside production.

1. Go to https://discord.com/developers/applications and click **New Application**
2. Name it (e.g. `TDbridge`) and click **Create**
3. **Optional but recommended:** Upload a bot icon on the General Information page.
   Use `TDbridge_icon.png` from the repository, or create your own 512×512 image.
   A unique icon makes the bot easy to spot among all the letter-circles in Discord.
4. Go to **Bot** in the left panel
5. Click **Reset Token** and copy the token — this is your `PROD_DISCORD_BOT_TOKEN`
6. Under **Privileged Gateway Intents**, enable:
   - **Server Members Intent** (required to fetch all guild members and roles)
   - **Message Content Intent** (required to read message text)
7. Click **Save Changes**
8. Go to **OAuth2 → URL Generator**
9. Under Scopes, check `bot`
10. Under Bot Permissions, check:
    - Read Messages / View Channels
    - Send Messages
    - Embed Links
    - Attach Files
    - Read Message History
    - Add Reactions
    - Use External Emojis
    - Manage Messages
    - Pin Messages
    - Manage Webhooks
    - Change Nickname
    - Use Application Commands
11. Copy the generated URL, paste it in your browser, and invite the bot to your server

Note the **Application ID** and **Public Key** from the **General Information**
page — these go in `.env` as `PROD_DISCORD_BOT_APPLICATION_ID` etc.

If you discover a missing permission later, go to **Server Settings → Roles**,
click the pencil icon next to the bot's role, go to **Permissions**, enable the
missing permission, and click **Save Changes**.

Repeat for a test bot if desired.

---

## Step 3 — Create the Telegram bot(s)

1. Open Telegram and start a chat with **@BotFather**
2. Send `/newbot` and follow the prompts to name your bot
3. BotFather will give you a bot token — this is your `PROD_TELEGRAM_BOT_TOKEN`
4. Note the bot username (e.g. `TDbridgeProdBot`) — this is `PROD_TELEGRAM_BOT_USERNAME`
5. **Optional but recommended:** Set a profile photo for the bot.
   Send `/setuserpic` to BotFather and upload `TDbridge_icon.png` or your own image.
   A recognizable icon helps users identify the bot in their group member lists.

**Required Telegram bot settings:**

```
/setprivacy  →  select your bot  →  Disable
```

Privacy mode must be **disabled** so the bot can read all messages in groups,
not just commands addressed to it. If you already added the bot to groups
before disabling privacy mode, remove it and re-add it.

**Deletion permission:** TDbridge needs to delete messages that other users
sent (when those messages are later deleted on the Discord side). This requires
the bot to have **administrator** status in each group — see Step 8.

Repeat for a test bot if desired.

---

## Step 4 — Create the Google Sheet

You can either make a copy of the TDbridge Config template (ask Graeme for
read-only access, then **File → Make a copy**) or create one from scratch.

### Create a Google Cloud service account

1. Go to https://console.cloud.google.com
2. Create a new project (e.g. `tdbridge`)
3. Go to **APIs & Services → Library** and enable the **Google Sheets API**
4. Go to **APIs & Services → Credentials**
5. Click **Create Credentials → Service Account**, name it (e.g. `tdbridge-service`), click **Done**
6. Click the service account → **Keys** → **Add Key → Create new key → JSON**
7. Download the JSON file and save it as `google_credentials_TDbridge.json` in `~/TDbridge/`
8. Note the service account email (e.g. `tdbridge-service@tdbridge.iam.gserviceaccount.com`)

### Create and share the spreadsheet

1. In Google Drive, create a new spreadsheet — name it whatever you like,
   e.g. `My TDbridge Config`. (You'll put the exact name in `.env`.)
2. Share it with the service account email address (Editor access)
3. Create a second spreadsheet for production if using separate test/prod environments

### Sheet structure

Create three sheets (tabs) with these exact names and column headers.
**Format the ID columns as Plain Text** (Format → Number → Plain text) before
entering any data, to prevent Google Sheets from rounding large Discord
snowflake IDs.

**Sheet: D_User_Sheet**

| D_ID | D_UserName | D_Nickname | D_DisplayName | D_LastFound | D_ChannelID | D_ChannelName | D_UserStatus | T_GroupID | T_Title | T_LastFound | Unlocked |
|---|---|---|---|---|---|---|---|---|---|---|---|

`D_ID`, `D_ChannelID`, `T_GroupID` — format as Plain Text.

**Sheet: D_Channel_Sheet**

| D_ChannelID | D_ChannelName | D_LastFound | D_ChannelStatus | Unlocked |
|---|---|---|---|---|

`D_ChannelID` — format as Plain Text.

**Sheet: T_Group_Sheet**

| T_GroupID | T_Title | T_Type | T_LastFound | T_Status | Unlocked |
|---|---|---|---|---|---|

`T_GroupID` — format as Plain Text.

The `Unlocked` column is a user-managed lock. Rename it to `Locked` (or any
name starting with "lock", case-insensitive) to prevent TDbridge from writing
to that table while you are editing it. Rename it back to `Unlocked` when done.

You may add extra columns anywhere — TDbridge reads and writes only the columns
it knows about and ignores all others.

---

## Step 5 — Set up TDbridge on your server

### Install Python virtual environment

```bash
cd ~/TDbridge
python3 -m venv venv
source venv/bin/activate          # Linux
# source venv/Scripts/activate    # Windows Git Bash
pip install -r requirements.txt
```

### Copy credentials and configuration

```bash
cp envexample.txt .env
# Edit .env and fill in all tokens and secrets (see Step 6)
# google_credentials_TDbridge.json should already be in ~/TDbridge/
```

### Obtain a TLS certificate (Linux server only)

TDbridge uses **stunnel** to terminate TLS, which requires a valid HTTPS
certificate. Use Let's Encrypt:

```bash
sudo apt install certbot
sudo certbot certonly --manual --preferred-challenges dns -d your.domain.example.com
```

Follow the prompts to add a DNS TXT record to your domain. The certificate
files will be saved to `/etc/letsencrypt/live/your.domain.example.com/`.

Grant your user read access to the certificate files:

```bash
sudo chgrp -R youruser /etc/letsencrypt/live /etc/letsencrypt/archive
sudo chmod 750 /etc/letsencrypt/live /etc/letsencrypt/archive
sudo chmod 750 /etc/letsencrypt/live/your.domain.example.com
sudo chmod 750 /etc/letsencrypt/archive/your.domain.example.com
sudo chmod 640 /etc/letsencrypt/archive/your.domain.example.com/*.pem
```

The certificate expires every 90 days. To renew, repeat the `certbot certonly`
command, add the new DNS TXT record, then re-run the `chmod 640` line on the
new archive files (certbot creates them as root-only).

### Set up stunnel (Linux server only)

stunnel terminates HTTPS from Telegram and forwards plain HTTP to the bot
process. This is required because python-telegram-bot v22's built-in TLS
support does not correctly present the full Let's Encrypt certificate chain.

```bash
sudo apt install -y stunnel4
```

Create `/etc/stunnel/tdbridge.conf`:

```ini
; TDbridge TLS terminator

pid = /var/run/stunnel4/stunnel.pid

cert = /etc/letsencrypt/live/your.domain.example.com/fullchain.pem
key  = /etc/letsencrypt/live/your.domain.example.com/privkey.pem

[tdbridge-prod]
; Telegram connects on port 8443; bot listens on localhost:8444
accept  = 8443
connect = 127.0.0.1:8444

[tdbridge-test]
; Telegram connects on port 88; bot listens on localhost:8088
; Only add this section if running a persistent test instance
accept  = 88
connect = 127.0.0.1:8088
```

Enable and start stunnel:

```bash
sudo sed -i 's/ENABLED=0/ENABLED=1/' /etc/default/stunnel4
sudo systemctl enable stunnel4
sudo systemctl start stunnel4
sudo systemctl status stunnel4
```

After each certificate renewal, restart stunnel so it picks up the new cert:

```bash
sudo systemctl restart stunnel4
```

---

## Step 6 — Configure `.env`

Copy `envexample.txt` to `.env` and fill in the values. Never commit `.env`
to git — it contains secrets. The file has `PROD_` and `TEST_` prefixed
parameters; use the prefix that matches your environment.

### The `!` shorthand in message strings

Any parameter whose name ends in `_ERRMSG`, or `DC_MSG_DELETE_BEHAVIOR`, may
use `!` as the first character of its value as a shorthand for `⚠️`. This lets
you keep `.env` in plain ASCII while still producing a friendly warning emoji
in Telegram or Discord messages.

```
PROD_UNROUTABLE_DTOT_ERRMSG="! Unable to route this message to Telegram."
# stored internally as: "⚠️ Unable to route this message to Telegram."
```

### Credentials and identity

| Parameter | Description |
|---|---|
| `PROD_DISCORD_BOT_TOKEN` | Discord bot token (Developer Portal → Bot → Reset Token) |
| `PROD_DISCORD_BOT_NAME` | Bot username (no spaces) |
| `PROD_DISCORD_BOT_NICKNAME` | Display name shown in your server (may contain spaces) |
| `PROD_DISCORD_BOT_APPLICATION_ID` | From Developer Portal → General Information |
| `PROD_TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather |
| `PROD_TELEGRAM_BOT_NAME` | Human-readable bot name |
| `PROD_TELEGRAM_BOT_USERNAME` | Bot username on Telegram |
| `PROD_TELEGRAM_BOT_URL` | `https://t.me/<username>` |
| `PROD_GOOGLE_SPREADSHEET_NAME` | Exact name of your Google Sheet |
| `GOOGLE_CREDENTIALS_FILE` | Path to the Google service account JSON key file |
| `LOCAL_TIMEZONE` | IANA timezone name (e.g. `America/Los_Angeles`) |

### Webhook configuration (Linux server only)

On Windows, polling mode is used automatically and these are ignored.

| Parameter | Description |
|---|---|
| `PROD_TELEGRAM_WEBHOOK_URL` | Public HTTPS URL Telegram POSTs updates to |
| `PROD_TELEGRAM_WEBHOOK_PORT` | Internal port the bot listens on (stunnel forwards to this) |
| `PROD_TELEGRAM_WEBHOOK_SECRET` | Random secret for authenticating Telegram POST requests |
| `TLS_CERT_FILE` | Path to Let's Encrypt `fullchain.pem` |
| `TLS_KEY_FILE` | Path to Let's Encrypt `privkey.pem` |

Generate a webhook secret:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Database and logging

| Parameter | Default | Description |
|---|---|---|
| `PROD_SQLITE_DB_FILE` | `TDbridge_prod.db` | SQLite file storing TG↔DC message ID mappings |
| `PROD_LOGFILENAME` | `TDbridge_prod.log` | Rotating log file (5 MB × 5 backups) |
| `PROD_SHEETS_REFRESH_INTERVAL` | `300` | Seconds between Google Sheets cache refreshes |

### Message routing behavior

#### Unroutable messages

Each parameter is a message string (empty = silent; `!` at start = `⚠️`).
When a string is non-empty the event is logged at WARNING level; when empty,
at INFO level only.

| Parameter | When posted | Where |
|---|---|---|
| `PROD_UNROUTABLE_DTOT_ERRMSG` | A Discord message can't be routed to any Telegram group | In Discord as a reply |
| `PROD_UNROUTABLE_TTOD_ERRMSG` | A Telegram message can't be matched to any D_User row at all | In Telegram as a reply |
| `PROD_ROUTED_INACTIVE_TTOD_ERRMSG` | A Telegram message was routed via an Inactive D_User row | In Telegram as a reply |

#### Discord message deletion

| Value | Behavior |
|---|---|
| `delete` | Attempt to delete the corresponding Telegram message(s) |
| `ignore` | Do nothing (log only) |
| Any other string | Post that string as a Telegram reply (use `!` for `⚠️`) |

```
PROD_DC_MSG_DELETE_BEHAVIOR=delete
PROD_DELETE_FAIL_ERRMSG="! Telegram deletion failed."
```

#### Telegram-initiated deletion

Users can delete TG messages (and their Discord counterparts) by replying with
a message matching `TG_MSG_DELETE_REGEX`. The regex is applied with `re.fullmatch`.
Use `\s*` to allow trailing whitespace (common on iPhone autocomplete).

```
PROD_TG_MSG_DELETE_REGEX="(?i)delete\s*"
PROD_TG_MSG_DELETE_ERRMSG="! Delete failed."
```

Empty `TG_MSG_DELETE_REGEX` disables the feature entirely.

**How it works:**
1. Reply "delete" to any TG message
2. If the parent message is tracked, TDbridge deletes it from Telegram
3. If other TG messages share the same Discord message, only the replied-to message is removed — the Discord message stays
4. If this was the only TG message for that Discord message, the Discord message is also deleted
5. The "delete" reply is removed from Telegram on success

#### Reaction bridging

| Parameter | Values | Description |
|---|---|---|
| `PROD_REACTIONS_TTOD` | `react` / `reply` / `both` / `neither` | How to bridge Telegram reactions to Discord |
| `PROD_REACTIONS_DTOT` | `react` / `reply` / `both` / `neither` | How to bridge Discord reactions to Telegram |

`react` adds a native emoji reaction; `reply` posts a short reply message;
`both` does both (if native reaction fails, the reply still posts);
`neither` suppresses reaction bridging entirely. Telegram native reactions
support only a limited emoji set — unsupported emoji fall back gracefully
when `both` is set.

---

## Step 7 — Populate the mapping tables

### D_Channel_Sheet — which Discord channels to monitor

TDbridge discovers and inserts all channels it can see at startup. Set
`D_ChannelStatus` to `Active` for channels you want to monitor. Leave
others blank or set to anything other than `Active` (treated as Inactive).

The **first Active channel** in the table is used as the fallback destination
for Telegram messages that cannot be matched to any specific user or role,
so arrange your rows with that in mind.

### D_User_Sheet — users and roles

TDbridge discovers and inserts all Discord server members **and roles** at
startup. TDbridge never deletes rows — use `D_LastFound` to identify rows
for users or roles that no longer exist in Discord.

**User rows** have a numeric `D_ID`. TDbridge fills in `D_UserName`,
`D_Nickname`, and `D_DisplayName` automatically.

**Role rows** have `D_ID = &<role_id>` (e.g. `&1234567890`). TDbridge fills
in `D_Nickname` with the role name.

For each user or role you want to bridge, fill in:

| Column | What to put |
|---|---|
| `D_ChannelID` | The Discord channel ID this user/role uses (copy from D_Channel_Sheet) |
| `D_UserStatus` | Set to `Active` |
| `T_GroupID` | The Telegram group ID for this user/role (see below) |

**Finding the Telegram group ID:**
Invite [@ShowJsonBot](https://t.me/ShowJsonBot) into the Telegram group.
It will post a JSON message containing:
```json
"chat": {"id": -1003917181930, "title": "Alice | Group", "type": "supergroup"}
```
The `id` value (negative for groups) is the T_GroupID. Enter it as plain text.

### T_Group_Sheet — known Telegram groups

TDbridge populates this automatically when it first receives a message from
each Telegram group. Set `T_Status` to `Active` for groups you want bridged.

---

## Step 8 — Run TDbridge

### Development (Windows)

```bash
cd ~/TDbridge
source venv/Scripts/activate    # Git Bash
python bot.py --env prod
```

On Windows, TDbridge uses Telegram polling mode automatically — no webhook
or certificate setup is needed for development.

### Production (Linux, systemd)

Create `/etc/systemd/system/TDbridge.service`:

```ini
# TDbridge.service (added by yourname)
[Unit]
Description=TDbridge Telegram-Discord bridge (prod) (added by yourname)
After=network.target stunnel4.service
Requires=stunnel4.service

[Service]
TimeoutStopSec=60
ExecStart=/home/youruser/TDbridge/venv/bin/python -u bot.py --env prod
WorkingDirectory=/home/youruser/TDbridge
User=youruser
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable TDbridge
sudo systemctl start TDbridge
sudo systemctl status TDbridge
```

View logs:
```bash
tail -f ~/TDbridge/TDbridge_prod.log     # TDbridge's own rotating log
journalctl -u TDbridge -f                # systemd journal
```

---

## Step 9 — Invite the Telegram bot into groups

For each person you want to bridge:
1. Create a Telegram group (or use an existing one)
2. Invite the person into the group
3. Invite the TDbridge bot (`@TDbridgeProdBot` or your bot's username)
4. **Make the bot an administrator** in the group

Administrator status is required for two reasons:
- To receive reaction events (Telegram only delivers reactions to admin bots)
- To delete messages sent by other users (needed when Discord messages are deleted)

TDbridge will post a warning in the group if it detects it is not an administrator.

---

## How messages are routed

### Telegram → Discord

TDbridge applies these routing cases in order, stopping at the first match.

**Case 1 — Active user/role row**

The T_GroupID appears in a D_User row where `D_UserStatus = Active`. The
message is posted to that row's `D_ChannelID` and the Discord user or role
is tagged with `<@D_ID>`.

**Case 2 — Inactive user/role row**

No Active row matches, but an Inactive row exists for this T_GroupID. The
message is still routed to that row's `D_ChannelID` and tagged with `<@D_ID>`.
`ROUTED_INACTIVE_TTOD_ERRMSG` is posted in Telegram if configured. Discord
renders mentions of departed users as @DeletedUser.

**Case 3 — Fallback**

No D_User row (Active or Inactive) exists for this T_GroupID. The message is
posted to the first Active Discord channel with a pseudo-tag showing the
Telegram group title. `UNROUTABLE_TTOD_ERRMSG` is posted in Telegram if configured.

### Discord → Telegram

TDbridge applies these routing rules in order, stopping at the first match.

**Rule 1 — Reply to a bridged message**

Always routes to the same Telegram group as the original message, regardless
of channel or tags.

**Rule 2 — Tagged user or role**

TDbridge scans @mentions left to right. The first mention (user `<@id>` or
role `<@&id>`) whose D_User row is Active, has a T_GroupID, and has a
D_ChannelID matching the incoming channel determines the destination.

**Rule 3 — Sender's user ID**

If no tagged mention matches, TDbridge checks whether the sender's own D_User
row meets the same conditions (Active, T_GroupID, D_ChannelID matches).

**Rule 4 — Sender's roles**

If the sender's user row doesn't match, TDbridge iterates through all D_User
rows in table order and checks whether the sender belongs to any role row that
meets the same conditions. Table row order determines priority.

**Unroutable**

If no rule matches, `UNROUTABLE_DTOT_ERRMSG` is posted in Discord as a reply
(empty = silent).

### The "two hats" scenario

A person can belong to multiple roles and TDbridge will route correctly. For
example, Angel is both a driver and an Ops Manager:

- Angel's **user row** (`D_ID = 450...`) — set to Inactive
- **@Ops Manager role row** (`D_ID = &role_id`) — set to Active, pointing to the Ops Manager Telegram group

Result:
- Discord messages from Angel (no tag) → Rule 4 finds @Ops Manager role → routed to Ops Manager TG group
- Telegram messages from Angel's personal TG group → Case 2 finds Inactive @Angel row → tagged on Angel's Discord channel with ROUTED_INACTIVE_TTOD_ERRMSG
- Telegram messages from Ops Manager TG group → Case 1 finds Active @Ops Manager role row → tagged as @Ops Manager on Discord

### Attachment and media bridging

**Telegram → Discord:** Telegram allows one media item per message internally,
even when the app displays a "collage". A 20-photo Telegram collage is 20
separate messages, bridged as 20 individual Discord messages. Each is
independently tracked, so replies, reactions, and deletions work at the
individual photo level.

**Discord → Telegram:** Discord allows up to 10 attachments per message.
TDbridge sends photos and videos as a Telegram media group (native collage)
of up to 10 items. Documents are sent individually. All resulting Telegram
message IDs are stored in the database so deletions from Discord remove every
corresponding Telegram message.

**Attachment size limits:** Discord has a 25 MB per-file limit (free tier)
and Telegram has a 50 MB limit. Files exceeding the target platform's limit
are skipped with a warning posted on both platforms.

### User-locking a table

Rename the `Unlocked` column in any sheet to `Locked` (or any name starting
with "lock", case-insensitive) to prevent TDbridge from writing to that table
while you are rearranging rows or columns. Rename it back to `Unlocked` when
done. The lock duration is tracked and reported in the 30-minute Status Report.

---

## Monitoring system health

TDbridge writes a `Status Report` line to the log every 30 minutes. Each line
contains the current environment, overall status (OK / WARN / ERROR), Discord
connectivity, minutes since the last Telegram update, Google Sheets health,
lock duration, and message count for the period.

```
2026-06-01 10:00:01 PDT - INFO - TDbridge: Status Report | env=prod | status=OK | dc=connected | tg_idle_min=2 | sheets=ok | locked_min=0 | bridged_30m=47 | summary=47 bridged, all systems nominal
```

Every organization will want to monitor these reports in their own way. At
Squadron Trucking, we use an iPhone app called **iSH** (a bash-like shell)
to SSH into the server, where `~/.bashrc` automatically runs a shell script
that scans the logs and prints a summary. Other options include a simple web
server that checks the log and presents a summary page, an email alert script
triggered by a cron job, or any other monitoring approach that fits your setup.

Whatever method you choose, designate someone in your organization as
responsible for checking the status reports regularly.

---

## Troubleshooting

### Telegram messages not arriving on Discord

1. Check the log for `TG→DC: cache lookup` lines — they show which routing case matched
2. Verify `T_GroupID` in D_User_Sheet matches the Telegram group ID exactly (plain text)
3. Verify `D_UserStatus = Active` for that row
4. Verify `D_ChannelID` is set and that channel's `D_ChannelStatus = Active`
5. Verify the Telegram bot is an administrator in the group (TDbridge warns if not)

### Discord messages not arriving on Telegram

1. Check the log for `DC→TG routing` lines — they show which rule matched or why each was skipped
2. Verify the message was sent in an Active Discord channel
3. Verify the tagged user/role (or sender, or sender's roles) has `D_UserStatus = Active`, a `T_GroupID`, and a `D_ChannelID` matching the channel

### Reactions not being bridged from Telegram

The bot must be an **administrator** in the Telegram group. Telegram only
delivers reaction events to admin bots. TDbridge logs a warning and posts a
message in the group if it detects it is not an administrator.

### Large ID numbers look wrong in Sheets

All ID columns (`D_ID`, `D_ChannelID`, `T_GroupID`) must be formatted as
**Plain Text** before data is entered. If a 19-digit Discord snowflake is
stored as a number it will be rounded, losing information. To fix: clear the
cell, format as Plain Text, then re-enter the value.

### Certificate errors on webhook startup

```bash
openssl x509 -noout -subject -dates -in /etc/letsencrypt/live/your.domain/fullchain.pem
```

If this fails, re-run the `chgrp`/`chmod` commands from Step 5.
