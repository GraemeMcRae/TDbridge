# TDbridge Guide

## What is TDbridge?

TDbridge is a message bridge between Telegram groups and a Discord server.
People on Discord send messages in a small number of monitored channels,
tagging others by name to indicate who on the Telegram side should receive
them.  People on Telegram send messages in their own group, and TDbridge
delivers those messages to the appropriate Discord channel.

Both sides see a continuous conversation as if they were in the same chat.
Replies, edits, reactions, and attachments are all bridged in both directions.

---

## Concepts

### Mapping model

Each Telegram user has their own private Telegram group that TDbridge's
Telegram bot has been invited into.  When that user sends a message in their
group, TDbridge posts it to a designated Discord channel and tags the
corresponding Discord user.

When a Discord user sends a message in a monitored channel, TDbridge delivers
it to the Telegram group of whoever they tagged (or their own group if they
didn't tag anyone).

### The three mapping tables (in Google Sheets)

| Table | What it maps |
|---|---|
| **D_User** | Discord user ↔ Telegram group |
| **D_Channel** | Discord channels to monitor |
| **T_Group** | Known Telegram groups |

TDbridge reads these tables at startup and refreshes them every 5 minutes
(configurable).  You control who is active by setting status fields; TDbridge
never overwrites your status values.

---

## Prerequisites

Before setting up TDbridge you will need:

- A Discord server where you have Administrator permissions
- A Telegram account
- A Google account (for Google Sheets)
- A Linux server reachable at a public hostname (for production)
- Python 3.11+ installed on your development machine and server

---

## Step 1 — Create the Discord bots

Create two bots: one for testing, one for production.

1. Go to https://discord.com/developers/applications and click **New Application**
2. Name it (e.g. `TDbridgeTest`) and click **Create**
3. Go to **Bot** in the left panel
4. Click **Reset Token** and copy the token — this is your `TEST_DISCORD_BOT_TOKEN`
5. Under **Privileged Gateway Intents**, enable:
   - **Server Members Intent** (required to fetch all guild members)
   - **Message Content Intent** (required to read message text)
6. Click **Save Changes**
7. Go to **OAuth2 → URL Generator**
8. Under Scopes, check `bot`
9. Under Bot Permissions, check:
   - Read Messages/View Channels
   - Send Messages
   - Manage Webhooks
   - Read Message History
   - Add Reactions
10. Copy the generated URL, paste it in your browser, and invite the bot to your server
11. Repeat steps 1–10 for the production bot (e.g. `TDbridge`)

Note the **Application ID** and **Public Key** for each bot from the **General Information** page — these go in `.env` as `TEST_DISCORD_BOT_APPLICATION_ID` etc.

---

## Step 2 — Create the Telegram bots

1. Open Telegram and start a chat with **@BotFather**
2. Send `/newbot` and follow the prompts to name your bot
3. BotFather will give you a bot token — this is your `TEST_TELEGRAM_BOT_TOKEN`
4. Note the bot username (e.g. `TDbridgeTestBot`) — this is `TEST_TELEGRAM_BOT_USERNAME`
5. Repeat for the production bot

**Important Telegram bot settings** (send these commands to @BotFather):

```
/setprivacy — select your bot — Disable
```

Privacy mode must be **disabled** so the bot can read all messages in groups,
not just commands directed at it.

---

## Step 3 — Create the Google Sheets spreadsheets

Create two spreadsheets: one for test, one for production.

### Create a Google Cloud service account

1. Go to https://console.cloud.google.com
2. Create a new project (e.g. `tdbridge`)
3. Go to **APIs & Services → Library** and enable the **Google Sheets API**
4. Go to **APIs & Services → Credentials**
5. Click **Create Credentials → Service Account**
6. Name it (e.g. `tdbridge-service`) and click **Done**
7. Click the service account, go to **Keys**, click **Add Key → Create new key → JSON**
8. Download the JSON file — save it as `google_credentials_TDbridge.json` in your TDbridge folder
9. Note the service account email address (e.g. `tdbridge-service@tdbridge.iam.gserviceaccount.com`)

### Create and share the spreadsheets

1. In Google Drive, create a new spreadsheet named exactly `TDbridge Config Test`
2. Share it with the service account email address (Editor access)
3. Create a second spreadsheet named exactly `TDbridge Config` for production
4. Share it with the service account email too

### Create the sheets and columns

In each spreadsheet, create three sheets (tabs) with these exact names and
column headers in row 1:

**Sheet: D_User_Sheet**
```
D_ID | D_UserName | D_Nickname | D_DisplayName | D_LastFound | D_ChannelID | D_ChannelName | D_UserStatus | T_GroupID | T_Title | T_LastFound
```

**Sheet: D_Channel_Sheet**
```
D_ChannelID | D_ChannelName | D_LastFound | D_ChannelStatus
```

**Sheet: T_Group_Sheet**
```
T_GroupID | T_Title | T_Type | T_LastFound | T_Status
```

**Format these columns as Plain Text** (Format → Number → Plain text) before
entering any data: `D_ID`, `D_ChannelID`, `T_GroupID`.  This prevents Google
Sheets from treating large integers as floating-point numbers.

You may add extra columns to any sheet — TDbridge reads and writes only the
columns it knows about and ignores all others.

---

## Step 4 — Set up TDbridge on your server

### Clone the repository

```bash
git clone https://github.com/GraemeMcRae/TDbridge.git ~/TDbridge
cd ~/TDbridge
python3 -m venv venv
source venv/bin/activate      # Linux
source venv/Scripts/activate  # Windows Git Bash
pip install -r requirements.txt
```

### Copy credentials and configuration

```bash
cp envexample.txt .env
# Edit .env and fill in all the tokens and secrets (see Step 5)
cp /path/to/google_credentials_TDbridge.json .
```

### Obtain a TLS certificate (Linux server only)

TDbridge uses **stunnel** to terminate TLS, which requires a valid certificate
from a trusted CA.  Use Let's Encrypt:

```bash
sudo apt install certbot
sudo certbot certonly --manual --preferred-challenges dns -d your.domain.example.com
```

Follow the prompts to add a DNS TXT record to your domain, then complete
the certificate request.  The certificate files will be saved to
`/etc/letsencrypt/live/your.domain.example.com/`.

Grant the bot user read access to the certificate files:

```bash
sudo chgrp -R youruser /etc/letsencrypt/live /etc/letsencrypt/archive
sudo chmod 750 /etc/letsencrypt/live /etc/letsencrypt/archive
sudo chmod 750 /etc/letsencrypt/live/your.domain.example.com
sudo chmod 750 /etc/letsencrypt/archive/your.domain.example.com
sudo chmod 640 /etc/letsencrypt/archive/your.domain.example.com/*.pem
```

The certificate expires every 90 days.  To renew, repeat the `certbot certonly`
command, add the new DNS TXT record when prompted, then re-run the `chmod 640`
line on the new archive files (certbot creates them as root-only).

### Set up stunnel (Linux server only)

stunnel terminates HTTPS from Telegram and forwards plain HTTP to the bot.
This is required because python-telegram-bot v22's built-in webhook server
only presents the leaf certificate to clients, causing Telegram to reject
the connection.  stunnel correctly presents the full certificate chain.

```bash
sudo apt install -y stunnel4
```

Create `/etc/stunnel/tdbridge.conf`:

```ini
; TDbridge TLS terminator
; Terminates HTTPS from Telegram and forwards plain HTTP to the bot.

pid = /var/run/stunnel4/stunnel.pid

cert = /etc/letsencrypt/live/your.domain.example.com/fullchain.pem
key  = /etc/letsencrypt/live/your.domain.example.com/privkey.pem

[tdbridge-test]
; Telegram connects on port 88; bot listens on localhost:8088
accept  = 88
connect = 127.0.0.1:8088

[tdbridge-prod]
; Telegram connects on port 8443; bot listens on localhost:8444
accept  = 8443
connect = 127.0.0.1:8444
```

Enable and start stunnel:

```bash
sudo sed -i 's/ENABLED=0/ENABLED=1/' /etc/default/stunnel4
sudo systemctl enable stunnel4
sudo systemctl start stunnel4
sudo systemctl status stunnel4
```

Set the internal webhook ports in `.env`:

```
TEST_TELEGRAM_WEBHOOK_PORT=8088
PROD_TELEGRAM_WEBHOOK_PORT=8444
```

The public webhook URLs remain unchanged — Telegram always connects to
ports 88 and 8443 on the public domain.

**After certificate renewal**, restart stunnel so it picks up the new cert:

```bash
sudo systemctl restart stunnel4
```

---

## Step 5 — Configure `.env`

Copy `envexample.txt` to `.env` and fill in the values.  Never commit `.env`
to git — it contains secrets.  The file has two sets of parameters: `TEST_`
prefix for the test bot and `PROD_` prefix for the production bot.  Parameters
with no prefix (timezone, credentials, TLS) are shared by both.

### The `!` shorthand in message strings

Any parameter whose name ends in `_ERRMSG`, or `DC_MSG_DELETE_BEHAVIOR`, may
use `!` as the first character of its value as a shorthand for `⚠️` (the warning
emoji).  This lets you keep the `.env` file in plain ASCII while still
producing a friendly emoji in Telegram or Discord messages.

```
TEST_UNROUTABLE_DTOT_ERRMSG="! Unable to route this message to Telegram."
# stored internally as: "⚠️ Unable to route this message to Telegram."
```

### Credentials and identity

| Parameter | Description |
|---|---|
| `TEST_DISCORD_BOT_TOKEN` | Discord bot token (from Discord Developer Portal → Bot → Reset Token) |
| `TEST_DISCORD_BOT_NAME` | Bot username (no spaces; matches the bot's Discord account name) |
| `TEST_DISCORD_BOT_NICKNAME` | Display name shown in your server (may contain spaces) |
| `TEST_DISCORD_BOT_APPLICATION_ID` | From Developer Portal → General Information |
| `TEST_TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather |
| `TEST_TELEGRAM_BOT_NAME` | Human-readable bot name |
| `TEST_TELEGRAM_BOT_USERNAME` | Bot username on Telegram (e.g. `TDbridgeTestBot`) |
| `TEST_TELEGRAM_BOT_URL` | `https://t.me/<username>` |
| `TEST_GOOGLE_SPREADSHEET_NAME` | Exact name of the Google Sheet (must match precisely) |
| `GOOGLE_CREDENTIALS_FILE` | Path to the Google service account JSON key file |
| `LOCAL_TIMEZONE` | IANA timezone name (e.g. `America/Los_Angeles`) |

### Webhook configuration (Linux server only)

On Windows, polling mode is used automatically and these are ignored.

| Parameter | Description |
|---|---|
| `TEST_TELEGRAM_WEBHOOK_URL` | Public HTTPS URL Telegram POSTs updates to |
| `TEST_TELEGRAM_WEBHOOK_PORT` | Internal port the bot listens on (stunnel forwards to this) |
| `TEST_TELEGRAM_WEBHOOK_SECRET` | Random secret for authenticating Telegram POST requests |
| `TLS_CERT_FILE` | Path to Let’s Encrypt `fullchain.pem` (shared by test and prod) |
| `TLS_KEY_FILE` | Path to Let’s Encrypt `privkey.pem` (shared by test and prod) |

Generate webhook secrets:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Database and logging

| Parameter | Default | Description |
|---|---|---|
| `TEST_SQLITE_DB_FILE` | `TDbridge_test.db` | SQLite file storing TG↔DC message ID mappings |
| `TEST_LOGFILENAME` | `TDbridge_test.log` | Rotating log file (5 MB × 5 backups) |
| `TEST_SHEETS_REFRESH_INTERVAL` | `300` | Seconds between Google Sheets cache refreshes |

### Message routing behavior

#### Unroutable messages

A message is “unroutable” when TDbridge cannot determine a destination.
Each parameter is a message string (empty = silent; `!` at start = `⚠️`).

| Parameter | When posted | Where |
|---|---|---|
| `TEST_UNROUTABLE_DTOT_ERRMSG` | A Discord message on an Active channel has no matching Active TG group | In Discord as a reply to the unroutable message |
| `TEST_UNROUTABLE_TTOD_ERRMSG` | A Telegram message arrives from an untracked or Inactive group | In Telegram as a reply to the unroutable message |

**Note:** An unroutable TG message is still forwarded to the first Active Discord
channel as a fallback.  The `UNROUTABLE_TTOD_ERRMSG` alerts the Telegram sender
that full routing was not resolved.

#### Discord message deletion

When a Discord message is deleted, `DC_MSG_DELETE_BEHAVIOR` controls what
happens on the Telegram side:

| Value | Behavior |
|---|---|
| `delete` | Attempt to delete the corresponding Telegram message(s) |
| `ignore` | Do nothing (log only) |
| Any other string | Post that string as a Telegram reply (use `!` for `⚠️`) |

If deletion of a Telegram message fails, `DELETE_FAIL_ERRMSG` is posted on
Telegram (empty = silent):

```
TEST_DC_MSG_DELETE_BEHAVIOR=delete
TEST_DELETE_FAIL_ERRMSG="! Telegram deletion failed."
```

#### Telegram-initiated deletion

Users can delete TG messages (and their Discord counterparts) by replying with
a message matching `TG_MSG_DELETE_REGEX`.  The regex is applied with
`re.fullmatch` so it covers the entire reply text.  Use `\s*` to allow trailing
whitespace (common on iPhone autocomplete).

```
TEST_TG_MSG_DELETE_REGEX="(?i)delete\s*"
TEST_TG_MSG_DELETE_ERRMSG="! Delete failed."
```

Empty `TG_MSG_DELETE_REGEX` disables the feature entirely.

**How it works:**
1. Reply "delete" (or your chosen word) to any TG message
2. If the parent message is tracked in the DB, TDbridge deletes it from Telegram
3. If other TG messages share the same Discord message, only the replied-to TG message is removed (disassociated) — the Discord message stays
4. If this was the only TG message for that Discord message, the Discord message is also deleted
5. The "delete" reply is removed from Telegram on success

#### Reaction bridging

| Parameter | Values | Description |
|---|---|---|
| `TEST_REACTIONS_TTOD` | `react` / `reply` / `both` / `neither` | How to bridge Telegram reactions to Discord |
| `TEST_REACTIONS_DTOT` | `react` / `reply` / `both` / `neither` | How to bridge Discord reactions to Telegram |

| Mode | Effect |
|---|---|
| `react` | Adds the emoji as a native reaction on the target platform |
| `reply` | Posts a short reply message (e.g. `❤️ Alice reacted to this message`) |
| `both` | Does both; if native reaction fails, the reply still posts |
| `neither` | Reactions are not bridged |

**Note:** Telegram native reactions only support a limited set of emoji.
Unsupported emoji fall back gracefully when `both` is set.


## Step 6 — Populate the mapping tables

### D_Channel_Sheet — which Discord channels to monitor

TDbridge will discover and insert all channels it can see at startup.  You
then set `D_ChannelStatus` to `Active` for channels you want to monitor.
Leave others blank or set to `Inactive`.

### D_User_Sheet — who maps to which Telegram group

TDbridge will discover and insert all Discord server members at startup.
For each person you want to bridge, you need to fill in two columns:

| Column | What to put |
|---|---|
| `D_ChannelID` | The Discord channel ID this person uses (copy from D_Channel_Sheet) |
| `D_UserStatus` | Set to `Active` |
| `T_GroupID` | The Telegram group ID for this person (see below) |

**Finding the Telegram group ID:**
Invite [@ShowJsonBot](https://t.me/ShowJsonBot) into the Telegram group.
It will post a JSON message; find the `"chat"` entry which looks like:
```json
"chat": {"id": -1003917181930, "title": "Alice | Group", "type": "supergroup"}
```
The `id` value is the T_GroupID.  Enter it as a plain text string.

### T_Group_Sheet — known Telegram groups

TDbridge populates this automatically when it first receives a message from
each Telegram group.  You set `T_Status` to `Active` for groups you want
to bridge.

---

## Step 7 — Running TDbridge

### Development (Windows)

```bash
source venv/Scripts/activate
python bot.py --env test
```

On Windows, TDbridge uses Telegram polling mode automatically — no webhook
setup is needed.

### Production (Linux, manual)

```bash
source venv/bin/activate
python bot.py --env prod
```

### Production (Linux, systemd)

Create `/etc/systemd/system/tdbridge.service`:

```ini
[Unit]
Description=TDbridge Telegram-Discord bridge (prod)
After=network.target

[Service]
TimeoutStopSec=60
ExecStart=/home/youruser/TDbridge/venv/bin/python -u bot.py --env prod
WorkingDirectory=/home/youruser/TDbridge
User=youruser
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable tdbridge
sudo systemctl start tdbridge
sudo systemctl status tdbridge
```

For the test instance, create `tdbridge-test.service` with `--env test`.

View logs:
```bash
journalctl -u tdbridge -f          # systemd logs
tail -f ~/TDbridge/TDbridge_prod.log  # TDbridge's own rotating log
```

---

## Step 8 — Invite the Telegram bot into groups

For each person you want to bridge, create a Telegram group and invite:
- The person themselves
- The TDbridge Telegram bot (`@TDbridgeProdBot` or `@TDbridgeTestBot`)

The bot needs to be able to **read all messages** in the group.  If the bot
was added before disabling privacy mode in @BotFather, remove it and re-add
it.

---

## How messages are routed

### Telegram → Discord

Any message sent in a Telegram group that TDbridge's bot is a member of is
bridged to Discord, provided:

- The Telegram group has `T_Status = Active` in T_Group_Sheet (or a new
  message will add it and you set it to Active)
- The group's T_GroupID appears in a D_User row where `D_UserStatus = Active`
  and `D_ChannelID` points to an Active Discord channel

The message appears in the Discord channel attributed to the Telegram sender.
The mapped Discord user is tagged at the top of the message.

### Discord → Telegram

TDbridge applies these rules in order, stopping at the first match.

**Rule 1 — Reply to a bridged message**

If the Discord message is a reply to a message that was previously bridged
(in either direction), it is always sent to the same Telegram group as that
original message.  The channel the reply was sent in and any @mentions in the
reply are both ignored for routing purposes — the reply chain determines the
destination.

**Rule 2 — Tagged user**

If this is a new message (not a reply), TDbridge scans the @mentions in the
message from left to right and uses the *first* mentioned user who meets all
three conditions:
- `D_UserStatus = Active` in D_User_Sheet
- `T_GroupID` is filled in (they have a Telegram group)
- `D_ChannelID` matches the channel the message was sent in

The message is sent to that user's Telegram group.  If you tag multiple users,
only the first qualifying one determines the destination.

**Rule 3 — Sender fallback**

If no tagged user qualifies, TDbridge checks whether the *sender* meets the
same three conditions (Active, has T_GroupID, D_ChannelID matches).  If so,
the message is sent to the sender's own Telegram group.

**Rule 4 — Unroutable**

If none of the above rules produce a match, TDbridge cannot determine where
to send the message.  Behaviour is controlled by `UNROUTABLE_BEHAVIOR` in `.env`:

- `warn` — TDbridge posts a reply in Discord: *"⚠️ Could not route this
  message to Telegram."*  Use this during initial setup to catch
  configuration gaps.
- `ignore` — The message is silently dropped (logged at INFO level only).
  Use this in production once everything is configured.

**Example** (using a channel called #dispatch-comms)

| Scenario | Result |
|---|---|
| Reply to a previously bridged message | Sent to the Telegram group of that message's chain, regardless of channel or tags |
| New message tagging @Angel (Active, T_GroupID set, D_ChannelID = dispatch-comms) | Sent to Angel's Telegram group |
| New message tagging @Angel then @Boont — Angel is Inactive, Boont is Active with dispatch-comms | Sent to Boont's Telegram group (Angel skipped, Boont is first qualifying mention) |
| New message, no tags, sender is Active with dispatch-comms as their channel | Sent to sender's Telegram group |
| New message, no tags, sender has no T_GroupID | Unroutable — behaviour per `UNROUTABLE_BEHAVIOR` |
| Message in a channel not listed as Active in D_Channel_Sheet | Silently ignored — TDbridge does not monitor that channel |


### Attachment and media bridging notes

**Telegram → Discord:** Telegram allows one media item per message internally,
even when the app displays multiple photos as a "collage".  A 20-photo Telegram
collage is actually 20 separate Telegram messages (each with one photo), so
TDbridge bridges them as 20 individual Discord messages.  Each TG message is
independently tracked in the database, so replies, reactions, and deletions on
either side work correctly at the individual photo level.

**Discord → Telegram:** Discord allows up to 10 attachments per message.
TDbridge sends photos and videos as a Telegram media group (native album/collage)
of up to 10 items per group, so a 20-photo Discord message becomes two Telegram
collages.  Documents are sent individually after the photo group.  All resulting
Telegram message IDs are stored in the database, so deletions from Discord
remove every corresponding Telegram message.

**Reactions on collages:** When you react to a Telegram collage, Telegram
associates the reaction with the first message in the collage.  TDbridge bridges
this as a reaction (or reply, depending on `REACTIONS_TTOD`) to the first
corresponding Discord message in that group.

**Attachment size limits:** Discord has a 25 MB per-file limit (free tier) and
Telegram has a 50 MB limit.  Files exceeding the target platform's limit are
skipped with a warning posted on both platforms indicating the filename, type,
and reason.

### User-locking a table

If any column name in a sheet begins with `lock` (case-insensitive), TDbridge
will read from the table but not write to it.  This lets you safely edit the
table without risk of concurrent writes from TDbridge.  TDbridge will retry
the write every minute for up to 10 minutes before backing off to 10-minute
intervals.

---

## Troubleshooting

### Telegram messages not arriving on Discord

1. Check the log for `TG→DC: cache lookup` lines — they show whether the
   group ID was found in the routing cache
2. Verify `T_GroupID` in D_User_Sheet is set and matches the Telegram group ID
   exactly (it is a text string, not a number)
3. Verify `D_UserStatus = Active` for that row
4. Verify `D_ChannelID` is set and the target channel's `D_ChannelStatus = Active`
5. Verify the Telegram bot has been invited to the group and privacy mode is
   disabled (see Step 8)

### Discord messages not arriving on Telegram

1. Check the log for `DC→TG routing` lines
2. Verify the message was sent in an Active Discord channel
3. Verify the tagged user (or sender) has `D_UserStatus = Active`, a
   `T_GroupID`, and a `D_ChannelID` matching the channel
4. Verify the Telegram bot is a member of the target group

### 403 Forbidden on webhook creation

The Discord bot needs **Manage Webhooks** permission in the channel.
Go to Channel Settings → Permissions and grant it to the bot's role.

### Certificate errors on webhook startup

Verify the cert files are readable:
```bash
openssl x509 -noout -subject -dates -in /etc/letsencrypt/live/your.domain/fullchain.pem
```
If this fails, re-run the `chgrp`/`chmod` commands from Step 4.

### Large ID numbers look wrong

All ID columns (D_ID, D_ChannelID, T_GroupID) must be formatted as
**Plain Text** in Google Sheets before data is entered.  If a 19-digit
Discord snowflake is stored as a number it will be rounded, losing
information.  To fix an affected cell: clear it, format as Plain Text,
then re-enter the value.
