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
to git — it contains secrets.

### Key parameters

| Parameter | Description |
|---|---|
| `TEST_DISCORD_BOT_TOKEN` | Discord bot token from the Developer Portal |
| `TEST_TELEGRAM_BOT_TOKEN` | Telegram bot token from @BotFather |
| `TEST_TELEGRAM_WEBHOOK_URL` | Public HTTPS URL Telegram will POST updates to |
| `TEST_TELEGRAM_WEBHOOK_PORT` | Port the bot listens on (88 for test, 8443 for prod) |
| `TEST_TELEGRAM_WEBHOOK_SECRET` | Random secret for webhook authentication |
| `TEST_GOOGLE_SPREADSHEET_NAME` | Exact name of the Google Sheet |
| `TLS_CERT_FILE` | Path to the Let's Encrypt fullchain.pem |
| `TLS_KEY_FILE` | Path to the Let's Encrypt privkey.pem |
| `LOCAL_TIMEZONE` | Your timezone (e.g. `America/Los_Angeles`) |
| `GOOGLE_CREDENTIALS_FILE` | Path to the Google service account JSON file |

### Generate webhook secrets

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Run this twice and put the outputs in `TEST_TELEGRAM_WEBHOOK_SECRET` and
`PROD_TELEGRAM_WEBHOOK_SECRET`.

### Behavioural parameters

| Parameter | Values | Description |
|---|---|---|
| `TEST_UNROUTABLE_BEHAVIOR` | `warn` / `ignore` | What to do when a Discord message can't be routed |
| `TEST_DELETE_BEHAVIOR` | `delete` / `notify` / `ignore` | What to do when a Discord message is deleted |
| `TEST_DELETE_FAIL_NOTIFY` | `true` / `false` | Post a notice if Telegram deletion fails |
| `TEST_REACTIONS_TTOD` | `react` / `reply` / `both` / `neither` | How to bridge Telegram reactions to Discord |
| `TEST_REACTIONS_DTOT` | `react` / `reply` / `both` / `neither` | How to bridge Discord reactions to Telegram |
| `TEST_SHEETS_REFRESH_INTERVAL` | seconds | How often to re-read the mapping tables (default 300) |

All behavioural parameters have `PROD_` equivalents.

#### Reaction bridging modes

| Mode | Telegram→Discord | Discord→Telegram |
|---|---|---|
| `react` | Adds the emoji as a native Discord reaction on the message | Adds the emoji as a native Telegram reaction (limited emoji set) |
| `reply` | Posts a reply: `❤️ Alice reacted to this message` | Posts a Telegram reply with the same text |
| `both` | Does both | Does both; if native reaction fails, reply still posts |
| `neither` | Does nothing | Does nothing |

Note: Telegram only supports a limited set of emoji for native reactions.
`react` or `both` will log a warning and fall back gracefully for unsupported emoji.

---

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

A Discord message is bridged to Telegram when:

1. **Reply to a bridged message** — always routes to the same Telegram group
   as the original message, regardless of channel or tags
2. **Tagged user** — the first `@mention` in the message (left to right) whose
   row in D_User_Sheet is Active, has a T_GroupID, and has a D_ChannelID
   matching the channel the message was sent in
3. **Sender fallback** — if no tagged user matches, the sender's own D_User
   row is used (same Active + T_GroupID + D_ChannelID requirements)
4. **Unroutable** — if none of the above apply, behaviour is controlled by
   `UNROUTABLE_BEHAVIOR`

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
