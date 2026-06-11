# TDbridge TLS and Certificate Management

## Overview

Telegram's Bot API requires that webhook endpoints use HTTPS with a certificate
signed by a trusted Certificate Authority.  This document explains how TDbridge
satisfies that requirement, how the various ports and processes relate to each
other, how the certificate was obtained and maintained, and how renewal is
automated.

---

## The problem: python-telegram-bot v22 and certificate chains

Let's Encrypt issues certificates with a chain of trust: the server presents
a **leaf certificate** (issued for your domain) plus one or more **intermediate
CA certificates** that connect the leaf to a root CA that browsers and servers
already trust.  All three are stored in `fullchain.pem`.

python-telegram-bot v22's built-in webhook server (tornado) accepts `key=` and
`cert=` file path arguments.  However, when it builds the TLS context from
these paths it only loads the leaf certificate — it discards the intermediate
CA certificates.  When Telegram's servers connect, they reject the handshake
because they cannot verify the leaf certificate without seeing the intermediate.
This produces the error:

```
[SSL: TLSV1_ALERT_UNKNOWN_CA] tlsv1 alert unknown ca
```

The solution is to place a TLS terminator in front of the bot that correctly
presents the full certificate chain.  We use **stunnel4** for this purpose.

---

## Architecture: how the pieces fit together

```
Internet
    │
    │  HTTPS (TLS)
    ▼
stunnel4  — listens on public ports 88 (test) and 8443 (prod)
            presents fullchain.pem to Telegram's servers
            terminates TLS
            forwards plain HTTP to the bot
    │
    │  Plain HTTP (no TLS)
    ▼
python-telegram-bot webhook server
    — listens on 127.0.0.1:8088 (test) and 127.0.0.1:8444 (prod)
    — bound to localhost only (not reachable from the internet directly)
    │
    ▼
bot.py — processes the update, bridges the message
```

**Why different ports?**  Telegram only accepts webhooks on four specific ports:
80, 88, 443, and 8443.  We use 88 for the test instance and 8443 for
production, so both can run simultaneously on the same server.

**Why localhost only?**  The bot's HTTP server is bound to `127.0.0.1`, not
`0.0.0.0`.  This means it cannot be reached directly from the internet — all
traffic must pass through stunnel.  This is a security measure: if stunnel
were to stop, incoming Telegram updates would simply fail rather than arriving
unencrypted over plain HTTP.

**Port mapping summary:**

| Instance | Telegram connects to | stunnel forwards to |
|---|---|---|
| Test | `hcf.squadrontrucking.com:88` | `127.0.0.1:8088` |
| Production | `hcf.squadrontrucking.com:8443` | `127.0.0.1:8444` |

---

## Certificate details

**Provider:** Let's Encrypt (free, automated, 90-day certificates)

**Domain:** `hcf.squadrontrucking.com`

**Certificate files** (at `/etc/letsencrypt/live/hcf.squadrontrucking.com/`):

| File | Contents | Used by |
|---|---|---|
| `fullchain.pem` | Leaf certificate + intermediate CAs | stunnel4 (`cert =`) |
| `privkey.pem` | Private key for the leaf certificate | stunnel4 (`key =`) |
| `cert.pem` | Leaf certificate only | Not used by TDbridge |
| `chain.pem` | Intermediate CAs only | Not used by TDbridge |

The files in the `live/` directory are symlinks pointing into the `archive/`
directory, where the actual files live.  When certbot renews the certificate,
it creates new files in `archive/` and updates the symlinks in `live/`.

**File permissions:**  Let's Encrypt creates certificate files as root-only
(mode 600).  TDbridge runs as the `graeme` user, so we must grant read access:

```bash
sudo chgrp -R graeme /etc/letsencrypt/live /etc/letsencrypt/archive
sudo chmod 750 /etc/letsencrypt/live /etc/letsencrypt/archive
sudo chmod 750 /etc/letsencrypt/live/hcf.squadrontrucking.com
sudo chmod 750 /etc/letsencrypt/archive/hcf.squadrontrucking.com
sudo chmod 640 /etc/letsencrypt/archive/hcf.squadrontrucking.com/*.pem
```

The `chmod 640` step must be repeated after each renewal because certbot
creates the new archive files as root-only.  The renewal script (`cert_renew.sh`)
handles this automatically.

---

## stunnel4 configuration

**Config file:** `/etc/stunnel/tdbridge.conf`

```ini
; TDbridge TLS terminator
; Accepts HTTPS connections from Telegram on the public ports,
; terminates TLS using the full Let's Encrypt certificate chain,
; and forwards plain HTTP to the bot process on localhost.

pid = /var/run/stunnel4/stunnel.pid

; Certificate — fullchain.pem includes the leaf cert AND intermediate CAs.
; This is essential: presenting only the leaf cert causes Telegram to reject
; the connection with TLSV1_ALERT_UNKNOWN_CA.
cert = /etc/letsencrypt/live/hcf.squadrontrucking.com/fullchain.pem
key  = /etc/letsencrypt/live/hcf.squadrontrucking.com/privkey.pem

[tdbridge-test]
accept  = 88
connect = 127.0.0.1:8088

[tdbridge-prod]
accept  = 8443
connect = 127.0.0.1:8444
```

**systemd service:**

```bash
sudo systemctl status stunnel4     # check status
sudo systemctl restart stunnel4    # restart (required after certificate renewal)
sudo journalctl -u stunnel4 -n 20  # view recent stunnel logs
```

---

## Initial certificate acquisition

The certificate was originally obtained using the **DNS challenge** because
port 80 was occupied by Webuzo's Apache server.  The DNS challenge proves
domain ownership by adding a TXT record to the domain's DNS rather than
serving a file over HTTP.

```bash
sudo certbot certonly --manual --preferred-challenges dns -d hcf.squadrontrucking.com
```

certbot prompts you to add a TXT record like:
```
_acme-challenge.hcf.squadrontrucking.com  →  <long-random-string>
```

This was added via Squarespace's DNS management interface (the domain
`squadrontrucking.com` is registered and DNS-managed through Squarespace).
After adding the record and waiting ~60 seconds for propagation, certbot
verified ownership and issued the certificate.

The DNS challenge process is fully manual and takes about 10 minutes.  It
was used once for the initial certificate and would be needed again only if
the HTTP challenge became unavailable.

---

## Freeing port 80 from Webuzo

Webuzo is a control panel pre-installed by Namecheap on the VPS.  It runs
its own Apache web server (`/usr/local/apps/apache2/bin/httpd`) on port 80.
Namecheap confirmed that removing Webuzo entirely would require rebuilding
the server from scratch, which was not acceptable.

Instead, we stopped and disabled Webuzo's Apache service while leaving
Webuzo itself installed:

```bash
sudo systemctl stop httpd.service
sudo systemctl disable httpd.service
```

`webuzo.service` (the Webuzo management daemon) continues to run but no
longer starts httpd.  Port 80 is now permanently free.

**Verification:**
```bash
sudo ss -tlnp | grep ':80\b'   # should return nothing
sudo ps aux | grep httpd | grep -v grep  # should return nothing
```

If `webuzo.service` ever restarts httpd unexpectedly, the daily `cert_renew.sh`
script will detect this and log an error.

---

## Automatic certificate renewal

### How it works

The renewal script `~/TDbridge/cert_renew.sh` runs daily via cron.
Each run:

1. **Frees port 80** — kills any process (typically Webuzo's httpd watchdog)
   holding port 80, then verifies it is free.  Also checks that
   `httpd.service` is still stopped and disabled, re-stopping/disabling it
   if Webuzo's watchdog has re-enabled it.
2. **Reads the certificate expiry date** using `openssl x509 -noout -enddate`
3. **If 30 or more days until expiry** (`days_remaining >= 30`): deletes the
   staging account cache and performs a certbot dry run using the
   `--standalone` challenge.  The dry run contacts Let's Encrypt's
   **staging servers** — not production — so it can run daily without
   hitting rate limits.
4. **If fewer than 30 days until expiry** (`days_remaining < 30`): performs
   the real renewal against the production ACME server, fixes certificate
   file permissions (`chmod 640`), and restarts stunnel4.

The threshold uses `days_remaining >= 30` (not `> 30`) because `days_remaining`
is computed by integer division which rounds down.  A certificate expiring in
30.9 days gives `days_remaining = 30` — still a full day of margin — so a dry
run is appropriate.  The next day it gives `days_remaining = 29`, which
triggers the real renewal.

### The staging account cache

certbot 2.9.0 has a bug specific to the staging environment: when a valid
staging authorization exists from a previous dry run, certbot deactivates it
to force a fresh challenge, but the staging server returns the same deactivated
authorization on the new order.  certbot then tries to answer a challenge on a
deactivated authorization, which Let's Encrypt rejects with:

```
Unable to update challenge :: authorization must be pending
```

The fix is to delete the staging account cache before each dry run:

```bash
rm -rf /etc/letsencrypt/accounts/acme-staging-v02.api.letsencrypt.org
```

This forces certbot to register a fresh staging account with no existing
authorizations.  The production account cache (at
`/etc/letsencrypt/accounts/acme-v02.api.letsencrypt.org`) is a completely
separate directory and is never touched by this deletion.  The real renewal
path is unaffected.

### The Ubuntu certbot.timer — why we ignore it

Ubuntu's certbot package installs a systemd timer (`certbot.timer`) that runs
`certbot renew` twice daily.  You can see it in the letsencrypt log as entries
like:

```
certbot._internal.display.obj: The following certificates are not due for
renewal yet:
  /etc/letsencrypt/live/hcf.squadrontrucking.com/fullchain.pem expires on
  2026-08-28 (skipped)
```

This timer is intentionally left **unable to renew** for this domain.  The
renewal configuration file
`/etc/letsencrypt/renewal/hcf.squadrontrucking.com.conf` specifies:

```ini
authenticator = manual
pref_challs = dns-01,
```

The manual DNS-01 authenticator requires a human to add a TXT record to the
domain's DNS — it cannot run unattended.  When the certificate eventually
becomes due, the timer will fail or skip rather than actually renewing.

**Do not change this to use the standalone authenticator.**  If you did, the
systemd timer would gain the ability to renew on its own schedule without:
killing Webuzo's httpd first, logging to `TDbridge_utility.log`, reporting
to the Manager Dashboard, or performing any of the other checks in
`cert_renew.sh`.  The cron job owns all renewal responsibility.


### Installing the cron job

```bash
sudo crontab -e
```

Add this line:
```
0 3 * * * /home/graeme/TDbridge/cert_renew.sh
```

Verify:
```bash
sudo crontab -l
```

The script runs as root (via `sudo crontab`) because certbot requires root
to write to `/etc/letsencrypt/` and `systemctl restart stunnel4` requires root.

### Why daily rather than monthly?

Running daily means:
- The Manager Dashboard can detect a failure within 26 hours
- Port 80 availability is checked every day — if Webuzo's httpd restarts
  unexpectedly you find out the next morning rather than 30 days later when
  renewal actually fails
- certbot dry runs use the staging environment and have no rate limit impact

### Rate limits

Let's Encrypt enforces rate limits only on **production certificate issuance**:
- 5 duplicate certificates per week for the same domain
- 50 certificates per domain per week

The daily dry run uses the staging environment and does not count toward
these limits.  Real renewals only happen when the certificate has 30 or fewer
days remaining, which means at most once every ~60 days in normal operation —
well within the limits.

---

## Manual renewal procedure (fallback)

If the automatic renewal fails and you need to renew manually:

```bash
# 1. Verify port 80 is free
sudo ss -tlnp | grep ':80\b'

# 2. Run certbot
sudo certbot certonly --standalone -d hcf.squadrontrucking.com

# 3. Fix permissions
sudo chmod 640 /etc/letsencrypt/archive/hcf.squadrontrucking.com/*.pem

# 4. Restart stunnel
sudo systemctl restart stunnel4

# 5. Verify stunnel is healthy
sudo systemctl status stunnel4
sudo journalctl -u stunnel4 -n 5
```

---

## Troubleshooting

### Port 80 is occupied

```bash
sudo ss -tlnp | grep ':80\b'
sudo ps aux | grep httpd | grep -v grep
```

If `httpd` has restarted:
```bash
sudo systemctl stop httpd.service
```

Since `httpd.service` is disabled, this stop should persist across reboots.
If it keeps restarting, check whether `webuzo.service` has a dependency that
relaunches it:
```bash
sudo systemctl cat webuzo.service
```

### stunnel is not forwarding traffic

Check the stunnel log for connection errors:
```bash
sudo journalctl -u stunnel4 -n 30
```

Common issues:
- `Connection refused` on the connect port → the bot process is not running
- `No such file or directory` for cert/key → certificate files moved or permissions wrong
- `TLSV1_ALERT_UNKNOWN_CA` in Telegram's error → stunnel is using `cert.pem` instead of `fullchain.pem`

### Checking the current certificate

```bash
# Expiry date
openssl x509 -noout -enddate -in /etc/letsencrypt/live/hcf.squadrontrucking.com/fullchain.pem

# How many certificates are in fullchain.pem (should be 3: leaf + 2 intermediates)
openssl crl2pkcs7 -nocrl -certfile /etc/letsencrypt/live/hcf.squadrontrucking.com/fullchain.pem \
  | openssl pkcs7 -print_certs -noout | grep subject

# What Telegram actually sees (connects from outside, checks the presented chain)
openssl s_client -connect hcf.squadrontrucking.com:8443 -servername hcf.squadrontrucking.com \
  </dev/null 2>/dev/null | openssl x509 -noout -enddate
```
