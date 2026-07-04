# TDbridge Gateway Integration Guide

*A practical guide for a partner programmer integrating an existing Telegram bot
with the TDbridge Gateway.*

> Copyright (c) 2026 Squadron Trucking. Released under the MIT License. The
> copyright notice and permission notice shall be included in all copies or
> substantial portions. For the full license text, see `LICENSE_TDbridge.md`.

This guide is a companion to **`TDbridge_Gateway_Protocol.md`**, which is the
formal wire specification. This document does not restate the wire format in
full; it references the spec by section (for example, "§6.1") for exact request
and response shapes. Read this guide to decide *how* to integrate and to see
worked examples; consult the spec for the authoritative field-by-field detail.

The running examples are in Python. If your bot is written in another language,
the concepts and the HTTP calls are identical — only the syntax differs — and a
language-specific version of this guide (with a complete example client in that
language) can be produced on request.

---

## 1. The problem this solves

You operate a Telegram bot — call it the dispatch bot — that already posts
messages into Telegram groups. A typical message looks like:

```
Hours of service alert: Angel Anthony Espino | Less than 30 minutes remaining
on Duty | Rule: Duty | Duty Stage: OFF | Break: 4:57 | Drive: 0:23 |
Shift: 0:23 | Cycle: 55:11 | Squadron
```

Those messages appear correctly in Telegram. But they are **not** reaching the
Discord side of the bridge, and replies made on Discord are not coming back to
you. The cause is a hard limitation of the Telegram Bot API:

> **A Telegram bot never receives messages authored by another bot.**

TDbridge's bot and your dispatch bot are both members of the same Telegram
group, but neither can see the other's messages. So when your bot posts an
hours-of-service alert, TDbridge's bot is blind to it and cannot bridge it to
Discord. The reverse is also true: TDbridge's own bot-authored messages are
invisible to your bot.

The **Gateway** is an authenticated HTTPS side channel that carries these
messages directly between the two systems, bypassing the bot-to-bot blindness
entirely. This guide is about connecting your bot to it.

---

## 2. Your integration options

There are several ways to close the gap, and they differ mainly in **how much
of your bot's core function you move onto TDbridge**. That is a real operational
decision, not just a technical one: the more you offload, the more your service
depends on TDbridge being up. The options below are ordered by *increasing*
reliance on TDbridge.

Throughout, "the server" means the TDbridge instance that owns the gateway
(it holds the public HTTPS endpoint and the outbound queue). "The client" means
your dispatch bot acting as a gateway connector. See spec §1 ("Roles").

### Option A — Convert your bot to a Telegram *userbot*

Instead of a Bot API bot, run as a **userbot** (a normal Telegram *user*
account, driven programmatically via a library such as Telethon or Pyrogram).
A userbot's messages are authored by a *user*, not a bot — so TDbridge's bot
**can** see them, and they bridge to Discord with no gateway involvement at all.
Replies and reactions on Discord bridge back to Telegram normally, and your
userbot sees them like any user would.

- **Pro:** No gateway integration whatsoever. Zero dependence on TDbridge for
  your core "post to Telegram" function — if TDbridge is down, your messages
  still post; only the bridging pauses. Full Telegram feature access.
- **Con:** Userbots are a different Telegram API (MTProto, not the Bot API), so
  this is a substantial rewrite of how your bot authenticates and sends. Running
  automation on a user account also carries Telegram terms-of-service
  considerations you should evaluate. You gain nothing bridging-specific from
  TDbridge here — you simply stop being invisible.

This option is included for completeness. It sidesteps the gateway rather than
using it, and it is usually only attractive if you were considering a userbot
for other reasons.

### Option B — Add a gateway *client* to your bot (recommended default)

Keep your bot exactly as it is for posting to Telegram. **Additionally**, make
it a gateway client: for each message you want bridged, send a copy over the
gateway (with the real Telegram `message_id` your bot already obtained when it
posted), and slow-poll the gateway to receive replies, reactions, edits, and
deletions from the Discord side.

This is the **`Echo = false`** mode (spec §9): the server does *not* re-post to
Telegram — your bot already did that — the gateway is used purely to convey the
bridged copy and to carry responses back.

- **Pro:** Your core function (posting alerts to Telegram) stays entirely inside
  your own bot and does not depend on TDbridge at all. You choose exactly which
  messages to bridge (e.g. bridge alerts, but not internal chatter). If TDbridge
  is down, your alerts still post to Telegram; you lose only the bridging and
  the inbound replies until it recovers.
- **Con:** You maintain two send paths for bridged messages (post to Telegram,
  then also send to the gateway) and a poll loop. There is a subtlety with
  `Echo=false` and correlation (see §7 below and spec §7): operations the
  Discord side initiates against your message rely on the id you supplied.

This is the option this guide is primarily written around, because it gives you
bridging while keeping your operational independence.

### Option C — Offload *sending* to the gateway (`Echo = true`)

Stop calling the Telegram Bot API to post the bridged messages yourself. Instead
send them to the gateway and let TDbridge's server post them into the Telegram
group on your behalf (**`Echo = true`**, spec §9). The server performs the
Telegram send, obtains the real `message_id`(s), and returns them to you. You
still poll for inbound responses.

- **Pro:** One send path for bridged messages. You need not be a member of the
  Telegram groups for those messages — the server posts them. Correlation is
  handled for you (the server returns the real ids).
- **Con:** Your ability to deliver those messages to Telegram now depends on
  TDbridge being up. If it is down, those messages do not post at all. You have
  moved a core function onto someone else's service.

### Option D — Offload *everything* to the gateway

As Option C, and additionally rely on the gateway to receive **all** group
activity — including messages authored by ordinary Telegram *users* — via the
server's **`RelayUserMessages = true`** flag (spec §9). With this, your bot need
not be a member of the Telegram groups at all: the server becomes your eyes and
ears, relaying user messages, replies, reactions, and deletions to you over the
gateway. (You still need the set of `T_GroupID` values; the simplest way to
obtain them is to be invited to each group once so the ids arrive, after which
membership is not required — spec §9.)

- **Pro:** Simplest possible client — one inbound stream, one outbound stream,
  no direct Telegram integration for the bridged conversations at all.
- **Con:** Maximum dependence on TDbridge. If it is down you are fully dark on
  those groups — no sends, no receipt of user activity. Appropriate only where
  that coupling is acceptable.

### The recommended shape: make the level a startup parameter

The levels above are not mutually exclusive designs — they are points on a
single dial. The prudent architecture is to **build your bot so the level is
chosen by a startup parameter**, defaulting to the least-dependent mode that
meets your needs (Option B), and able to fall back further (toward Option A's
independence) if TDbridge is unavailable for an extended period.

Concretely, that means structuring the bot so that "post this message" and
"bridge this message" are separate, independently switchable actions:

```python
# Pseudocode for the dial.
OFFLOAD_LEVEL = os.environ.get("BRIDGE_OFFLOAD", "B")   # A | B | C | D

def publish(msg):
    if OFFLOAD_LEVEL in ("A",):
        post_to_telegram_natively(msg)          # userbot: bridging is automatic
    elif OFFLOAD_LEVEL in ("B",):
        tg_ids = post_to_telegram_via_bot(msg)  # you post
        gateway_send(msg, message_ids=tg_ids, echo=False)   # and bridge a copy
    elif OFFLOAD_LEVEL in ("C", "D"):
        gateway_send(msg, echo=True)            # server posts on your behalf
```

The value of the dial is operational resilience: if TDbridge has an outage, an
operator can drop the bot to a lower level (or to native-only posting) with a
restart, claw back the core function, and restore bridging later — without a
code change. TDbridge itself is built on exactly this principle: it can run as a
server, as a client, or both, selected by configuration.

> **Whichever level you choose, the `Echo` and `RelayUserMessages` flags are set
> on the *server* side** (they are per-gateway server configuration — spec §10).
> Coordinate the intended level with the gateway operator (Graeme) so the server
> is configured to match. Your client behavior and the server flags must agree:
> e.g. Option B requires `Echo=false`; Option C requires `Echo=true`.

---

## 3. What the server does for you (brief)

You do not implement the server; TDbridge is the server. But understanding its
obligations clarifies what you can rely on (full detail in the spec):

- **Holds the public HTTPS endpoint** rooted at a base URL such as
  `https://hcf.squadrontrucking.com:8443/gateway` (spec §3). Your client makes
  only *outbound* requests to it, so your bot can run behind NAT.
- **Queues outbound events** (replies, reactions, edits, deletions destined for
  you) and hands them to you when you poll. The queue is persisted, so it
  survives a server restart (spec §8).
- **Performs Telegram sends on your behalf** when `Echo=true`, returning the real
  message id(s) — including all N ids for a media group (spec §6.3, §7).
- **Applies attachments, reactions, edits, and deletions** to the real Telegram
  messages and bridges them to Discord (and, for gateway-origin messages, relays
  responses back to you).
- **Prevents its own loops** by dropping messages its own bot authored (spec
  §12). You are responsible for the symmetric discipline on your side — see §8.

Authentication is a shared secret sent on every request; the server verifies it
and omits it from responses (spec §4). TLS is standard HTTPS.

---

## 4. The five endpoints

All requests are `POST` with a JSON body carrying the envelope (spec §4), except
the final file fetch which is a plain `GET`. Full request/response shapes are in
spec §3–§8; here is the map.

| Endpoint | You call it to… |
|---|---|
| `POST /gateway/send` | Submit a `message`, `edited_message`, `reaction`, or `deletion`. |
| `POST /gateway/poll` | Long-poll (≈10 s) for events queued *for you*. |
| `POST /gateway/ack` | Acknowledge events you received (only if `RequireACK` is on). |
| `POST /gateway/upload` | Upload one attachment's bytes; get back a `file_ref`. |
| `POST /gateway/getfile` | Exchange a `file_ref` for a short-lived download URL. |

The envelope on everything you send:

```python
def envelope(event_type, payload):
    return {
        "protocol_version": 1,
        "gateway": GATEWAY_NAME,      # e.g. "TDbridge_gw" — agreed with the operator
        "secret": GATEWAY_SECRET,     # the shared secret
        "event_type": event_type,     # message | edited_message | reaction | deletion | ack
        "payload": payload,
    }
```

---

## 5. Sending a message (the core of Option B)

Your bot has just posted an alert to Telegram and holds the real `message_id`
Telegram returned. To bridge it, send a `message` event with that id. Because
this is `Echo=false`, you supply the id and the server does **not** re-post.

```python
import requests

BASE = "https://hcf.squadrontrucking.com:8443/gateway"
GATEWAY_NAME = "SquadronDispatchBot_gw"
GATEWAY_SECRET = "…"   # provided by the operator; keep it out of source control

def gateway_send_message(tg_group_id, tg_message_id, text, reply_to=None):
    payload = {
        "chat": {"id": tg_group_id},          # the T_GroupID — the sole routing key
        "message_id": tg_message_id,          # the REAL Telegram id your bot already has
        "from": {"first_name": "Squadron", "is_synthetic": True},  # informational only
        "text": text,
        "reply_to": reply_to,                 # a message_id, or None
        "attachments": [],
    }
    r = requests.post(f"{BASE}/send",
                      json=envelope("message", payload),
                      timeout=180)
    r.raise_for_status()
    return r.json()
```

Key points, each backed by the spec:

- **`chat.id` is the only thing that routes** (spec §5.1). It is the Telegram
  `T_GroupID`. The receiver decides which conversation the message belongs to
  entirely from this. Get it right; everything else about routing follows.
- **`from` is non-authoritative** (spec §5.1). It is shown for attribution only
  and must never be used for routing. `is_synthetic: true` signals the identity
  was synthesized. For your alerts, a fixed sender name like "Squadron" is fine.
- **`message_id` is the correlation key** (spec §7). Under `Echo=false` it is
  *your* id and must be the real Telegram message id, so that a reply or reaction
  the operator's side makes against it lands on the right Telegram message.
- **Use a generous timeout.** The server may do real work inline (especially
  with attachments); 180 s is a safe client-side timeout. This matters more than
  it looks — see §9.

### Under `Echo = true` (Options C/D)

Omit `message_id` (the server assigns it) and read it back from the response:

```python
resp = gateway_send_message_echo(tg_group_id, text)   # no message_id supplied
tg_ids = resp["message_ids"]      # list; for a media group, ALL member ids
```

Record **every** id in `message_ids` (spec §7). For a media group there are N
of them, and you will need all N to correlate later reactions, edits, and
deletions.

---

## 6. Polling for inbound events

Run a loop that long-polls. The server holds each request up to ~10 s and
returns an array of events (empty at timeout). Poll again immediately.

```python
def poll_loop(handle_event):
    while True:
        try:
            r = requests.post(f"{BASE}/poll",
                              json=envelope("poll", {}),   # payload unused for poll
                              timeout=30)                  # > server's 10s hold
            r.raise_for_status()
            for ev in r.json().get("events", []):
                handle_event(ev)
        except requests.RequestException as e:
            log.warning("poll failed: %s (retrying)", e)
            time.sleep(1)     # brief backoff; then poll again
```

- The client-side timeout (30 s) must exceed the server's hold (10 s), or every
  poll will look like a timeout.
- **Occasional poll timeouts are normal**, not errors. Network hiccups and
  concurrent polling produce transient failures; log them and retry. Do not
  treat a single failed poll as an outage.
- Events arrive as full envelopes: dispatch on `ev["event_type"]`.

```python
def handle_event(ev):
    et = ev["event_type"]
    p = ev["payload"]
    if et in ("message", "edited_message"):
        on_message(p, edited=(et == "edited_message"))
    elif et == "reaction":
        on_reaction(p)          # p["message_id"], p["emoji"], p["from"]
    elif et == "deletion":
        on_deletion(p)          # p["message_ids"]  (a LIST)
```

---

## 7. Acknowledgement (`RequireACK`)

If the gateway is configured with `RequireACK = true` (spec §8), the server
keeps each event queued until you acknowledge it — so a poll response lost in
transit is redelivered rather than dropped. **You must ack, or the same event
is redelivered indefinitely** (this is a common first-integration bug: process
the event, forget to ack, and watch it loop).

Ack by the ids you received:

```python
def ack(chat_id, message_ids):
    requests.post(f"{BASE}/ack",
                  json=envelope("ack", {"chat": {"id": chat_id},
                                        "message_ids": message_ids}),
                  timeout=30)
```

Call it after you have durably handled the event. If `RequireACK` is false, an
ack you send is harmlessly ignored, so acking unconditionally is safe.

---

## 8. Loop prevention — your responsibility

The server drops messages authored by its own bot, so it will not re-bridge its
own writes (spec §12). You must apply the **symmetric discipline**:

- **Do not re-relay an event you received *from* the gateway.** If you receive a
  reply over the gateway and post it somewhere, do not then send that same event
  *back* to the gateway. Likewise, **a deletion you receive from the gateway must
  not be re-sent to the gateway** as if it were a new deletion you originated.
  Track which actions were locally originated versus gateway-originated, and only
  relay the locally-originated ones. (TDbridge learned this the hard way: without
  it, a single deletion bounces back and forth as an echo.)
- **Deduplicate by (chat.id, message_id).** If you receive a `message` whose id
  you already have (for instance, your own message echoed back to you), skip it
  rather than posting a duplicate.

These are cheap, stateless-ish checks and they prevent the most common
multi-party bridging pathologies.

---

## 9. Attachments

Attachments are transferred by reference, not inlined (spec §6): upload bytes,
get a `file_ref`, cite the `file_ref` in the message. Receiving is the mirror:
exchange the `file_ref` for a short-lived URL, then `GET` the bytes.

### Sending attachments

```python
def upload(file_bytes, file_name, mime_type):
    r = requests.post(f"{BASE}/upload",
                      data=file_bytes,
                      headers={
                          "X-File-Name": file_name,
                          "X-Mime-Type": mime_type,
                          "Content-Type": "application/octet-stream",
                          # plus your auth header / secret as the operator specifies
                      },
                      timeout=180)
    r.raise_for_status()
    return r.json()      # {file_ref, file_name, mime_type, size}

# Then reference the uploaded files in the message payload:
refs = [upload(b1, "IMG_01.jpg", "image/jpeg"),
        upload(b2, "IMG_02.jpg", "image/jpeg")]
payload["attachments"] = [
    {"file_ref": r["file_ref"], "file_name": r["file_name"],
     "mime_type": r["mime_type"], "size": r["size"]}
    for r in refs
]
```

### Receiving attachments

```python
def fetch_attachment(att):
    # 1) exchange the reference for a URL
    r = requests.post(f"{BASE}/getfile",
                      json=envelope("getfile", {"file_ref": att["file_ref"]}),
                      timeout=30)
    r.raise_for_status()
    url = r.json()["download_url"]
    # 2) GET the bytes (no secret needed on the URL)
    return requests.get(url, timeout=120).content
```

Operational realities we learned building and testing this, worth building in
from the start:

- **A `getfile`/download can 404.** The server only retains attachment bytes for
  a limited window (on the order of 24 hours). If a client is offline past that
  window, the reference expires. **Wrap each attachment fetch in its own
  try/except** and continue with the rest of the message rather than failing the
  whole message. Treat a missing attachment as "unavailable," not fatal.
- **Multiple attachments are a media group** (spec §6.3): N messages sharing a
  `media_group_id`, each with its own `message_id`. Under `Echo=true` the server
  returns **all N** ids — record them all. A reply targets one member's id; a
  **deletion of the logical message lists all member ids** in `message_ids`.
- **Delete the logical message only when the last member is gone.** If you track
  per-member ids, a deletion event may list all of them at once, or members may
  be deleted one at a time; delete your local representation when no members
  remain, not on the first. (This mirrors how TDbridge itself handles media-group
  deletion.)
- **Size limits differ per platform and are applied independently.** A file may
  be small enough for one destination and too large for another; each side
  applies its own limit to the same uploaded copy. Expect that an attachment may
  reach some destinations and not others, with a note explaining the skip, rather
  than all-or-nothing.
- **Give uploads and attachment-bearing sends a long timeout.** The first send
  over a freshly opened connection can be slow (a cold TLS handshake has been
  observed to take tens of seconds in this deployment). Too short a client
  timeout can cause a timeout-then-retry that posts the message *twice*. Use
  generous timeouts (≈180 s) on `send` and `upload`, exactly as the reference
  client does.

---

## 10. Reactions, edits, and deletions

These are ordinary events you both send and receive.

```python
# Send a reaction to a message (identified by its Telegram id).
def gateway_send_reaction(tg_group_id, target_message_id, emoji_list, who):
    payload = {
        "chat": {"id": tg_group_id},
        "message_id": target_message_id,
        "from": {"first_name": who, "is_synthetic": True},  # for attribution
        "emoji": emoji_list,       # e.g. ["👍"]; [] clears reactions
    }
    requests.post(f"{BASE}/send", json=envelope("reaction", payload), timeout=30)

# Send an edit: same shape as a message, new text, existing message_id.
def gateway_send_edit(tg_group_id, message_id, new_text):
    payload = {"chat": {"id": tg_group_id}, "message_id": message_id,
               "text": new_text, "reply_to": None, "attachments": []}
    requests.post(f"{BASE}/send", json=envelope("edited_message", payload), timeout=180)

# Send a deletion: one or more ids (all members, for a media group).
def gateway_send_deletion(tg_group_id, message_ids):
    payload = {"chat": {"id": tg_group_id}, "message_ids": message_ids}
    requests.post(f"{BASE}/send", json=envelope("deletion", payload), timeout=30)
```

- Include a `from.first_name` on reactions you originate so the other side can
  attribute them; without it, attribution falls back to the gateway name.
- Telegram accepts only a fixed set of emoji as *native* reactions. If you send
  one outside that set, the far side may relay it as a note rather than a native
  reaction. That is expected graceful degradation, not an error.
- Remember the loop discipline (§8): do not echo back a reaction/edit/deletion
  you *received* from the gateway.

---

## 11. A minimal integration checklist

1. Obtain from the operator: the base URL, the `gateway` name, the shared
   `secret`, and the agreed **offload level** (which fixes the server's `Echo`
   and `RelayUserMessages` flags, and whether `RequireACK` is on).
2. Implement the envelope helper and the shared-secret auth.
3. Implement `send` for `message` (with the real `message_id` if `Echo=false`).
4. Implement the poll loop with a client timeout > 10 s; log-and-retry on
   failure; dispatch by `event_type`.
5. If `RequireACK`: implement `ack` and call it after handling each event.
6. Implement attachment upload/getfile with per-attachment try/except and long
   timeouts.
7. Apply loop discipline: dedupe by (chat.id, message_id); never re-relay a
   received event.
8. Handle media-group id sets: record all ids; delete only when the last member
   is gone.
9. Make the offload level a startup parameter so you can fall back if TDbridge
   is unavailable.

---

## 12. Appendix — complete example client (Python)

The following is a small but complete gateway client illustrating Option B
(`Echo=false`): it sends bridged messages your bot has already posted to
Telegram, and it polls for inbound replies/reactions/edits/deletions. It uses
only the `requests` library. It is intentionally single-file and dependency-light
so it can be read end to end; adapt logging, persistence, and error handling to
your environment.

```python
#!/usr/bin/env python3
"""
Example TDbridge gateway client (Option B, Echo=false).

Bridges messages your dispatch bot has ALREADY posted to Telegram, and receives
Discord-side responses (replies, reactions, edits, deletions) over the gateway.

Configuration via environment:
  GATEWAY_BASE     e.g. https://hcf.squadrontrucking.com:8443/gateway
  GATEWAY_NAME     e.g. SquadronDispatchBot_gw
  GATEWAY_SECRET   shared secret (keep out of source control)
  GATEWAY_ACK      "1" if the gateway uses RequireACK, else "0"
"""

import os
import time
import logging
import threading
import requests

log = logging.getLogger("gw_client")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

BASE   = os.environ["GATEWAY_BASE"].rstrip("/")
NAME   = os.environ["GATEWAY_NAME"]
SECRET = os.environ["GATEWAY_SECRET"]
USE_ACK = os.environ.get("GATEWAY_ACK", "0") == "1"

SEND_TIMEOUT = 180     # generous: server may do work inline; cold TLS is slow
POLL_TIMEOUT = 30      # must exceed the server's ~10s hold
GETFILE_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 120

# Track ids we have SEEN inbound, so we never re-relay or duplicate them.
_seen_inbound = set()
_seen_lock = threading.Lock()


def envelope(event_type, payload):
    return {
        "protocol_version": 1,
        "gateway": NAME,
        "secret": SECRET,
        "event_type": event_type,
        "payload": payload,
    }


# --------------------------------------------------------------------------- #
# Outbound (your bot -> gateway)
# --------------------------------------------------------------------------- #

def send_message(tg_group_id, tg_message_id, text,
                 reply_to=None, attachments=None, sender_name="Squadron"):
    """Bridge a message your bot already posted to Telegram (Echo=false)."""
    payload = {
        "chat": {"id": tg_group_id},
        "message_id": tg_message_id,
        "from": {"first_name": sender_name, "is_synthetic": True},
        "text": text,
        "reply_to": reply_to,
        "attachments": attachments or [],
    }
    r = requests.post(f"{BASE}/send", json=envelope("message", payload),
                      timeout=SEND_TIMEOUT)
    r.raise_for_status()
    return r.json()


def send_reaction(tg_group_id, target_message_id, emoji_list, sender_name="Squadron"):
    payload = {
        "chat": {"id": tg_group_id},
        "message_id": target_message_id,
        "from": {"first_name": sender_name, "is_synthetic": True},
        "emoji": list(emoji_list),
    }
    requests.post(f"{BASE}/send", json=envelope("reaction", payload),
                  timeout=GETFILE_TIMEOUT).raise_for_status()


def send_edit(tg_group_id, message_id, new_text):
    payload = {"chat": {"id": tg_group_id}, "message_id": message_id,
               "text": new_text, "reply_to": None, "attachments": []}
    requests.post(f"{BASE}/send", json=envelope("edited_message", payload),
                  timeout=SEND_TIMEOUT).raise_for_status()


def send_deletion(tg_group_id, message_ids):
    payload = {"chat": {"id": tg_group_id}, "message_ids": list(message_ids)}
    requests.post(f"{BASE}/send", json=envelope("deletion", payload),
                  timeout=GETFILE_TIMEOUT).raise_for_status()


def upload(file_bytes, file_name, mime_type):
    """Upload one attachment; returns {file_ref, file_name, mime_type, size}."""
    headers = {
        "X-File-Name": file_name,
        "X-Mime-Type": mime_type,
        "Content-Type": "application/octet-stream",
        "X-Gateway": NAME,
        "X-Secret": SECRET,     # exact auth header per operator; adjust as needed
    }
    r = requests.post(f"{BASE}/upload", data=file_bytes, headers=headers,
                      timeout=SEND_TIMEOUT)
    r.raise_for_status()
    return r.json()


# --------------------------------------------------------------------------- #
# Inbound attachments (gateway -> your bot)
# --------------------------------------------------------------------------- #

def fetch_attachment(att):
    """Exchange a file_ref for a URL and download the bytes.
    Returns bytes, or None if unavailable (e.g. expired). Never raises upward."""
    try:
        r = requests.post(f"{BASE}/getfile",
                          json=envelope("getfile", {"file_ref": att["file_ref"]}),
                          timeout=GETFILE_TIMEOUT)
        r.raise_for_status()
        url = r.json()["download_url"]
        return requests.get(url, timeout=DOWNLOAD_TIMEOUT).content
    except Exception as e:
        log.warning("attachment %s unavailable: %s",
                    att.get("file_name", "?"), e)
        return None


# --------------------------------------------------------------------------- #
# Event handlers — replace the bodies with your own logic
# --------------------------------------------------------------------------- #

def on_message(p, edited=False):
    files = []
    for att in p.get("attachments", []):
        data = fetch_attachment(att)     # per-attachment; None if unavailable
        if data is not None:
            files.append((att.get("file_name", "attachment"), data))
    log.info("inbound %s | chat=%s msg=%s reply_to=%s text=%r attachments=%d",
             "edit" if edited else "message",
             p["chat"]["id"], p.get("message_id"), p.get("reply_to"),
             p.get("text"), len(files))
    # TODO: deliver to your system.


def on_reaction(p):
    who = (p.get("from") or {}).get("first_name") or NAME
    log.info("inbound reaction | chat=%s msg=%s emoji=%s from=%s",
             p["chat"]["id"], p.get("message_id"), p.get("emoji"), who)
    # TODO


def on_deletion(p):
    log.info("inbound deletion | chat=%s msgs=%s",
             p["chat"]["id"], p.get("message_ids"))
    # TODO: delete your local representation; if you track media-group members,
    # delete the logical message only when the last member id is gone.


def handle_event(ev):
    et = ev.get("event_type")
    p = ev.get("payload", {}) or {}
    chat = (p.get("chat") or {}).get("id")

    # Dedupe / loop-guard by (chat, message_id).
    ids = p.get("message_ids") or ([p["message_id"]] if p.get("message_id") else [])
    with _seen_lock:
        fresh = [(chat, i) for i in ids if (chat, i) not in _seen_inbound]
        for k in fresh:
            _seen_inbound.add(k)
    # (For a message we've already seen — e.g. our own echo — skip re-processing.)
    if et in ("message", "edited_message") and not fresh:
        return _maybe_ack(chat, ids)

    if et in ("message", "edited_message"):
        on_message(p, edited=(et == "edited_message"))
    elif et == "reaction":
        on_reaction(p)
    elif et == "deletion":
        on_deletion(p)
    else:
        log.info("ignoring unknown event_type=%s", et)

    _maybe_ack(chat, ids)


def _maybe_ack(chat, ids):
    if USE_ACK and chat is not None and ids:
        try:
            requests.post(f"{BASE}/ack",
                          json=envelope("ack", {"chat": {"id": chat},
                                                "message_ids": ids}),
                          timeout=GETFILE_TIMEOUT).raise_for_status()
        except Exception as e:
            log.warning("ack failed: %s", e)


# --------------------------------------------------------------------------- #
# Poll loop
# --------------------------------------------------------------------------- #

def poll_loop():
    log.info("gateway poll loop started for %s", NAME)
    while True:
        try:
            r = requests.post(f"{BASE}/poll", json=envelope("poll", {}),
                              timeout=POLL_TIMEOUT)
            r.raise_for_status()
            for ev in r.json().get("events", []):
                try:
                    handle_event(ev)
                except Exception as e:
                    log.exception("handler error: %s", e)
        except requests.RequestException as e:
            # Occasional timeouts are normal; log and retry.
            log.warning("poll failed: %s (retrying in 1s)", e)
            time.sleep(1)


if __name__ == "__main__":
    # Example: bridge one message your bot just posted (id 12345), then poll.
    # send_message(tg_group_id=-1003917181930, tg_message_id=12345,
    #              text="Hours of service alert: … | Squadron")
    poll_loop()
```

---

## 13. Where to look next

- **`TDbridge_Gateway_Protocol.md`** — the authoritative wire specification.
  Every field and status referenced here is defined there.
- The operator (Graeme) configures the server-side flags (`Echo`,
  `RequireACK`, `RelayUserMessages`) and issues the base URL, gateway name, and
  shared secret. Confirm the intended offload level with him so the server
  configuration and your client behavior agree.

*If your bot is written in a language other than Python, request a
language-specific edition of this guide and example client.*
