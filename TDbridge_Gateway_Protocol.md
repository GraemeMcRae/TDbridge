# TDbridge Gateway Protocol — Specification v1

**Status:** Implemented and validated. The interface below is in production use
between TDbridge instances (server and client roles) and has been exercised
end-to-end for messages, replies, attachments (including multi-attachment media
groups), reactions, edits, and deletions.
**License:** MIT — see `LICENSE_TDbridge.md`
**Audience:** Shareable. This document fully describes the gateway interface and
may be shared with partner organizations (e.g. Forward Relay) as the interface
contract. TDbridge-specific implementation notes are isolated in the final
section and may be stripped before sharing if desired. For a practical,
example-driven walkthrough aimed at a partner's programmer, see the companion
`TDbridge_Gateway_Integration_Guide.md`.

> Copyright (c) 2026 Squadron Trucking. Released under the MIT License. The
> copyright notice and permission notice shall be included in all copies or
> substantial portions. For the full license text, see `LICENSE_TDbridge.md`.

---

## 1. Purpose

The Gateway Protocol lets two cooperating systems exchange Telegram-group
messages over HTTPS, **without either system needing to observe the other's
Telegram bot messages.**

This solves a hard limitation of the Telegram Bot API: a bot never receives
messages authored by another bot. When two organizations each operate a Telegram
bot in the same group, neither bot can see the other's messages. The Gateway
Protocol provides a side channel — an authenticated HTTPS path — over which the
two systems exchange the messages directly, bypassing the bot-to-bot blindness
entirely.

### Applicability

The protocol is general. It suits any deployment with the following structural
mapping: a large number of participants on one platform (e.g. many users sharing
a small number of Discord channels) bridged to a large number of conversations
on the other (e.g. many Telegram groups, each representing a small number of
participants). This many-to-few / few-to-many asymmetry is the defining
characteristic; the protocol does not assume any particular industry or
participant type.

The motivating example throughout this document is trucking **dispatch** — where
each Telegram group represents one driver's conversation and a dispatch
automation system on the partner side needs to exchange messages with those
groups — but "driver," "dispatch," and similar terms are illustrative. Any use
case fitting the mapping above applies equally.

### Routing asymmetry between the two platforms

The two sides of the bridge are **not** symmetric with respect to routing, and
understanding this asymmetry is important to using the protocol correctly:

- **Telegram side — the group is the routing key.** Each Telegram `T_GroupID`
  identifies exactly one destination (the Discord user and channel to which
  messages from that group are routed). The architecture therefore requires one
  `T_GroupID` per represented participant. A message arriving from a Telegram
  group is routed solely by its `T_GroupID`.

- **Discord side — the channel of origin does NOT route.** Nothing in the
  architecture uses the Discord channel where a message originates as a routing
  input. The number of Discord channels is unconstrained — there could be one
  channel per participant, or a few channels shared by many — but the channel
  itself carries no routing meaning. A Discord message is routed to Telegram by
  explicit signals (a reply, or an @-mention/tag of a user or role), not by
  which channel it was typed in. In the absence of such a signal, a Discord
  message routes to the *author's own* Telegram group, regardless of the channel
  it appeared in.

A consequence worth flagging to operators: a Discord user may intuitively expect
that typing in a particular participant's channel will route the message to that
participant. It will not, unless the participant is tagged. Without a tag, the
message routes to the author's own group. (This behavior could be revised in a
future version — e.g. to treat the channel as a routing hint — but there are no
current plans to do so.)

### Roles

- **Server (gateway owner):** owns an HTTPS endpoint, has a stable public
  address, holds a queue of outbound events, and (optionally) performs Telegram
  sends on the client's behalf.
- **Client (gateway connector):** initiates all connections to the server. May
  run anywhere, including behind NAT, because it only makes outbound requests.
  Sends events to the server and retrieves queued events via slow polling.

A single program may act as a server for its own gateway while also acting as a
client to other gateways; the roles are independent.

### Design principle

The wire format mirrors Telegram's Bot API `Update`/`Message` shapes as closely
as practical, so a system that already parses Telegram updates can ingest
gateway events with minimal new code. The one deliberate divergence is
attachment transfer (see §6).

---

## 2. Transport and security

- **HTTPS only.** Plain HTTP is not supported. HTTPS is required to adequately
  safeguard data in transit.
- **Certificate verification is mandatory.** The client MUST verify the server's
  TLS certificate and MUST refuse to send data to a server whose certificate is
  invalid or untrusted.
- **Shared secret on every request.** Every request from the client carries a
  per-gateway secret string. The server verifies the secret before acting on the
  request or returning any data. A request with a missing or incorrect secret
  receives `401 Unauthorized` and is otherwise ignored.
- **Protocol version.** Every request and response carries a `protocol_version`
  integer (currently `1`) so the format can evolve without breaking either side.

---

## 3. Endpoints

All endpoints are rooted at the gateway's configured base URL (for example,
`https://hcf.squadrontrucking.com:8443/gateway`).

| Method & Path | Purpose |
|---|---|
| `POST /gateway/send` | Client submits an event (message, edit, reaction, deletion) to the server. |
| `POST /gateway/poll` | Client long-polls (10 s) for queued outbound events. |
| `POST /gateway/ack` | Client acknowledges receipt of one or more events (used when `RequireACK` is true). |
| `POST /gateway/upload` | Client uploads one attachment's bytes; receives a file reference. |
| `POST /gateway/getfile` | Either side exchanges a file reference for a short-lived download URL. |

A file is subsequently fetched with a plain `GET` on the returned download URL.

---

## 4. The event envelope

Every event — in either direction — is a JSON object with this envelope:

```json
{
  "protocol_version": 1,
  "gateway": "TDbridge_gw",
  "secret": "<shared-secret-string>",
  "event_type": "message",
  "payload": { ... }
}
```

- `protocol_version` — integer, currently `1`.
- `gateway` — the gateway name, matching the configuration on both sides.
- `secret` — the shared secret; verified by the server on every request.
  (On responses from server to client, the secret is omitted.)
- `event_type` — one of: `message`, `edited_message`, `reaction`, `deletion`,
  `ack`.
- `payload` — shape depends on `event_type` (see §5).

---

## 5. Event types and payloads

The payloads are modeled on Telegram Bot API objects.

### 5.1 `message`

A new message. Payload is a Telegram-`Message`-shaped object:

```json
{
  "chat": { "id": -1003917181930 },
  "message_id": 12345,
  "media_group_id": null,
  "from": {
    "id": 1217248354367701075,
    "first_name": "Driver One",
    "username": null,
    "is_synthetic": true
  },
  "date": 1781600000,
  "text": "Picking up load now.",
  "reply_to": null,
  "attachments": []
}
```

- `chat.id` — the Telegram **`T_GroupID`**. **This is the sole routing key
  within a gateway.** The receiver determines which conversation (in the dispatch
  example, which driver) the message belongs to entirely from `chat.id`, together
  with the identity of the gateway the event arrived on (the envelope's `gateway`
  field, §4). A receiver that participates in more than one gateway therefore
  keys on the pair (gateway, `chat.id`); a receiver on a single gateway can treat
  `chat.id` alone as the key. Either way, no separate synthetic routing id is
  introduced.
- `message_id` — the correlation ID for this message (see §7). For Echo=true
  server sends, this is the real Telegram message id and is assigned by the
  server. For Echo=false, it is supplied by the client.
- `media_group_id` — non-null when this message is one member of a multi-
  attachment media group (see §6.3). Members of one logical multi-attachment
  message share a `media_group_id` and each have their own `message_id`.
- `from` — a Telegram-`User`-shaped object. **Non-authoritative.** It is
  informational only and MUST NOT be used for routing. `is_synthetic: true`
  indicates the identity was synthesized (e.g. from a Discord user) and the
  `id` is not a real Telegram user id.
- `date` — Unix timestamp.
- `text` — message text (or omit/null if attachment-only; use `caption` style
  by placing the text here).
- `reply_to` — null, or the `message_id` of the message being replied to (see
  §7 for cross-gateway reply correlation).
- `attachments` — array of attachment references (see §6).

### 5.2 `edited_message`

Same payload shape as `message`, referencing the `message_id` it replaces.

### 5.3 `reaction`

Modeled on Telegram's `MessageReactionUpdated`:

```json
{
  "chat": { "id": -1003917181930 },
  "message_id": 12345,
  "from": { "id": 1217248354367701075, "first_name": "Driver One", "is_synthetic": true },
  "date": 1781600100,
  "emoji": ["❤️"]
}
```

- `message_id` — the target message being reacted to.
- `emoji` — array of reaction emoji (empty array means reactions cleared).

### 5.4 `deletion`

Telegram has no rich deletion update; this is a gateway convention:

```json
{
  "chat": { "id": -1003917181930 },
  "message_ids": [12345, 12346, 12347]
}
```

- `message_ids` — one or more message ids to delete. For a media group, lists
  all member ids.

### 5.5 `ack`

Acknowledges receipt of previously-polled events (see §8):

```json
{
  "chat": { "id": -1003917181930 },
  "message_ids": [12345, 12346]
}
```

---

## 6. Attachments (two-hop, file_id style)

Attachments follow Telegram's reference-then-download model rather than inlining
bytes. This keeps request bodies small, avoids non-standard HTTP body-size
limits, and matches the pattern a Telegram-aware system already implements.

### 6.1 Sending attachments

1. For each file, the sender calls `POST /gateway/upload` with the file's bytes
   and metadata (`file_name`, `mime_type`). The server stores the bytes
   temporarily and returns a **file reference**:

   ```json
   { "protocol_version": 1, "file_ref": "gw-file-9f3a...", "file_name": "IMG_8739.jpg", "mime_type": "image/jpeg", "size": 265216 }
   ```

2. The sender then submits the `message` (or `edited_message`) event with each
   attachment represented by its reference:

   ```json
   "attachments": [
     { "file_ref": "gw-file-9f3a...", "file_name": "IMG_8739.jpg", "mime_type": "image/jpeg", "size": 265216 },
     { "file_ref": "gw-file-7c1b...", "file_name": "IMG_8741.png", "mime_type": "image/jpeg", "size": 474112 }
   ]
   ```

### 6.2 Receiving attachments

1. For each attachment reference, the receiver calls `POST /gateway/getfile`
   with the `file_ref`. The server returns a **short-lived download URL**:

   ```json
   { "protocol_version": 1, "file_ref": "gw-file-9f3a...", "download_url": "https://.../gwfile/9f3a...?t=...", "expires": 1781600400 }
   ```

2. The receiver fetches the bytes with a plain `GET` on `download_url`.

The two-hop approach (reference → URL → bytes) mirrors Telegram's
`getFile`-then-download flow and allows a failed download to be retried without
re-requesting the reference.

### 6.3 Multiple attachments = media group

Telegram has no single "message with N attachments" object. Multiple attachments
are a **media group**: N separate messages sharing a `media_group_id`, each with
its own `message_id`, displayed grouped by Telegram clients.

Consequently:

- A logical multi-attachment message is represented on the wire as a media
  group: a set of related `message` payloads sharing one `media_group_id`.
- When the server sends such a message to Telegram on a client's behalf
  (Echo=true), Telegram produces **N message ids**. The server returns **all N**
  ids to the client (see §7), not just one.
- Reactions, replies, and deletions correlate against the appropriate member:
  a reply to any member references that member's id; a deletion of the logical
  message lists all member ids.

Limits: attachments may be as large as the platform maximums (Discord/Telegram
limits apply, on the order of 25–50 MB). Because bytes travel via dedicated
upload/download requests rather than inside event JSON, ordinary HTTP body-size
defaults are sufficient.

---

## 7. Message-id correlation

Correlation links later events (replies, reactions, deletions) to the original
message. The correlation key is the pair **(`chat.id`, `message_id`)** — the
same identifiers Telegram itself uses.

- **Echo = true:** When the client sends a message, the server performs the
  Telegram send and assigns the real Telegram `message_id`(s). The server's
  response to `POST /gateway/send` returns them:

  ```json
  {
    "protocol_version": 1,
    "chat": { "id": -1003917181930 },
    "message_ids": [12345]
  }
  ```

  For a media group, `message_ids` contains all member ids. The client records
  these and uses them to correlate subsequent events.

- **Echo = false:** The server does not send to Telegram. The client supplies
  its own `message_id` in the payload and uses it as the correlation key.
  (Note: if a client-supplied id corresponds to a Telegram message the server's
  bot cannot see — because it was authored by another bot — Telegram may not
  honor operations against it. This is an accepted limitation of Echo=false.)

The envelope/event `message_id` and the Telegram `message_id` are the **same
value**; the protocol does not introduce a separate synthetic gateway id.

---

## 8. Slow polling and acknowledgement

The client retrieves queued outbound events by long-polling:

- `POST /gateway/poll` with the envelope's identifying fields (gateway, secret,
  protocol_version). The server holds the request open up to **10 seconds**. If
  events are queued (or arrive during the wait), it responds immediately with an
  array of event envelopes; otherwise it responds with an empty array at
  timeout. The client immediately polls again.

  ```json
  { "protocol_version": 1, "events": [ { "event_type": "reaction", "payload": { ... } }, ... ] }
  ```

- **RequireACK = false:** The server dequeues an event as soon as it is sent in a
  poll response. If an `ack` is nonetheless received, it is silently discarded.
- **RequireACK = true:** The server retains the event after sending it in a poll
  response, and only dequeues it upon receiving a matching `POST /gateway/ack`.
  This guards against events lost if a poll response fails to reach the client.

Queued events are persisted (so they survive a server restart). Health reporting
includes the count of undelivered / undeliverable events.

---

## 9. The `Echo` and message-relay flags

Per-gateway behavior flags (configured on the server side — see §10):

- **`Echo`** — controls whether the server, upon receiving a `message` event
  from the client, also **sends that message into the real Telegram group**.
  - `Echo = true`: the server sends to Telegram, obtains real message id(s), and
    returns them (§7). Used when the client is not itself a member of the
    Telegram group and relies on the server to post on its behalf.
  - `Echo = false`: the server does not send to Telegram; the client is
    responsible for placing its own message in the group, and supplies its own
    message id. Used when the client (e.g. a dispatch bot) posts its own,
    possibly different, message to Telegram and uses the gateway only to convey
    the version destined for bridging.

- **`RequireACK`** — see §8.

- **`RelayUserMessages`** — controls whether the server forwards, over the
  gateway, messages authored by **ordinary Telegram users** in the group (not
  just messages the server itself originated from the Discord side).
  - `true`: the gateway carries all group activity, including genuine Telegram
    users' messages. This allows a client (e.g. a dispatch system) to receive
    all group activity **without being a member of the Telegram groups at all** —
    the server becomes its eyes and ears. (The client still needs the set of
    `T_GroupID` values; the easiest way to learn them is to be invited to each
    group once so the ids arrive automatically, after which membership is no
    longer required.)
  - `false`: the gateway carries only messages the server originated.

---

## 10. Gateway configuration (JSON)

Gateways are listed in a JSON file referenced by configuration (for example,
`telegram_gateways.json`). Each entry:

```json
{
  "gateways": [
    {
      "name": "TDbridge_gw",
      "url": "https://hcf.squadrontrucking.com:8443/gateway",
      "secret": "<shared-secret-string>",
      "role": "server",
      "echo": true,
      "require_ack": true,
      "relay_user_messages": true
    },
    {
      "name": "PartnerB_gw",
      "url": "https://partner.example.com:8443/gateway",
      "secret": "<shared-secret-string>",
      "role": "client",
      "echo": false,
      "require_ack": false,
      "relay_user_messages": false
    }
  ]
}
```

- `name` — gateway name; matches the `gateway` field in envelopes and the
  `T_Gateway` column value in the routing tables.
- `url` — base URL of the gateway endpoint (server's stable address).
- `secret` — shared secret for authentication.
- `role` — `server` (this instance owns/hosts this gateway) or `client` (this
  instance connects to a remote gateway).
- `echo`, `require_ack`, `relay_user_messages` — behavior flags (§8, §9). These
  are meaningful to the gateway **owner** (server).

A separate configuration value names **this instance's own gateway**, e.g.
`TELEGRAM_OWN_GATEWAY="TDbridge_gw"`. An instance that owns no gateway sets this
to the empty string and acts purely as a client.

---

## 11. Routing-table additions

Two routing tables gain a `T_Gateway` column.

- **`D_User`** gains `T_Gateway`. For a given Discord user, the
  (`T_GroupID`, `T_Gateway`) pair specifies the destination group **and** the
  path to it. A blank `T_Gateway` means the group is reached **natively** (this
  instance is itself in the Telegram group). A non-blank value names the gateway
  through which the group is reached.

- **`T_Group`** gains `T_Gateway`. The unique key becomes
  (`T_GroupID`, `T_Gateway`). The same real Telegram `T_GroupID` may appear with
  different gateways (or with none), reflecting that multiple bots may be members
  of one Telegram group. On duplicate (`T_GroupID`, `T_Gateway`) combinations,
  the **first** row in the table wins.

### Validation

When validating a `T_GroupID` for a message associated with gateway `G`, the
group must be **Active with the same gateway** `G`. That is, a `D_User` row
naming (`T_GroupID`, `G`) requires a matching Active `T_Group` row for
(`T_GroupID`, `G`). A `T_GroupID` arriving over a gateway that matches no group
reachable through that gateway is treated as **completely unroutable** (the
existing unroutable-message rule applies).

The active gateway set for an instance is determined at runtime from the
distinct `T_Gateway` values among active `D_User` rows.

---

## 12. Loop prevention

When a message arrives from the Telegram side and the **sender is this
instance's own bot account**, the message is **not bridged**. This single
identity check (received sender id equals own bot id → drop) prevents the
instance from re-bridging its own outbound writes, including Echo-originated
messages. It is stateless and requires no message-id bookkeeping.

---

## 13. Health reporting

- The **server's own gateway** is critical: if it is not functioning, the
  periodic health check reports a failure.
- Health reporting includes the count of undelivered / undeliverable queued
  events.
- Client-side gateway health is reported but is lower-severity (relevant chiefly
  during testing).

---

## 14. TDbridge implementation notes (may be stripped before external sharing)

These notes are specific to the TDbridge implementation and are not part of the
interface contract.

- **Two scenarios exercise the same machinery from opposite ends:**
  - *Testing:* TDbridge `--env test` (laptop) runs purely as a **client**,
    `TELEGRAM_OWN_GATEWAY=""`, impersonating a partner dispatch bot. It POSTs
    messages (with attachments, drawn from real Discord channels) to the prod
    gateway and slow-polls for responses. TDbridge `--env prod` (hcf) runs purely
    as a **server** in this scenario.
  - *Production (if adopted):* TDbridge `--env prod` runs purely as a **server**
    and is never a client. It performs normal Discord↔Telegram routing and, in
    addition, sends those messages out its own gateway to the partner dispatch
    bot (which is blind to TDbridge's bot-authored Telegram messages). Genuine
    Telegram-user activity is likewise relayed out the gateway when
    `relay_user_messages` is true.
- **Multi-attachment correlation** uses the TDbridge mechanism that stores all
  Telegram message ids belonging to one logical (Discord) message. Throughout
  the codebase, a single logical multi-attachment message corresponds to a
  Telegram media group — multiple Telegram messages sharing one
  `media_group_id`, each with its own message id — and TDbridge associates the
  full set of those ids with the one logical message so that replies, reactions,
  and deletions resolve correctly across all members. The gateway media-group id
  set (§6.3) maps directly onto this store.
- **Configuration parameters** follow the existing `.env` convention with
  `TEST_`/`PROD_` prefixes, e.g. `PROD_TELEGRAM_OWN_GATEWAY="TDbridge_gw"` and
  `PROD_TELEGRAM_GATEWAYS=telegram_gateways.json`.
- **Queue persistence** uses SQLite, consistent with the message-id store and the
  T_Group write-behind buffer.
- **TLS termination** reuses the existing stunnel pattern on the server side; the
  gateway endpoint can sit behind stunnel like the webhook endpoint.

---

## License

Copyright (c) 2026 Squadron Trucking.

This document is released under the MIT License. The above copyright notice and
the permission notice shall be included in all copies or substantial portions of
this document. For the full license text, see `LICENSE_TDbridge.md` in the
repository.
