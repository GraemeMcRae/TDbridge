# Bridge — Foundational Design Document (Companion to the frozen TDbridge codebase)

*Handoff artifact for a clean-slate rewrite. This is a **companion** to the frozen
TDbridge + userbot source, which will be in the new project's files with every
filename prefixed `TD-` (e.g. `TD-bot.py`, `TD-userbot_bridge.py`,
`TD-TDbridge_Scope_v1.0.docx`). Read those as the reference implementation and
behavioral spec. This document says what Bridge should BE, defines every concept
it uses, and points into the `TD-` files for concrete logic to adapt.*

**How to use this doc.** Pointers into the reference are written by *behavior*
("the handler that does X"), not by remembered symbol names, since names may have
drifted in the frozen version. When a pointer says "see the TD- code that does X,"
open the `TD-` file, find the code whose behavior matches, and confirm by behavior
before adapting. Where this doc gives an explicit spec, that spec is authoritative
for Bridge even if the TD- code differs — the TD- code carries baggage Bridge
discards (§8).

---

## 0. What Bridge is (and is not)

**Bridge is a single process that bidirectionally mirrors Discord channels and
Telegram groups, using a real Telegram user account (a "userbot") via Telethon for
the Telegram side and discord.py for the Discord side. One process owns both
clients.**

The defining simplification: because the Telegram side is a **real user account**,
the Telegram "bots cannot see each other's messages/reactions" limitation — the
entire reason the reference grew its gateway, client/server, and userbot-relay
layers — **does not exist for Bridge.** The userbot sees everything a human in the
group sees. That one fact lets Bridge delete almost all of the accumulated
machinery in the reference (§8).

**Bridge has NO** HTTP interface, gateway, client/server split, event-id
correlation protocol, network ACK, pending-work store, awaiting-index,
staleness/timeout sweeps, webhook, or any inbound connection. Telethon and
discord.py connect **outbound** only; Bridge listens on no port. No external bot
(e.g. Tim's Forward Relay bot) can reach Bridge, by design.

**Scope discipline (non-negotiable):** Bridge bridges five action types in both
directions, for the driver/role/channel routing the reference already supports. No
multi-gateway, no client/server roles, no HTTP proxying for other bots. Those are
out of scope, not deferred. Resist re-adding them.

---

## 1. Routing model (KEEP from reference; one correction)

Routing semantics are already correct in the reference (`TD-TDbridge_Scope_v1.0.docx`
§5, plus the Google Sheets layout). Tables (same spreadsheets, via `table_manager.py`):

- **D_User** — keyed by `D_ID` (Discord user snowflake). Has `D_UserStatus`
  (bridged only when it contains "Active"), `D_ChannelID`, and `T_GroupID` (the
  Telegram group this user bridges to).
- **D_Channel** — keyed by `D_ChannelID`, with `D_ChannelStatus`.
- **T_Group** — keyed by `T_GroupID`, with `T_Type` (`group`/`supergroup`) and
  `T_Status`.

### 1a. Discord → Telegram routing (which group to post to)

Reference scope §5.2, in order:
1. If the Discord message is a **reply** to a previously bridged message, route to
   that chain's Telegram group (look up in the map).
2. Else if it **tags a mapped driver**, route to that driver's `T_GroupID` (first
   tagged, if several).
3. Else if the **sender is a mapped driver** (no tag), route to the sender's own
   `T_GroupID` (the "Angel dual-role" case).
4. Else **unroutable** → warn in the channel; do not bridge.

**CORRECTION / ADDITION for Bridge — role routing.** The D_User lookup must fall
back from user to **role**: when resolving a target, if no matching `D_ID` (userid)
row is found, look for a matching **Discord role** and route via that role's
mapping. (The reference routes by user id; Bridge adds role-based routing as a
first-class fallback.) Design the lookup as: *resolve by userid; if none, resolve
by role.* Apply everywhere D_User is consulted, both directions. → See the TD-
Discord→Telegram routing branch (logs "DC→TG routing: routing to sender user … → TG
group …") and extend its lookup with the role fallback.

### 1b. Telegram → Discord routing (which channel/user to post as)

Reference scope §5.1: a Telegram event in a group → look up `T_GroupID` to find the
mapped Discord user/role and channel (the reference logs "TG→DC: cache lookup —
tg_group_id=… → Case 1 Active D_ID=…"). Post to the mapped channel via webhook with
the sender's display name; tag the mapped driver/role. If unmapped, use pseudo-tag
"@ GroupName" to flag a missing row. → See the TD- Telegram-inbound handler doing
this lookup + webhook post.

### 1c. Supergroup requirement — RELAXED for Bridge

The reference required **supergroups** because its gateway needed message-id
numbering to agree between the bot and the relaying client (ordinary groups give
per-member id numbering; supergroups unify it). **Bridge does not care** whether
`tg_msg` numbers differ between the userbot and other members — Bridge alone
assigns/reads Telegram ids through its own map (§2). Bridge works with ordinary
`group` and `supergroup` alike. Do NOT port the `T_Type == supergroup` gate or the
"added to ordinary group → warn" logic. `T_Type` may be recorded for info but must
not gate routing.

---

## 2. Identity and the map (replaces correlation + parking)

The core design idea; it dissolves the reference's
correlation/pending/awaiting/staleness complexity into one indirection.

### 2a. Why an indirection is needed

Telegram-side actions are **metered** (§3) through an outbound FIFO queue, so there
is a delay between "a Discord event arrives" and "the Telegram action is performed
and its real `tg_msg` is known." During that delay a dependent action
(reply/react/edit/delete on a still-queued message) can arrive before its
dependency has a `tg_msg`. This is a **purely local** ordering problem: we wait on
our own queue, which we fully observe and which always executes in order — so there
is no "it might never arrive" and no staleness.

### 2b. The abstraction: `tg_event_id` indirection

**The map never stores raw `tg_msg` ids; it stores an internal `tg_event_id`** — a
stable local token minted synchronously the moment Bridge decides to perform a
message-producing Telegram action (`message` or `reply`). Two tables (may be one
table + one index):

1. **Map**: `dc_msg ↔ tg_event_id` + context (channel/group, sender display name,
   reply-chain root, timestamp). Created immediately when a message is seen. Never
   holds a raw `tg_msg`.
2. **Resolution index**: `tg_event_id → tg_msg`, filled in when the metered queue
   posts the message and Telethon returns the real id. Until then the token is
   *unresolved*.

### 2c. Why queuing becomes immediate and synchronous

Any Telegram action can be built and enqueued **immediately and synchronously**,
referencing prior `tg_event_id`s, **without caring** whether those tokens have
resolved yet. A reply-to-reply can be queued referencing its parent's
`tg_event_id` while the parent is still in the queue. This is safe because of the
**single metered FIFO queue**: by the time an action reaches the front, everything
ahead of it — including its dependency — has posted and resolved its
`tg_event_id → tg_msg`. So the executor resolves each referenced token to a real
`tg_msg` at execution time, guaranteed present.

**Definition — "parking" (a reference term Bridge does NOT use).** In the
reference, an action whose dependency wasn't yet mapped was set aside ("parked")
and re-fired later when a network "correlation" resolved it, with
timeout/abandonment machinery for correlations that never arrived. **Bridge has no
parking:** "the dependency isn't posted yet" is handled implicitly by FIFO order —
the dependent sits behind its dependency in the same queue, and the token resolves
before the dependent executes. No set-aside store, no re-fire, no timeout. In the
TD- code you will see extensive parking / `finish_mapping` / `AwaitingIndex` /
staleness logic — **do not port any of it** (§8); it exists only because the
reference resolved ids over a network from a separate client.

### 2d. Reverse direction (Telegram → Discord)

Discord is metered lightly or not at all (§3); a Telegram inbound event already
carries a real `tg_msg`, and a Discord post returns a real `dc_msg` synchronously —
so reverse-direction tokens are effectively pre-resolved. **Open design choice
(resolve early):** one symmetric map where Telegram→Discord tokens resolve
immediately, OR treat `tg_event_id` indirection as Telegram-outbound-only. Prefer
one symmetric structure **if it adds no special cases**; if forcing symmetry
creates special cases, that's the §5 signal to split.

---

## 3. Metering & rate-limit recovery (KEEP + EXTEND to both sides)

The reference's discovery — pacing Telegram actions to a human-like cadence avoids
FLOOD while preserving like-for-like structure — is a core keeper. In Bridge, both
platforms flow through **metered, durable, FIFO outbound queues with integrated
rate-limit recovery.** Metering and recovery are one system, per side.

### 3a. Parameters (config, env-prefixed like the reference)

- **Telegram:** `USERBOT_METERING_MILLISEC` — production **2000**; testing
  **20000** (slower is easier to test).
- **Discord:** `DISCORD_METERING_MILLISEC` — production perhaps **100**; testing
  **20000**.
- **`0` means no metering delay** (dequeue as fast as possible), either side.

### 3b. Rate-limit / error recovery (same queue, same pattern)

- **Telegram:** on FLOOD_WAIT, wait the required interval, resume. → See the TD-
  userbot outbox worker (meters at `…_METERING_MILLISEC`, handles FLOOD).
- **Discord:** on HTTP **429** (and other recoverable errors), honor retry-after,
  resume — treat 429 exactly as the Telegram queue treats FLOOD_WAIT (same code
  shape).
- Permanent errors (target gone, etc.) → log, and per the reference optionally mark
  status "Error"; do not retry forever.

### 3c. Durability

Both queues are **durable** (SQLite-backed) so a shutdown mid-cadence resumes on
restart. On restart, resume each queue; unresolved `tg_event_id`s resolve as the
Telegram queue drains. → Generalize the TD- userbot outbox (durable FIFO + resume +
metering) into ONE reusable queue used by BOTH sides.

---

## 4. The five action types (KEEP the conceptual core)

Five kinds of actions, both directions:

| Action  | Depends on a prior message? | Produces a new message? |
|---------|-----------------------------|-------------------------|
| message | no                          | yes                     |
| reply   | yes (the parent)            | yes                     |
| react   | yes (the target)            | no                      |
| edit    | yes (the target)            | no                      |
| delete  | yes (the target)            | no                      |

Two independent properties: **(a) depends on a prior message** (resolve its id
before executing) and **(b) produces a new message** (executing mints a new
`tg_event_id` / records a map row). `message` = "reply with no parent"; they share
one path with a nullable parent. One executor — parameterized by direction, verb,
dependency, payload — serves all five both ways. If a type needs a special case,
the boundary is cut wrong (§5).

Per-verb behavior (reference scope §5.3–5.6; KEEP):
- **react** → mirrored as an **attributed reply message** (not a native reaction),
  preserving who reacted ("👍 Alice (Discord) reacted to this message"). `.env`
  toggle for format / excerpt. (The reference's reactions-behavior toggle idea.)
- **edit** → cascade: edit the mirrored message in place with an edit-indicator
  prefix ("✏️ EDIT — 👤 …"); if that fails, post a new reply with the edited text.
  → See the TD- edit in-place-then-reply cascade.
- **delete** → configurable (delete / ignore / post-a-notice), `.env` selected;
  see build-order T-2 re: native delete.
- **message/reply** → attributed prefix ("👤 Name (Discord):"); reply threading
  anchored to the chain root's group. → See the TD- attribution + reply-anchor
  logic.

### 4a. Attachments, media groups, stickers, polls, forwards (KEEP; adapt from TD-)

Substantial and correct in the reference — **adapt, don't re-derive.** Spec
(reference scope §5.4) to preserve:
- Photos/videos **re-uploaded** to the destination (not by URL — Discord CDN URLs
  expire).
- Voice (Telegram `.ogg`) → file attachment on Discord; Discord audio → Telegram
  file in reverse.
- Size limits enforced (reconfirm current Discord/Telegram limits); oversize → text
  notice "[File too large to bridge: name]".
- Stickers → `.webp` image if possible, else text "[Sticker: 🎉]"; `.env`
  force-text toggle.
- Polls → text summary; `.env` format.
- Forwards → new message with "↪️ Forwarded from …" prefix.
- **Media groups / multiple attachments:** Telegram delivers an album as **several
  separate messages** sharing a media-group id; the reference batches them. → This
  is the single most important place to READ the TD- code rather than trust prose:
  find the TD- logic that (i) detects a Telegram media group and gathers its parts
  and (ii) fans a multi-attachment message out to the destination. Adapt it. Note
  the map must handle one logical message ↔ several `tg_event_id`s (each album part
  is its own Telegram message → its own token/row).

---

## 5. Design method (the discipline)

Axioms → lemmas → theorems. Each black box (lemma) does a few well-understood
steps; each bridge behavior (theorem) is a short composition. **If a behavior needs
a special case, the block boundary is wrong — re-cut the blocks, don't add a
conditional.** Proliferating special cases is the diagnostic.

Candidate black boxes (validate by whether five actions × two directions compose):
- **map**: dc_msg ↔ tg_event_id (+ context, + chain root); create, resolve, lookup
  (by dc_msg and by token/tg_msg), purge.
- **resolution index**: tg_event_id → tg_msg; set-on-post, lookup.
- **metered queue** (one impl, instantiated per side): durable FIFO, synchronous
  enqueue, dequeue-at-cadence, rate-limit recovery (FLOOD / 429),
  resume-on-restart, `0` = no delay.
- **one executor**: (direction, verb, dependency, payload) → resolve dependency id,
  perform verb; identical for all five, both directions.
- **two inbound normalizers**: Discord-event → action; Telegram-event → action;
  same action shape.
- **routing resolver**: (event) → target (channel/group + user/role), with the
  userid→role fallback (§1a).

Validation loop (KEEP): incremental, live-tested, one behavior at a time.

---

## 6. Build order

Working reference code exists for most of this — build in capability-sized steps,
adapting from `TD-` files, but gate on verifications first.

0. **Skeleton:** one process, Telethon + discord.py both connecting (outbound
   only); `bridge_config.py` (env-prefixed, `--env test|prod`, `.env`); Sheets via
   `table_manager.py` against the existing spreadsheets; SQLite initialized.

1. **VERIFY native Telegram capabilities** (don't design workarounds for problems
   that may not exist — the reference's reply-"delete" kludge is the caution):
   - **T-1:** the Telethon **user account** receives, for messages posted by
     OTHERS, all five as distinct events: message, reply (with parent ref),
     **reaction** (add/remove + actor), **edit** (new content), **delete** (ids).
     Telethon has `events.NewMessage`, `events.MessageEdited`,
     `events.MessageDeleted`, and raw updates for reactions.
   - **T-1b:** the userbot is notified when **added to a new group** (Telethon
     `events.ChatAction` / relevant update), so Bridge can respond to membership
     changes.
   - **T-2:** native **delete** works (`client.delete_messages`) for the userbot's
     own posts (expected yes); note limits for others' (needs admin). Design delete
     around the verified capability; retire the reply-"delete" kludge unless the
     test shows otherwise.
   - Commit these as tests in the new repo.

2. **Plain message bridging, both directions, WITH the map + metered queues +
   `tg_event_id` indirection** (§2, §3). Adapt the reference's inbound handlers,
   attribution, posting — but route all Telegram-outbound through the new metered
   queue and token/resolution model. Confirm a reply-to-a-still-queued-message
   resolves at execution with no parking.

3. **Remaining verbs** — reply, react, edit, delete — each as the same action
   through the one executor, adapting reference per-verb behavior (§4). Per-direction
   handling only where a *verified* platform difference demands it.

4. **Attachments / media groups / stickers / polls / forwards** (§4a) — adapt TD-
   logic, especially media-group batching (read the code).

5. **Symmetry & robustness** — resolve the §2d symmetric-map question; durable-queue
   resume across restart; Discord 429 recovery alongside Telegram FLOOD recovery;
   map purge window; reaction/delete `.env` toggles; role-routing fallback (§1a).

Non-goals restated: no HTTP, no gateway, no client/server, no correlation, no
parking/staleness, no supergroup gate.

---

## 7. Structure & conventions

- New namespace, all `bridge_` prefixed: `bridge_bot.py` (entry, owns both
  clients), `bridge_config.py`, `bridge_db.py`, `bridge_queue.py`, etc. Never reuse
  reference names.
- `bridge_config.py` mirrors the reference `config.py` style: env prefixes
  (`TEST_`/`PROD_`), `--env`, `.env`, `.env` toggles for any multi-option behavior.
- Reuse `table_manager.py` (copied in) and the **same actual spreadsheets** so
  Bridge runs against real routing data immediately.
- SQLite for map + resolution index + the two durable queues.
- New private GitHub repo, new home dir, new venv (human side).

---

## 8. Explicitly discarded (present in TD-; do NOT port)

These exist in the reference solely because its Telegram side was a **bot**
(invisible to other bots) and ids were resolved over a network from a separate
client. Bridge's Telegram side is a **user**, so they are moot:

- Gateway client/server; loopback HTTP; any inbound HTTP / webhook.
- Event-id correlation protocol + ACK; the correlate endpoint.
- Pending-work store; `finish_mapping`; `AwaitingIndex`.
- Parking; two-kinds staleness (timeout + ACK-order); `CORRELATION_MISSING` /
  `CORRELATION_STUCK` / `CORRELATION_TIMEOUT`; reply-fallback-to-ordinary-message.
- Blind-to-native flags; visibility-matrix gateway flags; client/server role
  assertions.
- Supergroup requirement + "added to ordinary group" warning (§1c).
- Reaction double-application (an artifact of the native+gateway dual path Bridge
  lacks).
- The reply-"delete" kludge (pending T-2).

Rule of thumb: if a piece of reference logic only makes sense in a world where "the
other side is a bot that can't see me," it does not belong in Bridge.
