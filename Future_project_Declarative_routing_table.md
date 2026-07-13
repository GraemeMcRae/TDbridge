# Future Project: Declarative Routing Table

**Status:** Proposed (not started). To be undertaken *after* the Gateway +
Userbot subsystem is complete, fully tested in test mode, and frozen at a
known-good milestone commit (e.g. "Frozen TDbridge with Gateway and Userbot
Final Final Version 1").

**One-line summary:** Replace the imperative, hard-coded dispatch logic in
`bot.py` (the "arcane method" that decides whether to send a message/reaction/
edit/deletion natively, via a gateway, or both) with a **declarative routing
table** — a re-keyed `message_map` — so that routing becomes *look up the rows,
do the simple thing each row says*, instead of *evaluate a tangle of conditions
and branch*.

---

## 1. Motivation

Today TDbridge decides how to propagate a Discord-side action (a reply, a
reaction, an edit, a deletion) to Telegram using conditional logic embedded in
one or more long methods in `bot.py`. That logic asks questions like: is this a
reply to a gateway-origin message? does this instance serve a `client_reposts`
gateway? should we send natively, via the gateway, or both? The answers are
computed each time from flags and origin lookups, and the "both" case (send
natively *and* via the gateway) is handled by a single method that does one
send and then, under the right circumstances, also does the other.

This works, but it concentrates hard-to-follow decision-making in imperative
code. The insight behind this project is that **the decision is really a data
lookup**: given a Discord message, *which Telegram targets does it map to, and
by what route (native or which gateway)?* If the mapping table records that
directly, the code that acts on it becomes a loop over rows, each row saying
"send to this (route, group, message)" — no branching on flags, no arcane
conditions.

### The two-sided fan-out (the core idea)

- **Discord → Telegram fan-out:** One Discord message can map to multiple
  Telegram targets — e.g. a media group (already one-DC-to-many-TG today), or
  the same content sent *both* natively *and* via a gateway (Tim's B/C/D case:
  native so humans in the group see it, gateway so Tim's bot — blind to native
  posts — also receives it). A downstream reaction on that Discord message
  should fan out to *all* its targets: look up all rows for the Discord
  message, perform the action once per row.
- **Telegram → Discord fan-out (the mirror):** A lookup keyed from the Telegram
  side (`find_by_tg`) can likewise return *all* matching rows across routes. A
  native Telegram update matches the native row; a gateway event matches that
  gateway's row. When no route is specified, the lookup returns the full set —
  which is exactly what the current arcane method computes by hand before
  testing `origin_gateway` and choosing one/other/both.

Both directions collapse the same way: **imperative branching becomes row
iteration.**

---

## 2. The central change: re-key `message_map`

### Current schema (as of the frozen milestone)

```
message_map (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_group_id     TEXT NOT NULL,
    tg_message_id   TEXT NOT NULL,
    dc_channel_id   TEXT NOT NULL,
    dc_message_id   TEXT NOT NULL,
    root_tg_msg_id  TEXT NOT NULL,
    dc_user_id      TEXT NOT NULL DEFAULT '',
    origin_gateway  TEXT NOT NULL DEFAULT '',   -- currently a payload column
    created_at      REAL NOT NULL
)
UNIQUE INDEX idx_tg ON message_map (tg_group_id, tg_message_id)
INDEX        idx_dc ON message_map (dc_channel_id, dc_message_id)   -- non-unique
```

`origin_gateway` already exists but is a *payload* column, not part of the key.
The unique key is `(tg_group_id, tg_message_id)`, so the *same* (group, message)
cannot appear twice — which is precisely why "send both natively and via
gateway" cannot be represented as two rows and must instead be jammed into
imperative logic.

### Proposed schema

Promote `origin_gateway` **into the unique key**:

```
UNIQUE INDEX idx_route_tg ON message_map (origin_gateway, tg_group_id, tg_message_id)
```

- A **blank** `origin_gateway` means **native bridging** (the current default
  meaning is preserved).
- A **non-blank** `origin_gateway` means the row represents the message as it
  exists *through that gateway* (which, for an invented-id client, may be an id
  meaningful only to that client).

Now the same `(group, message)` can exist once per route: one native row
(blank gateway) and one row per gateway. "Send both ways" is simply **two rows**
pointing at the same Discord message:

```
(origin_gateway='',            tg_group=456, tg_msg=123) → dc_msg=789
(origin_gateway='Userbot_gw',  tg_group=456, tg_msg=123) → dc_msg=789
```

A downstream reaction on `dc_msg=789` looks up both rows and performs the action
twice: once natively to (456,123), once out `Userbot_gw` to (456,123). The
"one/other/both" decision is *gone* — it's just "do each row."

### 2a. Candidate: consolidate mapping + correlation state into one table

A strong option for the re-keyed table (to evaluate during this project): key
`message_map` by **`discord_msgid`** (+ channel) rather than by the Telegram
tuple, and allow a row to exist in a *provisional/awaiting* state — a NULL
Telegram tuple `(tg_group, tg_message)` plus an `event_id` column naming the
outbound event whose correlation will fill in the tuple. A **secondary
non-unique index** on the Telegram tuple preserves efficient reverse lookups
(Telegram-side → Discord-side). With this shape, a single lookup answers all
three states an incoming Discord action needs to distinguish:

- **Mapped**: a row exists with a non-NULL Telegram tuple → route now.
- **Awaiting correlation**: a row exists with a NULL tuple + an `event_id` →
  the correlation has not completed yet → park the action on that event_id.
- **Unknown**: no row → genuinely not ours → drop.

This would collapse the interim Phase-4 two-structure approach (the live
`message_map` plus a small separate "awaiting" table keyed by dc_msg) into one
table, and it dovetails with putting `origin_gateway` in the key (a row per
route, some provisional). Deferred to this project because it re-keys the
central table and touches every lookup in prod's core — exactly the blast radius
the userbot subsystem deliberately avoids. The userbot subsystem instead uses a
small, separate, volatile "awaiting" table so `message_map` is untouched until
this project.

### 3a. Deferred bug: multi-level cascade gap (dependency still parked)

Observed live (2026-07-12, supergroup, unified ids). A reaction to a
reply-to-reply (grandchild) was silently dropped when the reaction arrived while
the grandchild's OWN relay was still parked.

Timeline: the grandchild reply was itself a parked action (waiting on its
parent, the third child, to correlate). A few seconds after writing the
grandchild, the user reacted to it. At that instant the grandchild's dc_msg was:
- **not mapped** — its reply had not executed, so no message_map row; and
- **not awaiting** — its reply was still parked, so `register_pending_mapping`
  (which creates the awaiting-index entry) had not run yet.

Neither mapped nor awaiting → the reaction hit the "not ours → drop" branch
(no log). The gap: the awaiting-index only knows about messages whose relay has
**executed** (and thus registered a pending mapping). A message whose relay is
still in the parking lot — waiting behind another parked action — is invisible,
so an action depending on it finds nothing to wait on.

**Root-cause reframing (this is the fix the refactor should adopt).** Today the
provisional record and the awaiting-index are keyed by **event_id**, and the
event_id is not assigned until the message is **enqueued** to the gateway — which
is exactly what a parked relay defers. But `register_pending_mapping` does NOT
reference the parent or the message map at all; it only records dc-side context
and writes a row when the event correlates. The ONLY reason it cannot run at
Discord-message-arrival is the event_id key. Two things are conflated in
"event_id":
1. **identity of the dc_msg awaiting a mapping** — known the instant the Discord
   message arrives; and
2. **the correlation token** the client echoes back to report the tg_msg — known
   only at enqueue time.

Separate them: **key the provisional/awaiting record by dc_msg (known
immediately), and treat event_id as merely the token that later resolves it.**
Then a provisional record exists from the moment a Discord message is seen
(tg_msg = NULL), so anything depending on that dc_msg can park against it
immediately — regardless of how many levels of parked relays sit above it. The
correlation later fills in the tg_msg (provisional → resolved). This is exactly
§2a's single-table, dc_msg-keyed, NULL-tuple provisional row: the awaiting-index
and message_map are the same table in two states. Downstream actions park on a
dc_msg, not an event_id; the event_id is associated at enqueue and consumed at
correlation.

Deferred to this project because the principled fix is precisely the dc_msg-keyed
provisional-row model (§2, §2a) — a bolt-on subscription in the current
event_id-keyed machinery would be replaced by that model anyway. Low current
exposure (requires a fast reaction to a message whose own relay is still parked —
a multi-level cascade, reachable but narrow). When the routing table is
dc_msg-keyed with provisional rows, this gap does not exist: the grandchild's
provisional row is present the moment it is seen.

### Consequence analysis (worked through in design discussion)

1. **Same (group,msg) can exist twice (native + gatewayed).** This is the point
   — it's how "both ways" is represented. Today it can't be, which is why the
   dual-send is imperative. **Improvement.**
2. **Lookups gain a route dimension.** `find_by_tg` / `find_by_dc` must accept a
   gateway/native context. Two forms:
   - `find_by_tg(gateway, group, msg)` — called from a context that *implies* a
     route (a native Telegram update implies blank; a gateway event implies that
     gateway). Most existing call sites are of this form and refactor trivially.
   - `find_by_tg(ANY, group, msg)` — returns the *list* of all rows across
     routes (efficient in SQL: drop the `origin_gateway` term from the WHERE
     clause; the remaining index columns still apply). This is the Telegram-side
     fan-out that mirrors the Discord-side fan-out, and it is what the arcane
     method currently computes by hand. **This is the cost** (touching every
     lookup call site), and also **the payoff** (the arcane method becomes a
     list lookup + iteration).
3. **DC→(reaction/edit/delete) becomes a fan-out over rows.** Look up all rows
   for the Discord message; perform the action once per row, each to its
   (route, group, message). The hard-coded dual-send branch disappears.
   **Improvement — the strongest single argument for the project.**

---

## 3. Correlation completion CREATES/UPDATES rows (for all messages)

(Builds on the Gateway/Userbot subsystem's correlation machinery.)

- **Every** message_map row is born from a **correlation completion**, uniformly:
  - **Server echo (`echo:true`)**: the server posts to Telegram natively, then
    *self-emits* a `correlate(event_id → [tg_id])`; the correlate handler writes
    the (native) row. (Migrated from today's inline row-write in the echo path —
    see §5 staging note; this is the higher-blast-radius change because the echo
    path handles all of prod's normal traffic.)
  - **Client repost (`client_reposts:true`)**: the client posts and sends
    `correlate(event_id → [tg_id])`; the handler writes the (gateway) row.
  - **Invented ids**: a client that never really posts but invents ids still
    sends a correlate; we record it faithfully and route downstream actions out
    that gateway with the client's own id. The server neither knows nor cares
    whether the id is "real." (Validates the domain-agnostic registry.)

- **Merge, not replace.** Because message_map is one-Discord-to-many-Telegram,
  correlation completion must **add** rows, never clobber siblings. Each
  correlated target id becomes its **own row**. Two correlates for one event
  (the echo+repost misconfiguration) therefore *merge* — both routes end up
  represented — rather than one overwriting the other. The only legitimate
  REPLACE is re-storing the *same* (route, group, msg) (e.g. an edit/resend).
  - Note: with the route in the key, "server echoed AND client reposted the same
    event" produces a native row *and* a gateway row — no collision, no data
    loss, exactly the desired merge. "Do what we're told and move on."

- **The correlation event needs no gateway field.** Every correlate arrives
  *through* a gateway (or natively for echo), so the (route) dimension of the
  resulting row is already determined by *how the correlate arrived*. The
  correlate payload stays minimal: `event_id → [target_id, ...]` (bare ids).
  Group and route come from arrival context, not from the payload. Keeps the
  correlation registry domain-agnostic.

---

## 4. New gateway flag: "blind to native bridging"

(Working name — final name TBD, e.g. `client_blind_to_native` /
`native_invisible_to_client`.)

The B/C/D rationale for dual-send is: *Tim's bot cannot see messages
TDbridgeProdBot posts natively into the Telegram group, so we must also send
everything via the gateway.* Today that fact is implicit in the arcane method.
This project makes it **an explicit per-gateway boolean** in
`telegram_gateways.json`.

When set on a gateway, it means: for messages on that gateway's groups, generate
**two** correlations (hence two message_map rows) — the native one (from the
server, per `echo:true`) and the gateway one (from the client, per this new
flag) — so that both routes are recorded and downstream fan-out naturally hits
both. This replaces the imperative "also send it the other way" with a
declarative "this gateway is blind to native, so both rows exist."

Interaction with existing flags:
- `echo` (a.k.a. `server_reposts`) — server posts to Telegram.
- `client_reposts` — client posts to Telegram (userbot case).
- the new blind-to-native flag — client cannot see native posts, so native
  content must *also* be relayed via the gateway (B/C/D case).

These are orthogonal booleans; the routing table expresses their *combined*
effect as rows, so the code never has to reason about the combination.

---

## 5. The end state: arcane method → table lookup

> "The Python method that makes painful agonizing decisions using arcane
> hard-coded methods will be replaced by a look-up in a database, followed by
> do this simple thing."

Concretely, the target shape of the propagation code:

```
# Discord-side action (reaction / edit / delete / reply) on dc_msg:
rows = find_all_by_dc(dc_channel, dc_msg)        # fan-out: all routes
for row in rows:
    route = row.origin_gateway                    # '' = native, else a gateway
    perform_action(route, row.tg_group, row.tg_message, action)   # one simple thing
```

No flag-testing, no "one or the other or both" branching. The table is the
single source of truth for *what exists where*, and the loop does the rest.

### Staging note (protecting prod, even though prod is low-exposure)

Prod is currently low-exposure (drivers are not actively using their Telegram
groups; Tim's bot is blind to TDbridgeProdBot's native posts; TDbridgeProdBot
could be down for ~2 days with only the maintainers noticing). This reduces —
but does not eliminate — the value of staging. Recommended order within this
project:

1. Re-key `message_map` (add `origin_gateway` to the unique index) and migrate
   all lookup call sites to the route-aware forms, **preserving current behavior**
   (native rows only, blank gateway) — a pure refactor with no behavior change,
   verified against prod's normal traffic.
2. Route the **client_reposts** row creation through correlation completion
   (already true from the Userbot subsystem's Phase 3) and confirm gateway rows
   coexist with native rows.
3. Migrate the **echo path** to create its native row via self-correlate
   (highest blast radius — this is prod's normal traffic). Test thoroughly.
4. Introduce the **blind-to-native** flag and represent dual-send as two rows;
   delete the arcane imperative dual-send branch.
5. Collapse the DC-side and TG-side propagation code to the fan-out/iterate
   shape above.

---

## 6. Related refactors folded into this project

These were accumulated on the running wishlist during the Gateway/Userbot build
and belong here, as they are the same "make bot.py modular" effort:

- **Extract gateway + correlation logic out of `bot.py`** into a dedicated
  module (or modules), so bridging/correlation becomes a black box and `bot.py`
  stops being the catch-all host. The declarative routing table is the natural
  seam along which to cut.
- **Analyze `bot.py`'s longer / more complex methods for modularization**,
  *preferring to repurpose or generalize existing black boxes over creating new
  ones*. The best refactor removes a long method by discovering it is a special
  case of something a black box already does. Fewer boxes doing more general
  work beats more narrow boxes.
- **`echo:true` + present-id override** (deferred design item): when echo is on
  and an id is already present, decide the precise semantics (server reposts
  only if no message id, etc.) — the "server_reposts but only if there is no
  message id" refinement discussed during the Userbot build.
- **`PIN_GATEWAY=true`** (`.env` param already stubbed): route *all* Discord
  messages through the gateway (not just replies), so nothing is invisible to a
  gateway consumer. Interacts with `client_reposts` (a pinned non-reply message
  under `client_reposts=true` should relay-only, not double-send) — an
  interaction the routing table expresses naturally as rows.

---

## 6a. Discord → Telegram emoji reaction translator

**Problem observed (Phase 3 live test):** Telegram only accepts reactions from a
curated allowed set. A Discord user can react with *any* emoji (hundreds
possible), and when that emoji is not in Telegram's allowed set, the reaction is
rejected at the final API call (`setMessageReaction`/`SendReactionRequest` →
HTTP 400, "Invalid reaction provided (only emoji are allowed)"). Today the
reaction then fails silently (dropped in the outbox / logged as a warning), so a
driver reacting with a non-approved emoji gets no effect and no feedback.

**Proposed:** a translation layer that maps any Discord reaction emoji to the
nearest **approved Telegram** reaction, by category, before the reaction is sent
to Telegram (native or via gateway). Only translate when the original emoji is
not already approved (pass approved emoji through unchanged).

**Design work required:** classify the full set of plausible Discord reaction
emoji into categories, each with a representative approved-Telegram emoji.
Starter category table (representatives):

| Category | Emoji | Unicode | Python literal | Notes |
| --- | --- | --- | --- | --- |
| Positive / Approval | ✅ | U+2705 | `"\u2705"` | White Heavy Check Mark |
| Negative / Disapproval | ❌ | U+274C | `"\u274c"` | Cross Mark |
| Surprise / Shock | 😮 | U+1F62E | `"\U0001f62e"` | Face with Open Mouth |
| Sadness / Sympathy | 😭 | U+1F62D | `"\U0001f62d"` | Loudly Crying Face |
| Other Expressive | 🤷 | U+1F937 | `"\U0001f937"` | Shrug (no gender modifier) |
| Other Non-Expressive | 😐 | U+1F610 | `"\U0001f610"` | Neutral Face |

**Approved Telegram reaction set** (the common default/standard reactions to map
*to*; verify against Telegram's current list at implementation time, as it can
change):
- Positive / Approval: 👍 ❤️ 🔥 🎉 👏 😁 🤩 😂 🤣 🥰 🥳 😎 ✅
- Negative / Disapproval: 👎 💩 🤮 🤬 🖕 ❌
- Surprise / Shock: 😱 😮 🤯 😳
- Sadness / Sympathy: 😢 😭 💔 🥺
- Other Expressive: 🤔 🤷 🤫 ⚡ 🏆 💤
- Other Non-Expressive: 😐 🐳 🕊️ 🍌 🍓

**Notes / open questions for the implementation:**
- Should live as a small standalone black box (`emoji_translate.py`): input any
  emoji → output an approved Telegram emoji (pass-through if already approved).
  Domain-agnostic, table-driven, unit-testable in isolation.
- Consider whether an unmapped/unknown emoji falls back to a neutral
  representative (😐) or to a reply-style bridge ("X reacted <emoji>") so the
  original emoji is preserved as text when it cannot be sent as a reaction.
- The approved list should be easy to update (Telegram may change it); keep it as
  data, not code.
- Applies to BOTH the native reaction path and the gateway reaction path, so the
  translator should sit at a shared choke point before the reaction leaves for
  Telegram.

## 6a-bis. Deferred bug: reaction double-application (server-role client_reposts)

Observed live (supergroup test, 2026-07-12). In the server-role `client_reposts`
case, a Discord reaction to a gateway-reposted message is applied TWICE to the
Telegram message: once natively by the server instance's bot account, and once
by the client (userbot) that was enqueued the reaction. Two distinct accounts →
Telegram shows two reaction pills. (The `both` reply-message is not duplicated,
because only the native path expands to a reply-message; the enqueued client
reaction is a bare reaction.)

Root cause: the reaction executor does BOTH `_perform_reaction_to_target` (which
falls through to a native reaction/reply when we are not the gateway client) AND
`_gw_enqueue_outbound_reaction`. This is inconsistent with the MESSAGE contract,
where under `client_reposts` the server deliberately does NOT send natively (only
the client reposts). In the ordinary-group era the native reaction failed with
HTTP 400 and the duplication was invisible; in a supergroup the native reaction
succeeds, exposing it.

Deferred deliberately to this project (not patched inline) because the clean fix
is not "add another role check here" — it is the role delineation and the
unified transport model below (6b/6c). A local patch would add one more instance
of the scattered role conditionals that 6b exists to consolidate. Low current
exposure (no driver reacts to reposted messages yet). When fixed under the
unified model, a gateway-origin reaction in the server role will take the gateway
transport ONLY, and the duplicate native application simply won't exist.

## 6a-ter. Framing vision: the bridge as a Telegram-like server/client pair

This is the organizing analogy the refactor should be built around (captured so
the design blocks are chosen well from the start rather than discovered late).

The claim: **the Discord side of the bridge behaves like a Telegram-like
*server*, and the Telegram side behaves like a Telegram *client*.** Where the
real Telegram server packages/stores/manages messages, our "Telegram-like
server" packages/stores/manages messages *on Discord*. The same abstraction then
serves both:
- the **native bridge** (Discord-side = Telegram-like server; Telegram-side =
  real Telegram client — we poll/send against the Bot API), and
- a **gateway** (Discord-side = Telegram-like server; Telegram-side = a
  Telegram-like client, e.g. the userbot).

Consequences if the analogy holds:
- **One black box per server-like function** (package a message for
  storage/management, assign/track ids, correlate, fan out) usable by BOTH the
  bridge and the gateway, with necessary differences appearing as short
  either/or paths inside, not as duplicated implementations.
- **One black box per client-like function** (receive/poll inbound, send/react/
  edit/delete outbound, ack) shared between the real-Telegram client and the
  Telegram-like client.
- The `client`/`server` **role** (6b) is then not an ad-hoc flag but the
  fundamental identity of each side of each transport, asserted once per unit of
  work: `((role=client AND transport≠own) XOR (role=server AND transport=own))`.
- The reaction double-application (6a-bis) dissolves: a server-role action emits
  on its transport once; there is no separate "also do it natively" path,
  because "native" is just one transport the server drives through the same
  client-like black box.

**Refinement (root cause is VISIBILITY, not id-spaces).** The multiple-id-space
problem is a *consequence*, not the root. The root is **Telegram message
visibility** across the mix of bots and users in a group: bots cannot see other
bots' messages/reactions; a userbot's reposts are visible to users but the
correlation must be tracked; and the "rogue" client that invents its own tg_msg
numbers and never posts is simply the LIMITING CASE of "this participant's
messages are invisible to all other Telegram participants" (invisibility total).
If the gateway boolean flags capture the visibility matrix — candidates like
`echo`, `relay_user_messages`, `client_reposts`, and a
`messages_invisible_to_bots` / `client_invisible` style flag — then id-space
divergence falls out of visibility rather than being modeled as its own problem.
Design the server-like/client-like blocks around a declared visibility model;
the id bookkeeping (route-keyed rows) then expresses the consequences of that
model rather than being a separate concern. This subsumes stress-test #1 below:
id-space ownership is one facet of the visibility declaration.

Stress-tests to resolve DURING design (where the analogy might strain, so we
find out before building rather than after):
1. **Id spaces.** The real Telegram server owns the message-id space; our
   Telegram-like server owns the Discord id space; a Telegram-like *client*
   (userbot) invents/holds its own ids. The "package a message" black box must
   be explicit about WHOSE id space a given id lives in (this is exactly what the
   declarative routing table's route-keyed rows encode). If the analogy is going
   to break, it will most likely break here — so id-space ownership must be a
   first-class parameter of the server-like black boxes, not an afterthought.
2. **Fan-out asymmetry.** A real Telegram client talks to ONE server; our
   Telegram-like client (the bridge's Telegram side) may need to represent a
   message that exists on multiple transports (native + gateway). The
   client-like black box must tolerate one logical message having several
   transport-specific realizations (the two-sided fan-out, §1).
3. **Reactions/edits/deletes as first-class client verbs.** The analogy is
   cleanest for "message"; verify each of react/edit/delete maps to a clean
   client-like verb with a server-like counterpart, rather than becoming a
   special case. If special cases multiply here, that is signal (per the note
   below) that a block boundary is drawn in the wrong place.

Design method (explicitly adopted): build from small, proven building blocks —
axioms → lemmas → theorems. Each "lemma" (black box) does a few well-understood
steps; each "theorem" (a bridge/gateway behavior) is composed of a handful of
lemmas. If a behavior cannot be expressed as a short composition of existing
blocks, the blocks are wrong, not the behavior — redesign the blocks. A
proliferation of special cases or work-arounds is the diagnostic that a block
boundary is misplaced; treat it as a signal to re-cut the blocks, not to add
another conditional.

## 6b. Explicit client/server role discipline per gateway unit of work

At the start of every unit of work that touches a gateway, set an explicit
`role = "client"` or `role = "server"`, and assert the invariant:

```
((role == "client" and gateway != own_gateway) XOR
 (role == "server" and gateway == own_gateway))
```

This makes the role explicit and catches confusion early (a whole class of bugs
this subsystem hit — e.g. a reaction that should have been server-enqueued going
down a client/native path instead — would trip the assertion immediately). The
role is partly implicit today (scattered `is_serving()` / `_get_gateway_client`
checks); this consolidates it into one stated, asserted variable per unit of
work. Loud, early failure on a role/gateway mismatch beats a silent
mis-delivery.

## 6c. Unify native bridging as "client of the Telegram API"

Native bridging and gateway actions should be unified by treating the **native
Telegram bridge as just another gateway** — specifically, TDbridge acting as a
*client* of the Telegram Bot API exactly as it acts as a client of any gateway
it does not own. The client mentality is already implicit in the original
bridging code (it polls the Telegram API to receive events, posts to the API to
send — classic client behavior), but because native bridging was designed and
built *before* the client/server gateway concept existed, the client code was
later "bolted on" separately, leaving two parallel implementations of
fundamentally the same pattern (poll for inbound, send outbound, ack/track). The
result is duplication of functions that a unified model would collapse.

Target: a single "client-of-a-transport" abstraction where the Telegram Bot API
is one transport (the native bridge) and each gateway is another. Inbound
(poll/receive) and outbound (send/react/edit/delete) then share one code path
parameterized by transport, rather than native-specific and gateway-specific
copies. This dovetails with the declarative routing table (each message_map row
names a route/transport; acting on a row means "do the action on that
transport") and with 6b (role is a property of the transport interaction).

**Pre-existing noise this would clean up (observed in Phase 4 testing):** for a
server-role, gateway-origin message, the current reaction path always attempts a
*native* Telegram reaction first (which fails with HTTP 400 "message to react
not found", because the message lives only in the client's Telegram space) and
*then* enqueues out the gateway (which succeeds). The reaction is delivered
correctly, but two scary WARNING lines are logged every time. Under the unified
model, the route on the message_map row would say "gateway" and only the gateway
transport would be used — no doomed native attempt, no noise. (Do NOT special-
case this away in isolation before the unification; it is a symptom of the
duplication, and the clean fix is the unified transport model.)

## 7. Black-box discipline (carried over)

- The **correlation registry** and **parked-action store** remain standalone,
  domain-agnostic modules (opaque event ids → target ids; callbacks keyed on
  event ids). They must not gain knowledge of Discord/Telegram specifics. If a
  boundary starts to mention platform specifics, that is the signal the
  abstraction is leaking and should be reshaped.
- The correlation registry **feeds** the message store (correlation completion
  writes message_map rows); it is not a parallel duplicate of it.
- Reaping / staleness / purge hang off **existing** cadences (the 24-hour
  cleanup, the periodic dashboard cycle), not a new scheduler.
- Generality is not gold-plating here: a route-aware, domain-agnostic routing
  table and correlation registry are what let the *same* machinery serve the
  userbot gateway **and** the future client/server gateway (the test-data
  generator setup: test TDbridge as client, prod TDbridge as server, passing
  messages/edits/reactions/replies/deletes over the gateway) without
  duplication.

---

## 8. Explicit non-goals for this project

- Not a change to the Telegram Bot API usage, polling/webhook mode, or the
  Discord side beyond routing.
- Not a change to the Google Sheets mapping tables' meaning (D_User / T_Group),
  except insofar as routing reads them.
- Not a rewrite of the outbox / metering / FLOOD handling (those black boxes are
  reused as-is).
