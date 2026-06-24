"""
gateway_client_test.py — Phase 5 validation harness for GatewayClient.

Run from the laptop (the client side) against the live prod gateway server on
hcf. Exercises every GatewayClient method: health, upload→getfile→download
round trip, send (Echo=true → posts to Telegram, returns real message_ids),
empty long-poll timing, and (if debug endpoints are enabled server-side)
enqueue→poll→ack via the debug enqueue.

Usage:
    python gateway_client_test.py --gateway TDbridge_gw --chat -5102793369 \\
        [--file /path/to/test.jpg] [--secret <secret>] [--debug-enqueue]

The gateway URL and secret are read from telegram_gateways.json (via the same
loader the bot uses) unless --secret is given. --chat is the Telegram group to
echo into (must be a group the prod bot belongs to).

This is a TEST tool, not part of the running bot.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

import gateway_config as gc
from gateway_client import GatewayClient, GatewayClientError


def _load_gateway(name: str, gateways_file: str, secret_override: str = "") -> gc.GatewayDef:
    gws = gc.load_gateways(gateways_file)
    if name not in gws:
        print(f"ERROR: gateway {name!r} not found in {gateways_file}")
        print(f"  available: {list(gws.keys())}")
        sys.exit(2)
    gw = gws[name]
    if secret_override:
        gw.secret = secret_override
    if not gw.secret or gw.secret.startswith("<"):
        print("ERROR: no real secret. Pass --secret or use a real gateways file.")
        sys.exit(2)
    return gw


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default="TDbridge_gw")
    ap.add_argument("--gateways-file", default="telegram_gateways.json")
    ap.add_argument("--secret", default="")
    ap.add_argument("--chat", type=int, required=True,
                    help="Telegram group id to echo a test message into")
    ap.add_argument("--file", default="",
                    help="optional local file to round-trip through upload/getfile/download")
    ap.add_argument("--debug-enqueue", action="store_true",
                    help="also test enqueue→poll→ack (requires GATEWAY_DEBUG_ENDPOINTS server-side)")
    ap.add_argument("--no-send", action="store_true",
                    help="skip the Echo send test (which posts a real Telegram message)")
    args = ap.parse_args()

    gw = _load_gateway(args.gateway, args.gateways_file, args.secret)
    print(f"Gateway: {gw.name}  url={gw.url}  echo={gw.echo}  require_ack={gw.require_ack}")

    async with GatewayClient(gw) as client:
        # 1. Health
        print("\n[1] health ...")
        h = await client.health()
        print("    ->", h)
        assert h.get("ok") is True and h.get("gateway") == gw.name, "health mismatch"
        print("    PASS")

        # 2. File round trip
        if args.file:
            print(f"\n[2] file round trip ({args.file}) ...")
            with open(args.file, "rb") as f:
                original = f.read()
            up = await client.upload_file(original, os.path.basename(args.file), "application/octet-stream")
            print("    upload ->", {k: up[k] for k in ("file_ref", "size")})
            got = await client.download_file(up["file_ref"])
            print(f"    downloaded {len(got)} bytes; match = {got == original}")
            assert got == original, "byte mismatch on file round trip"
            print("    PASS")
        else:
            print("\n[2] file round trip SKIPPED (no --file)")

        # 3. Echo send (posts a real Telegram message)
        if not args.no_send:
            print(f"\n[3] echo send to chat {args.chat} ...")
            text = f"Phase 5 client test {time.strftime('%H:%M:%S')}"
            resp = await client.send_message(args.chat, text=text)
            print("    ->", resp)
            assert resp.get("status") == "sent", f"unexpected send status: {resp}"
            assert resp.get("message_ids"), "no message_ids returned (Echo should return them)"
            print(f"    PASS — message {resp['message_ids']} posted to Telegram group {args.chat}")
        else:
            print("\n[3] echo send SKIPPED (--no-send)")

        # 4. Empty long-poll timing
        print("\n[4] empty long-poll (expect ~10s, empty) ...")
        t0 = time.monotonic()
        events = await client.poll_once()
        dt = time.monotonic() - t0
        print(f"    -> {len(events)} event(s) after {dt:.1f}s")
        print("    PASS" if not events else "    (note: queue was not empty)")

        # 5. Debug enqueue → poll → ack (optional)
        if args.debug_enqueue:
            print("\n[5] debug enqueue -> poll -> ack ...")
            # Use the client's raw session to hit the debug endpoint.
            import json as _json
            session = await client._ensure_session()
            env = {
                "protocol_version": 1, "gateway": gw.name, "secret": gw.secret,
                "event_type": "message",
                "payload": {"chat": {"id": args.chat}, "message_id": 99001, "text": "debug-enqueue test"},
            }
            async with session.post(client._url("debug/enqueue"),
                                    data=_json.dumps(env),
                                    headers={"Content-Type": "application/json"}) as r:
                de = await r.json()
            print("    enqueue ->", de)
            if de.get("status") != "enqueued":
                print("    (debug endpoints not enabled server-side? skipping rest of [5])")
            else:
                evs = await client.poll_once()
                print(f"    poll -> {len(evs)} event(s); first msg_id = "
                      f"{evs[0]['payload']['message_id'] if evs else None}")
                ackresp = await client.ack(args.chat, [99001])
                print("    ack ->", ackresp)
                evs2 = await client.poll_once()
                print(f"    poll after ack -> {len(evs2)} event(s) (expect 0)")
                print("    PASS" if not evs2 else "    (note: still present after ack)")
        else:
            print("\n[5] debug enqueue SKIPPED (no --debug-enqueue)")

    print("\nAll requested client tests completed.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except GatewayClientError as e:
        print(f"\nCLIENT ERROR: {e}")
        sys.exit(1)
