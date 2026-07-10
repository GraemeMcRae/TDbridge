"""
gateway_config.py — Load and validate the TDbridge gateway definitions.

The gateways JSON file (named by the TELEGRAM_GATEWAYS .env parameter) lists
every gateway this instance knows about — both the one it may own (act as
server for) and any it connects to as a client.

See TDbridge_Gateway_Protocol.md for the full protocol and the meaning of each
field. This module is intentionally dependency-free (standard library only) so
it can be imported anywhere without pulling in the bot runtime.

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict


# Current gateway wire-protocol version this build speaks.
GATEWAY_PROTOCOL_VERSION = 1


@dataclass
class GatewayDef:
    """
    One gateway definition, parsed from the gateways JSON file.

    Note on roles: a gateway's role (server vs. client) is NOT stored in the
    file, because the same shared file is read by multiple instances and the
    role depends on which instance is reading. An instance is the SERVER for the
    gateway whose name equals its OWN_GATEWAY, and a CLIENT for every other
    gateway. Use is_server_for() / is_client_for() to resolve the role for a
    given instance.
    """
    name: str
    url: str
    secret: str
    echo: bool = True
    require_ack: bool = False
    relay_user_messages: bool = False
    # client_reposts: does the CLIENT on the other end re-post relayed messages
    # into the real Telegram group itself?
    #   false (default) — the client CONSUMES relayed messages (e.g. a partner's
    #     dispatch bot) and does NOT put them in the group. So for the group to
    #     show a reply, the SERVER must ALSO send it natively — i.e. the server
    #     double-sends (native + gateway). This is the original B/C/D behavior.
    #   true — the client (e.g. the Option-E userbot) re-posts relayed messages
    #     into the group as a user account ("reverse echo"). The server must then
    #     send replies ONLY via the gateway and SUPPRESS the native send, or the
    #     message would appear twice. Mirror image of `echo` (server_reposts).
    client_reposts: bool = False

    def is_server_for(self, own_gateway_name: str) -> bool:
        """True if the reading instance OWNS this gateway (acts as its server)."""
        return bool(own_gateway_name) and self.name == own_gateway_name

    def is_client_for(self, own_gateway_name: str) -> bool:
        """True if the reading instance CONNECTS to this gateway (acts as client)."""
        return not self.is_server_for(own_gateway_name)


class GatewayConfigError(Exception):
    """Raised when the gateways JSON file is missing required data or malformed."""


def _as_bool(value, default: bool) -> bool:
    """Coerce a JSON value (bool/str/number) into a bool, with a default."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return default


# Hosts for which plain HTTP is acceptable: a same-host (loopback) connection
# never leaves the machine, so TLS would add nothing. Used by the co-located
# userbot (Option E), which reaches its owned gateway over http://127.0.0.1.
_LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "[::1]", "::1")


def _url_transport_ok(url: str) -> bool:
    """True if `url` uses an acceptable transport.

    HTTPS is always accepted. Plain HTTP is accepted ONLY for a loopback host
    (127.0.0.1 / localhost / ::1), where the connection stays on the same
    machine and TLS is unnecessary. Any other http:// url is rejected, so a
    remote gateway accidentally configured as http:// still fails loudly.
    """
    lower = url.lower()
    if lower.startswith("https://"):
        return True
    if lower.startswith("http://"):
        host = lower[len("http://"):]
        # Strip anything after the host[:port] (path, query).
        host = host.split("/", 1)[0]
        # Compare against loopback hosts, allowing an optional :port suffix.
        for lb in _LOOPBACK_HOSTS:
            if host == lb or host.startswith(lb + ":"):
                return True
    return False


def load_gateways(path: str) -> Dict[str, GatewayDef]:
    """
    Load and validate the gateways JSON file at `path`.

    Returns a dict mapping gateway name → GatewayDef.
    Returns an empty dict if `path` is empty/unset (gateway feature inactive).
    Raises GatewayConfigError on a missing file or malformed/duplicate entries.
    """
    if not path:
        return {}

    if not os.path.isfile(path):
        raise GatewayConfigError(f"Gateways file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        raise GatewayConfigError(f"Gateways file is not valid JSON: {e}") from e

    entries = raw.get("gateways")
    if not isinstance(entries, list):
        raise GatewayConfigError(
            'Gateways file must contain a top-level "gateways" array.'
        )

    result: Dict[str, GatewayDef] = {}
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise GatewayConfigError(f"Gateway entry #{i} is not an object.")

        name = str(entry.get("name", "")).strip()
        url = str(entry.get("url", "")).strip()
        secret = str(entry.get("secret", "")).strip()

        if not name:
            raise GatewayConfigError(f"Gateway entry #{i} is missing 'name'.")
        if not url:
            raise GatewayConfigError(f"Gateway '{name}' is missing 'url'.")
        if not _url_transport_ok(url):
            raise GatewayConfigError(
                f"Gateway '{name}' url must be HTTPS, or HTTP to a loopback "
                f"host (127.0.0.1/localhost/::1) for a same-machine gateway "
                f"(got: {url})."
            )
        if not secret:
            raise GatewayConfigError(f"Gateway '{name}' is missing 'secret'.")
        if name in result:
            raise GatewayConfigError(f"Duplicate gateway name: '{name}'.")

        result[name] = GatewayDef(
            name=name,
            url=url,
            secret=secret,
            echo=_as_bool(entry.get("echo"), default=True),
            require_ack=_as_bool(entry.get("require_ack"), default=False),
            relay_user_messages=_as_bool(
                entry.get("relay_user_messages"), default=False
            ),
            client_reposts=_as_bool(entry.get("client_reposts"), default=False),
        )

    return result


def validate_own_gateway(
    gateways: Dict[str, GatewayDef], own_gateway_name: str
) -> None:
    """
    Validate that, if this instance declares an OWN_GATEWAY, that gateway is
    listed in the file. An empty own_gateway_name is valid (client-only
    instance) and passes. The role itself is derived at runtime
    (GatewayDef.is_server_for), so there is nothing role-related to check here
    beyond existence.
    """
    if not own_gateway_name:
        return
    if own_gateway_name not in gateways:
        raise GatewayConfigError(
            f"OWN_GATEWAY '{own_gateway_name}' is not listed in the gateways file."
        )
