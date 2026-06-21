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
        if not url.lower().startswith("https://"):
            raise GatewayConfigError(
                f"Gateway '{name}' url must be HTTPS (got: {url})."
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
