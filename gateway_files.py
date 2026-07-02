"""
gateway_files.py — Disk-backed store for gateway attachments (two-hop transfer).

Attachment bytes are held on disk under the configured GATEWAY_FILES directory
until they have been successfully sent or received through the gateway; a small
SQLite table (see db.py) maps the upload file_ref and the download capability
token to the on-disk path plus metadata.

Lifecycle (deletion at the "retry no longer possible" moment):
  • Receive/poll direction: a polled event's files are deleted when the event is
    acked (RequireACK) or right after the delivering poll (non-RequireACK).
  • Send/bridge direction (Phase 6): files are deleted once the send-and-bridge
    has either succeeded or definitively given up after the retry budget.
  • Safety net: a daily sweep deletes any file older than 24h and logs an ERROR
    per leftover (name, size, all timestamps in local time) for investigation —
    a leftover should not happen in normal operation.

Copyright (c) 2026 Squadron Trucking. Released under the MIT License. See
LICENSE_TDbridge.md.
"""

from __future__ import annotations

import logging
import os
import secrets
import time
from datetime import datetime
from typing import Optional

import db
from config import config

logger = logging.getLogger(config.bot_name)

# Age (seconds) past which a still-present gateway file is considered a leftover
# and swept with an ERROR. Tied to the ~24h Discord-refresh cadence.
LEFTOVER_AGE_SECONDS = 24 * 3600


def _dir() -> str:
    return config.gateway_files_dir or ""


def ensure_dir() -> None:
    """Create the gateway files directory if needed (when a gateway is owned)."""
    d = _dir()
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)
        logger.info("Gateway files directory created: %s", d)


def max_bytes() -> int:
    return int(config.gateway_filesize_mb) * 1024 * 1024


def store_upload(gateway: str, data: bytes, file_name: str, mime_type: str) -> dict:
    """Persist uploaded bytes to disk and record a reference row.
    Returns {file_ref, file_name, mime_type, size}."""
    ensure_dir()
    file_ref = "gw-file-" + secrets.token_hex(16)
    path = os.path.join(_dir(), file_ref)
    with open(path, "wb") as f:
        f.write(data)
    size = len(data)
    db.gateway_file_add(file_ref, gateway, path, file_name, mime_type, size)
    logger.debug("Gateway file stored: %s (%d bytes) as %s", file_name, size, file_ref)
    return {
        "file_ref": file_ref,
        "file_name": file_name,
        "mime_type": mime_type,
        "size": size,
    }


def make_download_token(file_ref: str) -> Optional[str]:
    """Assign (or reuse) a random capability token for downloading file_ref.
    Returns the token, or None if the file_ref is unknown."""
    row = db.gateway_file_by_ref(file_ref)
    if row is None:
        return None
    if row.get("download_token"):
        return row["download_token"]
    token = secrets.token_urlsafe(32)
    if db.gateway_file_set_token(file_ref, token):
        return token
    return None


def resolve_token(download_token: str) -> Optional[dict]:
    """Return the file row for a download token (dict) or None. Verifies the
    on-disk file still exists."""
    row = db.gateway_file_by_token(download_token)
    if row is None:
        return None
    if not os.path.isfile(row["path"]):
        return None
    return row


def read_file_by_ref(file_ref: str) -> Optional[dict]:
    """Load an inbound gateway file's bytes + metadata by file_ref.

    Used by the inbound attachment path: the client uploaded the file to our
    store (via /gateway/upload), then referenced it in a message event; we read
    the bytes here to send to Telegram and bridge to Discord.

    Returns {data, file_name, mime_type, size} or None if the ref is unknown or
    the file is missing on disk.
    """
    row = db.gateway_file_by_ref(file_ref)
    if not row:
        logger.warning("Gateway file read: unknown file_ref %s", file_ref)
        return None
    path = row.get("path")
    if not path or not os.path.isfile(path):
        logger.warning("Gateway file read: missing file on disk for %s", file_ref)
        return None
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as e:
        logger.warning("Gateway file read: could not read %s: %s", path, e)
        return None
    return {
        "data": data,
        "file_name": row.get("file_name", "") or "",
        "mime_type": row.get("mime_type", "") or "",
        "size": row.get("size", len(data)) or len(data),
    }


def delete_file(file_ref: str) -> None:
    """Delete a gateway file (disk bytes + reference row). Called at the
    no-more-retry moment. Safe if already gone."""
    path = db.gateway_file_delete(file_ref)
    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except OSError as e:
            logger.warning("Gateway file: could not remove %s: %s", path, e)


def _fmt_local(epoch: float) -> str:
    """Format an epoch time in the configured local timezone."""
    from zoneinfo import ZoneInfo
    try:
        tz = ZoneInfo(config.local_timezone)
    except Exception:
        tz = None
    try:
        dt = datetime.fromtimestamp(epoch, tz) if tz else datetime.fromtimestamp(epoch)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z").strip()
    except Exception:
        return str(epoch)


def _expire_attachment_in_events(file_ref: str, file_name: str) -> int:
    """Rewrite any queued events that reference an expiring attachment file so
    they remain coherent once the file is gone: remove the attachment from the
    event and append a note to the message text. Returns the number of events
    rewritten.

    This handles the legitimate case where a client stopped polling long enough
    (>24h) that we can no longer hold its undelivered attachments. When the
    client resumes, it receives the messages with the stale attachment replaced
    by an explanatory note rather than a broken file_ref that 404s on download.
    """
    import json as _json
    rewritten = 0
    for ev in db.gateway_queue_all():
        try:
            env = _json.loads(ev["event_json"])
        except Exception:
            continue
        payload = env.get("payload", {}) or {}
        atts = payload.get("attachments") or []
        if not atts:
            continue
        keep = [a for a in atts if a.get("file_ref") != file_ref]
        if len(keep) == len(atts):
            continue  # this event doesn't reference the expiring file
        # Remove the stale attachment and annotate the text.
        payload["attachments"] = keep
        note = (
            f"[attachment '{file_name}' expired: it was not retrieved within "
            f"{int(LEFTOVER_AGE_SECONDS // 3600)} hours and is no longer available]"
        )
        existing = payload.get("text") or ""
        payload["text"] = (existing + ("\n" if existing else "") + note)
        env["payload"] = payload
        try:
            db.gateway_update_event(ev["id"], _json.dumps(env))
            rewritten += 1
        except Exception as e:
            logger.warning("Could not rewrite event %s for expired file %s: %s",
                           ev["id"], file_ref, e)
    return rewritten


def sweep_leftovers() -> int:
    """Delete gateway files older than LEFTOVER_AGE_SECONDS. For each, first
    rewrite any queued events that still reference it (removing the attachment
    and annotating the message) so a client that resumes polling after a long
    outage gets coherent messages rather than broken file_refs.

    Two cases:
      • The file is still referenced by queued (undelivered) event(s): an
        expected consequence of a client not polling for >24h. Logged at WARNING
        and the events are rewritten.
      • The file is referenced by NO queued event: a genuine leftover that should
        not happen (delivered/acked but not deleted). Logged at ERROR.
    Returns the number of files swept. Call from the daily maintenance task.
    """
    now = time.time()
    swept = 0
    for row in db.gateway_files_all():
        path = row.get("path", "")
        created = row.get("created_at", 0) or 0
        age = now - created
        if age < LEFTOVER_AGE_SECONDS:
            continue
        file_ref = row.get("file_ref")
        file_name = row.get("file_name", "") or "attachment"

        # First, rewrite any queued events that reference this file so they stay
        # coherent after the file is gone.
        rewritten = _expire_attachment_in_events(file_ref, file_name)

        # Gather filesystem timestamps if the file is still present.
        st = None
        if path and os.path.isfile(path):
            try:
                st = os.stat(path)
            except OSError:
                st = None
        details = [
            f"file_ref={file_ref}",
            f"gateway={row.get('gateway')}",
            f"file_name={file_name!r}",
            f"size={row.get('size')}",
            f"ref_created={_fmt_local(created)}",
            f"events_rewritten={rewritten}",
        ]
        if st is not None:
            details.append(f"fs_mtime={_fmt_local(st.st_mtime)}")
            details.append(f"fs_ctime={_fmt_local(st.st_ctime)}")
            details.append(f"fs_atime={_fmt_local(st.st_atime)}")
        else:
            details.append("fs=missing-on-disk")

        if rewritten > 0:
            # Expected case: a client didn't poll in time. WARNING, not ERROR.
            logger.warning(
                "Gateway attachment expired (age %.1fh) — client did not retrieve "
                "it in time; %d queued event(s) rewritten to note the loss: %s",
                age / 3600.0, rewritten, " | ".join(details),
            )
        else:
            # Genuine leftover: no queued event references it. Should not happen.
            logger.error(
                "Gateway file leftover (age %.1fh) — should not happen; investigate: %s",
                age / 3600.0, " | ".join(details),
            )
        delete_file(file_ref)
        swept += 1
    if swept:
        logger.info("Gateway file sweep removed %d file(s).", swept)
    return swept
