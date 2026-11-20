"""Snapshots: save the whole dataset to a file, and load it back at startup.

The file is JSON (binary values are base64) so it is easy to inspect. Writes are atomic:
the data goes to a temporary file first, then replaces the real one in a single step, so a
crash in the middle of saving can never leave a half-written snapshot behind.
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
from collections import deque

from .store import Store, kind_of

FORMAT_VERSION = 1


class PersistenceError(Exception):
    """The snapshot file is unreadable or not in the expected format."""


def _enc(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _dec(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"), validate=True)


def snapshot(store: Store) -> dict:
    entries = []
    for key in list(store.data):
        value = store.get(key)  # also skips anything that has just expired
        if value is None:
            continue
        kind = kind_of(value)
        if kind == "string":
            payload = _enc(value)
        elif kind == "list":
            payload = [_enc(item) for item in value]
        elif kind == "hash":
            payload = {_enc(field): _enc(item) for field, item in value.items()}
        else:
            payload = sorted(_enc(item) for item in value)
        entries.append({"key": _enc(key), "type": kind, "value": payload, "expire_at_ms": store.expires.get(key)})
    return {"version": FORMAT_VERSION, "saved_at_ms": store.now_ms(), "entries": entries}


def save(store: Store, path: str) -> int:
    """Write a snapshot atomically. Returns the number of keys saved."""
    data = snapshot(store)
    directory = os.path.dirname(os.path.abspath(path))
    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".snapshot-", suffix=".tmp")
    try:
        with os.fdopen(handle, "w") as out:
            json.dump(data, out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp_path, path)
    except BaseException:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return len(data["entries"])


def load(store: Store, path: str) -> int:
    """Load a snapshot into `store`. A missing file is fine (first run). Returns keys loaded."""
    if not os.path.exists(path):
        return 0
    try:
        with open(path) as handle:
            data = json.load(handle)
        if data.get("version") != FORMAT_VERSION:
            raise PersistenceError(f"unsupported snapshot version {data.get('version')!r}")
        now, loaded = store.now_ms(), 0
        for entry in data["entries"]:
            deadline = entry["expire_at_ms"]
            if deadline is not None and deadline <= now:
                continue  # it expired while the server was off
            kind, payload = entry["type"], entry["value"]
            if kind == "string":
                value = _dec(payload)
            elif kind == "list":
                value = deque(_dec(item) for item in payload)
            elif kind == "hash":
                value = {_dec(field): _dec(item) for field, item in payload.items()}
            elif kind == "set":
                value = {_dec(item) for item in payload}
            else:
                raise PersistenceError(f"unknown type {kind!r}")
            store.set(_dec(entry["key"]), value, expire_at_ms=deadline)
            loaded += 1
        return loaded
    except PersistenceError:
        raise
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise PersistenceError(f"could not read snapshot {path!r}: {exc}") from exc
