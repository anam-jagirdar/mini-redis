"""The in-memory data store: keys, values and expiry times.

Keys and string values are `bytes`. A key holds exactly one of:
    string -> bytes        list -> collections.deque
    hash   -> dict         set  -> set

Empty lists, hashes and sets never exist: commands delete the key when the last item goes.

Expiry works the way real Redis does it, in two layers:
  * lazy:   an expired key is removed the moment anything touches it
  * active: a background task calls `purge_expired` to sweep keys nobody asks for
"""
from __future__ import annotations

import fnmatch
import random
import time
from collections import deque

_KINDS = ((bytes, "string"), (deque, "list"), (dict, "hash"), (set, "set"))


class WrongType(Exception):
    """A command was used on a key that holds a different kind of value."""


def kind_of(value) -> str:
    for python_type, name in _KINDS:
        if isinstance(value, python_type):
            return name
    raise TypeError(f"unsupported value type {type(value).__name__}")


class Store:
    def __init__(self, clock=time.time) -> None:
        self._clock = clock  # injectable, so tests can control time
        self.data: dict[bytes, object] = {}
        self.expires: dict[bytes, int] = {}  # key -> deadline in epoch milliseconds

    def now_ms(self) -> int:
        return int(self._clock() * 1000)

    # -- reading -----------------------------------------------------------------------
    def _drop_if_expired(self, key: bytes) -> bool:
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= self.now_ms():
            self.delete(key)
            return True
        return False

    def get(self, key: bytes, expected: type | None = None):
        """The value stored at `key`, or None. Raises WrongType if it is not an `expected`."""
        if self._drop_if_expired(key):
            return None
        value = self.data.get(key)
        if value is not None and expected is not None and not isinstance(value, expected):
            raise WrongType
        return value

    def exists(self, key: bytes) -> bool:
        return self.get(key) is not None

    def type_of(self, key: bytes) -> str:
        value = self.get(key)
        return "none" if value is None else kind_of(value)

    def keys(self, pattern: bytes = b"*") -> list[bytes]:
        glob = pattern.decode("latin-1")
        return [
            key
            for key in list(self.data)
            if not self._drop_if_expired(key) and fnmatch.fnmatchcase(key.decode("latin-1"), glob)
        ]

    def dbsize(self) -> int:
        return sum(1 for key in list(self.data) if not self._drop_if_expired(key))

    # -- writing -----------------------------------------------------------------------
    def set(self, key: bytes, value, expire_at_ms: int | None = None, keep_ttl: bool = False) -> None:
        self.data[key] = value
        if expire_at_ms is not None:
            self.expires[key] = expire_at_ms
        elif not keep_ttl:
            self.expires.pop(key, None)

    def delete(self, key: bytes) -> bool:
        existed = key in self.data
        self.data.pop(key, None)
        self.expires.pop(key, None)
        return existed

    def clear(self) -> None:
        self.data.clear()
        self.expires.clear()

    # -- expiry ------------------------------------------------------------------------
    def ttl_ms(self, key: bytes) -> int:
        """Milliseconds left to live. -2 if the key does not exist, -1 if it never expires."""
        if not self.exists(key):
            return -2
        deadline = self.expires.get(key)
        return -1 if deadline is None else max(0, deadline - self.now_ms())

    def set_expiry(self, key: bytes, deadline_ms: int) -> bool:
        if not self.exists(key):
            return False
        self.expires[key] = deadline_ms
        return True

    def persist(self, key: bytes) -> bool:
        if not self.exists(key):
            return False
        return self.expires.pop(key, None) is not None

    def purge_expired(self, sample: int = 100) -> int:
        """Active expiry: check a random sample of keys that have a deadline."""
        if not self.expires:
            return 0
        candidates = random.sample(list(self.expires), min(sample, len(self.expires)))
        return sum(1 for key in candidates if self._drop_if_expired(key))
