"""Fast-path idempotency store in front of the inbox's unique constraint.

A key moves through three states:

    (absent) --begin--> processing --complete--> done --(ttl)--> (absent)
                            |
                            +--release / lock ttl--> (absent)

* `processing` has a short TTL. If the worker that claimed it crashes, the claim
  expires and the sender's next retry is processed normally: a crash can never turn
  a delivery into a silently dropped "duplicate".
* `done` has a long TTL so redeliveries are answered without touching the inbox
  table's insert path.
* If the store is flushed, expires a key, or is down, the hub falls back to the
  database unique constraint. The store makes duplicates cheap; it does not make
  them correct.
"""

import time
from collections.abc import Callable
from enum import StrEnum
from typing import Protocol

from redis.asyncio import Redis


class ClaimState(StrEnum):
    NEW = "new"  # caller owns the key and must complete() or release() it
    IN_PROGRESS = "in_progress"  # another delivery holds the claim right now
    DONE = "done"  # already processed


class IdempotencyStore(Protocol):
    backend: str

    async def begin(self, key: str, lock_ttl_s: int) -> ClaimState: ...
    async def complete(self, key: str, ttl_s: int) -> None: ...
    async def release(self, key: str) -> None: ...
    async def ping(self) -> bool: ...
    async def aclose(self) -> None: ...


_PROCESSING = "processing"
_DONE = "done"


class InMemoryIdempotencyStore:
    """Single-process store for local runs and tests. Not shared between workers."""

    backend = "memory"

    def __init__(self, clock: Callable[[], float] = time.monotonic, purge_every: int = 1000):
        self._clock = clock
        self._entries: dict[str, tuple[str, float]] = {}
        self._ops = 0
        self._purge_every = purge_every

    # No awaits inside: each method runs atomically on the event loop.
    async def begin(self, key: str, lock_ttl_s: int) -> ClaimState:
        now = self._clock()
        self._maybe_purge(now)
        entry = self._entries.get(key)
        if entry is not None and entry[1] > now:
            return ClaimState.DONE if entry[0] == _DONE else ClaimState.IN_PROGRESS
        self._entries[key] = (_PROCESSING, now + lock_ttl_s)
        return ClaimState.NEW

    async def complete(self, key: str, ttl_s: int) -> None:
        self._entries[key] = (_DONE, self._clock() + ttl_s)

    async def release(self, key: str) -> None:
        self._entries.pop(key, None)

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self._entries.clear()

    def clear(self) -> None:
        self._entries.clear()

    def _maybe_purge(self, now: float) -> None:
        self._ops += 1
        if self._ops % self._purge_every == 0:
            self._entries = {k: v for k, v in self._entries.items() if v[1] > now}


class RedisIdempotencyStore:
    """Shared store for multiple hub workers (Docker Compose)."""

    backend = "redis"

    def __init__(self, client: Redis, prefix: str = "irlab:idem:"):
        self._r = client
        self._prefix = prefix

    @classmethod
    def from_url(cls, url: str) -> "RedisIdempotencyStore":
        return cls(Redis.from_url(url, socket_timeout=0.5, socket_connect_timeout=0.5))

    async def begin(self, key: str, lock_ttl_s: int) -> ClaimState:
        k = self._prefix + key
        # Two attempts cover the race where the claim expires between SET NX and GET.
        for _ in range(2):
            if await self._r.set(k, _PROCESSING, nx=True, ex=lock_ttl_s):
                return ClaimState.NEW
            value = await self._r.get(k)
            if value is not None:
                return ClaimState.DONE if value in (_DONE, _DONE.encode()) else ClaimState.IN_PROGRESS
        return ClaimState.IN_PROGRESS

    async def complete(self, key: str, ttl_s: int) -> None:
        await self._r.set(self._prefix + key, _DONE, ex=ttl_s)

    async def release(self, key: str) -> None:
        # Not compare-and-delete: if our lock already expired and someone else holds it,
        # deleting it at worst lets a third delivery race to the unique constraint, which
        # still rejects the duplicate.
        await self._r.delete(self._prefix + key)

    async def ping(self) -> bool:
        try:
            return bool(await self._r.ping())
        except Exception:
            return False

    async def aclose(self) -> None:
        await self._r.aclose()


def build_idempotency_store(redis_url: str) -> IdempotencyStore:
    return RedisIdempotencyStore.from_url(redis_url) if redis_url else InMemoryIdempotencyStore()
