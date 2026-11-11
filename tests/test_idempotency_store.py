"""Contract tests: both backends behave identically."""

import pytest

from irlab.idempotency import ClaimState, InMemoryIdempotencyStore, RedisIdempotencyStore, build_idempotency_store
from tests.conftest import redis_client


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture(params=["memory", "redis"])
async def store(request):
    if request.param == "memory":
        s = InMemoryIdempotencyStore()
    else:
        s = RedisIdempotencyStore(redis_client())
    yield s
    await s.aclose()


async def test_first_claim_is_new_second_is_in_progress(store):
    assert await store.begin("k", 30) is ClaimState.NEW
    assert await store.begin("k", 30) is ClaimState.IN_PROGRESS


async def test_completed_key_reports_done(store):
    await store.begin("k", 30)
    await store.complete("k", 3600)
    assert await store.begin("k", 30) is ClaimState.DONE


async def test_released_key_can_be_claimed_again(store):
    await store.begin("k", 30)
    await store.release("k")
    assert await store.begin("k", 30) is ClaimState.NEW


async def test_keys_are_independent(store):
    assert await store.begin("a", 30) is ClaimState.NEW
    assert await store.begin("b", 30) is ClaimState.NEW


async def test_ping(store):
    assert await store.ping() is True


async def test_memory_processing_claim_expires_after_lock_ttl():
    clock = FakeClock()
    store = InMemoryIdempotencyStore(clock=clock)
    assert await store.begin("k", 30) is ClaimState.NEW
    clock.t += 29
    assert await store.begin("k", 30) is ClaimState.IN_PROGRESS
    clock.t += 2  # the claiming worker "crashed": its lock expires, a retry may proceed
    assert await store.begin("k", 30) is ClaimState.NEW


async def test_memory_done_expires_after_ttl():
    clock = FakeClock()
    store = InMemoryIdempotencyStore(clock=clock)
    await store.begin("k", 30)
    await store.complete("k", 100)
    clock.t += 101
    assert await store.begin("k", 30) is ClaimState.NEW


async def test_memory_purges_expired_entries():
    clock = FakeClock()
    store = InMemoryIdempotencyStore(clock=clock, purge_every=10)
    for i in range(9):
        await store.begin(f"k{i}", 1)
    clock.t += 5
    await store.begin("trigger", 1)  # 10th op purges the 9 expired claims
    assert list(store._entries) == ["trigger"]


async def test_redis_keys_have_ttls():
    client = redis_client()
    store = RedisIdempotencyStore(client, prefix="t:")
    await store.begin("k", 30)
    assert 0 < await client.ttl("t:k") <= 30
    await store.complete("k", 3600)
    assert 30 < await client.ttl("t:k") <= 3600
    await store.aclose()


async def test_redis_ping_false_when_unreachable():
    store = RedisIdempotencyStore.from_url("redis://127.0.0.1:1/0")  # nothing listens on port 1
    assert await store.ping() is False
    await store.aclose()


async def test_builder_picks_backend():
    memory = build_idempotency_store("")
    redis_store = build_idempotency_store("redis://localhost:6379/0")  # lazy: no connection is made
    assert (memory.backend, redis_store.backend) == ("memory", "redis")
    await redis_store.aclose()
