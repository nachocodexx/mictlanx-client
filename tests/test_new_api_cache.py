"""Happy paths for the cache, AccessStats and the mictlanx.objects Bucket/Ball API.

Unit tests need no infrastructure (fake clock, respx-mocked router, in-memory
fake client).  Tests marked ``integration`` need a live VSS (see ``.env.test``).
"""
import math
import os
import time
import httpx
import pytest
import respx
from option import Ok, Err
from xolo.utils.utils import Utils as XoloUtils

import mictlanx.errors as EX
from mictlanx.asyncx import AsyncClient
from mictlanx.caching import CacheFactory, LFUCache
from mictlanx.caching.stats import AccessStats
from mictlanx.interfaces.index import Ball as IndexBall, Bucket as IndexBucket
from mictlanx.interfaces.responses import AsyncGetResponse, Metadata
from mictlanx.objects import Ball, Bucket, BallConflictError, NoActiveClientError


class FakeClock:
    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


# ─── Cache ────────────────────────────────────────────────────────────────────

def test_cache_put_get_returns_readonly_view(sample_metadata):
    cache = CacheFactory.create("LFU", capacity_storage=1024)
    assert cache.put("bk/b1", b"hello", sample_metadata) == 0
    meta, view = cache.get("bk/b1").unwrap()
    assert view.readonly and view.tobytes() == b"hello"
    assert meta is sample_metadata


def test_lfu_keeps_most_read_key(sample_metadata):
    cache = CacheFactory.create("LFU", capacity_storage=20)
    cache.put("hot", b"h" * 10, sample_metadata)
    cache.put("cold", b"c" * 10, sample_metadata)
    for _ in range(3):
        cache.get("hot")
    cache.put("new", b"n" * 10, sample_metadata)
    assert cache.get("hot").is_some
    assert cache.get("cold").is_none


def test_lfu_ranks_with_access_stats_scorer(sample_metadata):
    stats = AccessStats(half_life_s=60, clock=FakeClock())
    cache = CacheFactory.create("LFU", capacity_storage=20, scorer=stats.rank)
    assert isinstance(cache, LFUCache)
    cache.put("a", b"a" * 10, sample_metadata)
    cache.put("b", b"b" * 10, sample_metadata)
    for _ in range(5):
        stats.record_get("a", hit=True)
    stats.record_get("b", hit=True)
    cache.put("c", b"c" * 10, sample_metadata)
    assert cache.get("a").is_some
    assert cache.get("b").is_none


# ─── AccessStats ──────────────────────────────────────────────────────────────

def test_stats_counters_and_hit_miss_split():
    stats = AccessStats(clock=FakeClock())
    stats.record_put("bk/b1", nbytes=100)
    stats.record_get("bk/b1", hit=False, nbytes=100)
    stats.record_get("bk/b1", hit=True, nbytes=100)
    s = stats.get("bk/b1")
    assert (s.num_puts, s.num_gets, s.hits, s.misses) == (1, 2, 1, 1)
    assert (s.bytes_written, s.bytes_read) == (100, 200)
    assert stats.get("unknown").num_gets == 0


def test_score_halves_after_one_half_life():
    clock = FakeClock()
    stats = AccessStats(half_life_s=600, clock=clock)
    for _ in range(8):
        stats.record_get("k", hit=True)
    assert stats.score("k") == pytest.approx(8)
    clock.t = 600
    assert stats.score("k") == pytest.approx(4)
    clock.t = 1200
    assert stats.score("k") == pytest.approx(2)
    assert stats.get("k").num_gets == 8  # lifetime count never decays


def test_freq_approximates_steady_rate():
    clock = FakeClock()
    stats = AccessStats(half_life_s=60, clock=clock)
    for _ in range(3000):          # 2 gets/s for 25 minutes (25 half-lives)
        clock.t += 0.5
        stats.record_get("k", hit=True)
    assert stats.freq("k") == pytest.approx(2.0, rel=0.05)
    assert stats.tau == pytest.approx(60 / math.log(2))


# ─── AsyncClient.get with the cache (router mocked with respx) ───────────────

MOCK_URI  = "mictlanx://mictlanx-router-0@localhost:61234/?protocol=http&api_version=4&http2=0"
MOCK_BASE = "http://localhost:61234/api/v4/buckets"


def _mock_client(**kwargs) -> AsyncClient:
    return AsyncClient(uri=MOCK_URI, client_id="test-new-api", debug=False, enable_logging=False, **kwargs)


def _mock_ball(bucket_id: str, ball_id: str, data: bytes):
    """Register respx routes for a single-chunk ball; returns the data route."""
    meta = {
        "service_time": 0, "peer_id": "peer-0", "local_peer_id": "peer-0",
        "metadata": {
            "key": f"{ball_id}_0", "size": len(data), "checksum": XoloUtils.sha256(data),
            "tags": {"index": "0", "num_chunks": "1", "full_checksum": XoloUtils.sha256(data), "mictlanx_filters": ""},
            "content_type": "application/octet-stream", "producer_id": "p", "ball_id": ball_id, "bucket_id": bucket_id,
        },
    }
    respx.get(f"{MOCK_BASE}/{bucket_id}/metadata/{ball_id}_0").mock(return_value=httpx.Response(200, json=meta))
    return respx.get(f"{MOCK_BASE}/{bucket_id}/{ball_id}_0").mock(return_value=httpx.Response(200, content=data))


@pytest.mark.asyncio
@respx.mock
async def test_client_get_miss_then_hit():
    client     = _mock_client()
    data_route = _mock_ball("bk", "b1", b"cached payload")

    first  = (await client.get("bk", "b1", cache=True)).unwrap()
    second = (await client.get("bk", "b1", cache=True)).unwrap()

    assert first.data.tobytes() == second.data.tobytes() == b"cached payload"
    assert data_route.call_count == 1               # second read never downloaded
    s = client.stats.get("bk/b1")
    assert (s.num_gets, s.misses, s.hits) == (2, 1, 1)
    assert client.cache.get_used_storage_capacity() == len(b"cached payload")


@pytest.mark.asyncio
@respx.mock
async def test_client_get_without_cache_by_default():
    client     = _mock_client()
    data_route = _mock_ball("bk", "b2", b"not cached")

    await client.get("bk", "b2")
    await client.get("bk", "b2")

    assert data_route.call_count == 2
    assert len(client.cache) == 0
    assert client.stats.get("bk/b2").num_gets == 2  # stats are always recorded


@pytest.mark.asyncio
@respx.mock
async def test_client_cache_default_and_force():
    client     = _mock_client(cache_default=True)
    data_route = _mock_ball("bk", "b3", b"force me")

    await client.get("bk", "b3")
    await client.get("bk", "b3")
    await client.get("bk", "b3", force=True)
    assert data_route.call_count == 2


# ─── Bucket/Ball handles over an in-memory fake client ────────────────────────

class FakeAsyncClient:
    """Minimal in-memory stand-in exposing the AsyncClient methods the handles use."""

    def __init__(self):
        self.stats   = AccessStats(clock=FakeClock())
        self.objects = {}   # (bucket_id, ball_id) -> (bytes, tags)

    def _metadata(self, bucket_id, ball_id):
        data, tags = self.objects[(bucket_id, ball_id)]
        checksum   = XoloUtils.sha256(data)
        return Metadata(
            key=f"{ball_id}_0", size=len(data), checksum=checksum,
            tags={**tags, "index": "0", "num_chunks": "1", "full_checksum": checksum, "updated_at": "1700000000000"},
            content_type="application/octet-stream", producer_id="fake", ball_id=ball_id, bucket_id=bucket_id,
        )

    async def put(self, bucket_id, ball_id, value, tags=None, **kwargs):
        self.objects[(bucket_id, ball_id)] = (bytes(value), dict(tags or {}))
        self.stats.record_put(AccessStats.key(bucket_id, ball_id), nbytes=len(value))
        return Ok(True)

    async def get(self, bucket_id, ball_id, **kwargs):
        if (bucket_id, ball_id) not in self.objects:
            return Err(EX.NotFoundError())
        data = self.objects[(bucket_id, ball_id)][0]
        self.stats.record_get(AccessStats.key(bucket_id, ball_id), hit=False, nbytes=len(data))
        return Ok(AsyncGetResponse(data=memoryview(data), metadatas=[]))

    async def get_metadata(self, bucket_id, ball_id, **kwargs):
        if (bucket_id, ball_id) not in self.objects:
            return Err(EX.NotFoundError())
        b = IndexBall(bucket_id=bucket_id, ball_id=ball_id, chunks=[self._metadata(bucket_id, ball_id)])
        b.build()
        return Ok(b)

    async def get_bucket_metadata(self, bucket_id, **kwargs):
        balls = {}
        for (bk, ball_id) in self.objects:
            if bk == bucket_id:
                balls[ball_id] = (await self.get_metadata(bk, ball_id)).unwrap()
        return Ok(IndexBucket(bucket_id=bucket_id, balls=balls))

    async def delete(self, ball_id, bucket_id, **kwargs):
        self.objects.pop((bucket_id, ball_id), None)
        return Ok(None)


@pytest.fixture
def fake_bucket():
    return Bucket("bk1", client=FakeAsyncClient())


@pytest.mark.asyncio
async def test_bucket_put_get_roundtrip(fake_bucket):
    ball = await fake_bucket.put("b1", b"hello world", tags={"owner": "nacho"})
    assert isinstance(ball, Ball)
    assert await fake_bucket.get("b1") == b"hello world"
    assert ball.size == len(b"hello world")
    assert ball.data_checksum == XoloUtils.sha256(b"hello world")


@pytest.mark.asyncio
async def test_get_metadata_exposes_only_user_tags(fake_bucket):
    await fake_bucket.put("b1", b"data", tags={"owner": "nacho", "kind": "frame"})
    ball = await fake_bucket.get_metadata("b1")
    assert ball.tags == {"owner": "nacho", "kind": "frame"}
    assert ball.num_chunks == 1
    assert ball.updated_at == 1700000000000
    assert ball.content_type == "application/octet-stream"
    assert ball.chunks[0].key == "b1_0"


@pytest.mark.asyncio
async def test_balls_yields_every_ball(fake_bucket):
    for i in range(3):
        await fake_bucket.put(f"b{i}", f"data-{i}".encode())
    ids = sorted([b.ball_id async for b in fake_bucket.balls()])
    assert ids == ["b0", "b1", "b2"]


@pytest.mark.asyncio
async def test_replicate_adds_puts(fake_bucket):
    ball = await fake_bucket.put("b1", b"replicate me", tags={"t": "1"})
    await ball.replicate(2)
    assert ball.num_puts == 3
    assert (await fake_bucket.get_metadata("b1")).tags == {"t": "1"}


@pytest.mark.asyncio
async def test_same_data_is_replica_different_data_conflicts(fake_bucket):
    await fake_bucket.put("b1", b"v1")
    await fake_bucket.put("b1", b"v1")           # replica: no error
    assert fake_bucket.stats("b1").num_puts == 2
    with pytest.raises(BallConflictError):
        await fake_bucket.put("b1", b"v2")


@pytest.mark.asyncio
async def test_put_many_all_succeed(fake_bucket):
    res = await fake_bucket.put_many([("a", b"1"), ("b", b"2", {"x": "y"}), {"ball_id": "c", "data": b"3"}])
    assert sorted(b.ball_id for b in res.ok) == ["a", "b", "c"]
    assert res.failed == []


@pytest.mark.asyncio
async def test_ball_stats_are_live(fake_bucket):
    ball = await fake_bucket.put("b1", b"x")
    for _ in range(4):
        await fake_bucket.get("b1")
    assert ball.num_gets == 4
    assert ball.freq > 0


@pytest.mark.asyncio
async def test_implicit_bucket_inside_client_context():
    async with _mock_client() as mx:
        bk = Bucket("bk1")
        assert bk.client is mx
        assert mx.bucket("bk2").client is mx
    with pytest.raises(NoActiveClientError):
        Bucket("bk1").client


# ─── Integration (live VSS) ───────────────────────────────────────────────────

@pytest.fixture
def live_client():
    return AsyncClient(
        uri       = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0"),
        client_id = "test-new-api",
        debug     = False,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_get_miss_then_hit(live_client, unique_id):
    bucket_id, ball_id = f"bk-{unique_id[:8]}", f"b-{unique_id[:8]}"
    payload = os.urandom(300 * 1024)
    assert (await live_client.put(bucket_id, ball_id, payload)).is_ok

    t1 = time.monotonic(); first  = (await live_client.get(bucket_id, ball_id, cache=True)).unwrap(); miss_s = time.monotonic() - t1
    t2 = time.monotonic(); second = (await live_client.get(bucket_id, ball_id, cache=True)).unwrap(); hit_s  = time.monotonic() - t2

    assert first.data.tobytes() == second.data.tobytes() == payload
    s = live_client.stats.get(AccessStats.key(bucket_id, ball_id))
    assert (s.misses, s.hits) == (1, 1)
    print(f"miss={miss_s*1000:.1f}ms hit={hit_s*1000:.1f}ms")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_bucket_roundtrip_and_stats(live_client, unique_id):
    async with live_client as mx:
        bk   = mx.bucket(f"bk-{unique_id[:8]}")
        ball = await bk.put("b1", b"hello mictlanx", tags={"owner": "tests"})
        assert ball.tags == {"owner": "tests"}
        for _ in range(3):
            assert await bk.get("b1") == b"hello mictlanx"
        meta = await bk.get_metadata("b1")
        assert meta.checksum == ball.checksum
        assert meta.num_gets == 3 and meta.hits == 2
        assert meta.freq > 0
        with pytest.raises(BallConflictError):
            await bk.put("b1", b"different")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_plain_get_does_not_fill_cache(live_client, unique_id):
    bucket_id, ball_id = f"bk-{unique_id[:8]}", "plain"
    assert (await live_client.put(bucket_id, ball_id, b"plain data")).is_ok
    assert (await live_client.get(bucket_id, ball_id)).is_ok
    assert live_client.cache.get(AccessStats.key(bucket_id, ball_id)).is_none
