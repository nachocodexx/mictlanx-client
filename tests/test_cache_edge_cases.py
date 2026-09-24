import array
import pytest
from mictlanx.caching import CacheFactory, LRUCache, LFUCache
from mictlanx.caching.stats import AccessStats


class FakeClock:
    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture(params=["LRU", "LFU"])
def cache(request):
    return CacheFactory.create(request.param, capacity_storage=100)


# --- Byte accounting ---

def test_overwrite_replaces_size(cache, sample_metadata):
    cache.put("k", b"a" * 40, sample_metadata)
    cache.put("k", b"b" * 10, sample_metadata)
    assert cache.get_used_storage_capacity() == 10
    cache.put("k", b"c" * 60, sample_metadata)
    assert cache.get_used_storage_capacity() == 60
    assert len(cache) == 1


def test_evicts_multiple_entries_to_fit(cache, sample_metadata):
    for i in range(5):
        cache.put(f"k{i}", b"x" * 20, sample_metadata)
    assert cache.get_used_storage_capacity() == 100
    assert cache.put("big", b"y" * 70, sample_metadata) == 0
    assert cache.get("big").is_some
    assert cache.get_used_storage_capacity() <= 100
    assert len(cache) == 2  # needed to drop 4 entries of 20 bytes


def test_oversized_value_rejected(cache, sample_metadata):
    cache.put("small", b"s" * 10, sample_metadata)
    assert cache.put("huge", b"h" * 101, sample_metadata) == -1
    assert cache.get("huge").is_none
    assert cache.get("small").is_some  # nothing evicted for a rejected value
    assert cache.get_used_storage_capacity() == 10


def test_used_never_exceeds_capacity(cache, sample_metadata):
    for i in range(50):
        cache.put(f"k{i % 7}", b"z" * (i % 30 + 1), sample_metadata)
        assert cache.get_used_storage_capacity() <= cache.get_total_storage_capacity()
        total = sum(len(v) for _, v in cache.cache.values())
        assert cache.get_used_storage_capacity() == total


def test_nbytes_for_non_byte_memoryview(cache, sample_metadata):
    ints = array.array("i", [1, 2, 3, 4])  # 4 elements, 16 bytes
    mv   = memoryview(ints)
    assert len(mv) == 4 and mv.nbytes == 16
    cache.put("ints", mv, sample_metadata)
    assert cache.get_used_storage_capacity() == 16
    _, val = cache.get("ints").unwrap()
    assert val.nbytes == 16 and val.tobytes() == ints.tobytes()


def test_returned_view_is_readonly_and_detached(cache, sample_metadata):
    src = bytearray(b"mutable")
    cache.put("k", src, sample_metadata)
    src[0:1] = b"X"
    _, val = cache.get("k").unwrap()
    assert val.readonly
    assert val.tobytes() == b"mutable"


def test_uf_zero_capacity(sample_metadata):
    for policy in ("LRU", "LFU"):
        c = CacheFactory.create(policy, capacity_storage=0)
        assert c.get_uf() == 0.0
        assert c.put("k", b"x", sample_metadata) == -1


# --- LFU heap ---

def test_lfu_heap_bounded(sample_metadata):
    c = LFUCache(capacity_storage=100)
    c.put("a", b"a" * 10, sample_metadata)
    c.put("b", b"b" * 10, sample_metadata)
    for _ in range(10_000):
        c.get("a")
    assert len(c.freq_heap) <= 2 * len(c) + 17


def test_lfu_remove_then_reinsert_resets_frequency(sample_metadata):
    c = LFUCache(capacity_storage=20)
    c.put("a", b"a" * 10, sample_metadata)
    for _ in range(5):
        c.get("a")
    c.remove("a")
    c.put("a", b"a" * 10, sample_metadata)   # freq 1 again
    c.put("b", b"b" * 10, sample_metadata)
    c.get("b")                                # b freq 2
    c.put("c", b"c" * 10, sample_metadata)    # must evict a (stale heap entries ignored)
    assert c.get("a").is_none
    assert c.get("b").is_some


def test_lfu_with_decaying_scorer_evicts_old_popular(sample_metadata):
    clock = FakeClock()
    stats = AccessStats(half_life_s=10, clock=clock)
    c     = CacheFactory.create("LFU", capacity_storage=20, scorer=stats.rank)
    assert isinstance(c, LFUCache)

    c.put("old", b"o" * 10, sample_metadata)
    for _ in range(100):                       # very popular at t=0
        stats.record_get("old", hit=True)
    clock.t = 200                              # 20 half-lives later: score ~ 100 / 2^20
    c.put("new", b"n" * 10, sample_metadata)
    stats.record_get("new", hit=True)
    stats.record_get("new", hit=True)          # score 2 now

    c.put("third", b"t" * 10, sample_metadata)  # evicts the lowest current score
    assert c.get("old").is_none
    assert c.get("new").is_some


def test_lru_still_default_for_unknown_policy():
    assert isinstance(CacheFactory.create("???", capacity_storage=10), LRUCache)
