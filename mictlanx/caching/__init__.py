from typing import Callable, Dict, List, Optional, OrderedDict as ODT, Tuple, Union
from abc import ABC, abstractmethod
from collections import OrderedDict, Counter
import heapq
import itertools
from mictlanx.interfaces import Metadata
from mictlanx.logger import Log
from option import Option, Some, NONE

log = Log(name=__name__)

# A scorer maps a cache key to a time-invariant eviction rank (higher = more valuable).
Scorer = Callable[[str], float]
BytesLike = Union[bytes, bytearray, memoryview]


class CacheX(ABC):
    """Abstract Base Class for a byte-budget-bounded key-value cache.

    Values are stored as immutable ``bytes`` and returned as read-only
    ``memoryview`` objects together with their ``Metadata``.
    """
    @abstractmethod
    def get_keys(self)->List[str]:
        """Return all keys currently stored in the cache.

        Returns:
            A list of cache key strings.
        """
        pass
    @abstractmethod
    def get(self, key: str) -> Option[Tuple[Metadata, memoryview]]:
        """Retrieve ``(metadata, memoryview)`` for ``key``, or ``NONE``."""
        pass

    @abstractmethod
    def put(self, key: str, value: BytesLike, metadata: Metadata)->int:
        """Insert a value into the cache. Returns ``0`` on success, ``-1`` if rejected."""
        pass

    @abstractmethod
    def remove(self, key: str):
        """Remove a value from the cache."""
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Return the current number of entries in the cache."""
        pass

    @abstractmethod
    def clear(self):
        """Clear the cache."""
        pass
    @abstractmethod
    def get_total_storage_capacity(self):
        """Return the maximum byte capacity of this cache.

        Returns:
            Total capacity in bytes.
        """
        pass

    @abstractmethod
    def get_used_storage_capacity(self):
        """Return the number of bytes currently consumed by cached values.

        Returns:
            Used capacity in bytes.
        """
        pass

    @abstractmethod
    def get_uf(self):
        """Return the utilisation factor of this cache (0.0 = empty, 1.0 = full).

        Returns:
            Float in ``[0.0, 1.0]`` representing the fraction of capacity used.
        """
        pass

class CacheFactory:
    """Factory for creating byte-budget-bounded in-memory caches."""

    @staticmethod
    def create(eviction_policy:str, capacity_storage:int, scorer:Optional[Scorer] = None):
        """Instantiate a cache with the given eviction policy and byte budget.

        Args:
            eviction_policy: ``"LRU"`` for least-recently-used eviction or
                ``"LFU"`` for least-frequently-used.  Any other value falls
                back to LRU.
            capacity_storage: Maximum number of bytes the cache may hold
                before evicting entries.
            scorer: Optional LFU rank function (e.g. ``AccessStats.rank``).
                Ignored by LRU.

        Returns:
            A ``CacheX`` instance (``LRUCache`` or ``LFUCache``).
        """
        if eviction_policy == "LFU":
            return LFUCache(capacity_storage = capacity_storage, scorer = scorer)
        return LRUCache(capacity_storage=capacity_storage)


class _ByteBudgetCache(CacheX):
    """Shared byte accounting for LRU/LFU.

    Guarantees ``used_capacity <= capacity_storage``: oversized values are
    rejected, overwrites replace the old size, and eviction loops until the
    new value fits.  Subclasses provide ``_pick_victim`` and the ``_on_*`` hooks.
    """

    def __init__(self, capacity_storage:int):
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache: Dict[str, Tuple[Metadata, bytes]] = {}

    @staticmethod
    def _as_bytes(value: BytesLike) -> bytes:
        if type(value) is bytes:
            return value
        # Zero-copy when the view spans an entire bytes object (e.g. a merged download).
        if isinstance(value, memoryview) and type(value.obj) is bytes and value.nbytes == len(value.obj) and value.c_contiguous:
            return value.obj
        # bytes(memoryview) copies raw bytes, so len() == nbytes for any format.
        return bytes(value)

    def _on_insert(self, key: str, existed: bool): pass
    def _on_hit(self, key: str): pass
    def _on_remove(self, key: str): pass

    @abstractmethod
    def _pick_victim(self) -> Optional[str]:
        pass

    def get_keys(self) -> List[str]:
        """Return all keys currently held in the cache."""
        return list(self.cache.keys())

    def get(self, key: str) -> Option[Tuple[Metadata, memoryview]]:
        """Retrieve ``Some((metadata, memoryview))`` if the key exists, else ``NONE``."""
        entry = self.cache.get(key)
        if entry is None:
            return NONE
        self._on_hit(key)
        metadata, data = entry
        return Some((metadata, memoryview(data)))

    def put(self, key: str, value: BytesLike, metadata: Metadata) -> int:
        """Insert or replace a value, evicting entries until it fits.

        Args:
            key: Cache key.
            value: Bytes-like value; stored as immutable ``bytes``.
            metadata: ``Metadata`` object associated with the value.

        Returns:
            ``0`` on success, ``-1`` if the value is larger than the whole cache
            or an error occurs.
        """
        try:
            data = self._as_bytes(value)
            size = len(data)
            if size > self.capacity_storage:
                log.warning({
                    "event": "CACHE.PUT.REJECTED",
                    "message": "value larger than cache capacity",
                    "key": key,
                    "context": {"size": size, "capacity": self.capacity_storage},
                })
                return -1
            existed = key in self.cache
            if existed:
                self.used_capacity -= len(self.cache.pop(key)[1])
            self._evict_until(size)
            self.cache[key]     = (metadata, data)
            self.used_capacity += size
            self._on_insert(key, existed)
            return 0
        except Exception as e:
            log.error({
                "event": "CACHE.PUT.ERROR",
                "message": str(e),
                "key": key,
                "error_type": type(e).__name__,
            })
            return -1

    def _evict_until(self, size: int):
        while self.cache and self.used_capacity + size > self.capacity_storage:
            victim = self._pick_victim()
            if victim is None:
                break
            self.remove(victim)

    def remove(self, key: str):
        """Remove a key and reclaim its byte budget. No-op if absent."""
        entry = self.cache.pop(key, None)
        if entry is not None:
            self.used_capacity -= len(entry[1])
            self._on_remove(key)

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        """Remove all entries and reset the byte counter."""
        self.cache.clear()
        self.used_capacity = 0

    def get_total_storage_capacity(self):
        """Return the maximum byte capacity."""
        return self.capacity_storage

    def get_used_storage_capacity(self):
        """Return the number of bytes currently occupied by cached values."""
        return self.used_capacity

    def get_uf(self):
        """Return the utilisation factor in ``[0.0, 1.0]`` (``0.0`` when capacity is 0)."""
        if self.capacity_storage <= 0:
            return 0.0
        return self.used_capacity / self.capacity_storage


class LRUCache(_ByteBudgetCache):
    """LRU (Least Recently Used) cache: evicts the entry accessed longest ago."""

    def __init__(self, capacity_storage:int):
        super().__init__(capacity_storage)
        self.cache: ODT[str, Tuple[Metadata, bytes]] = OrderedDict()

    def _on_insert(self, key: str, existed: bool):
        self.cache.move_to_end(key)

    def _on_hit(self, key: str):
        self.cache.move_to_end(key)

    def _pick_victim(self) -> Optional[str]:
        return next(iter(self.cache), None)


class LFUCache(_ByteBudgetCache):
    """LFU (Least Frequently Used) cache backed by a lazily-invalidated min-heap.

    Without a ``scorer`` the rank is the local access counter (a put counts 1,
    each get adds 1).  With a ``scorer`` (e.g. ``AccessStats.rank``) the rank is
    the decayed access score, so entries that were popular long ago age out.
    """

    def __init__(self, capacity_storage:int, scorer:Optional[Scorer] = None):
        super().__init__(capacity_storage)
        self.scorer       = scorer
        self.freq_counter = Counter()  # Key -> local access count
        self.freq_heap: List[Tuple[float, int, str]] = []
        self._seq         = itertools.count()

    def _rank(self, key: str) -> float:
        if self.scorer is not None:
            return self.scorer(key)
        return float(self.freq_counter[key])

    def _push(self, key: str):
        heapq.heappush(self.freq_heap, (self._rank(key), next(self._seq), key))
        if len(self.freq_heap) > 2 * len(self.cache) + 16:
            self._rebuild_heap()

    def _rebuild_heap(self):
        self.freq_heap = [(self._rank(k), next(self._seq), k) for k in self.cache]
        heapq.heapify(self.freq_heap)

    def _on_insert(self, key: str, existed: bool):
        self.freq_counter[key] = self.freq_counter[key] + 1 if existed else 1
        self._push(key)

    def _on_hit(self, key: str):
        self.freq_counter[key] += 1
        self._push(key)

    def _on_remove(self, key: str):
        self.freq_counter.pop(key, None)

    def _pick_victim(self) -> Optional[str]:
        while self.freq_heap:
            rank, _, key = heapq.heappop(self.freq_heap)
            if key not in self.cache:
                continue
            current = self._rank(key)
            if current != rank:
                # Stale entry (rank changed since it was pushed): re-queue with the current rank.
                heapq.heappush(self.freq_heap, (current, next(self._seq), key))
                continue
            return key
        # Heap exhausted (should not happen): fall back to any key.
        return next(iter(self.cache), None)

    def clear(self):
        """Remove all entries and reset frequency tracking."""
        super().clear()
        self.freq_counter.clear()
        self.freq_heap.clear()


class NoCache(CacheX):
    """No-op cache that never stores anything.

    Useful as a drop-in when caching should be disabled without changing
    calling code.  All lookups return ``NONE``; all writes are ignored.
    """

    def get(self, key):
        return NONE

    def put(self, key, value, metadata=None):
        return None

    def remove(self, key):
        pass

    def clear(self):
        pass

    def __len__(self):
        return 0

    def get_keys(self) -> List[str]:
        return []

    def get_total_storage_capacity(self) -> int:
        return 0

    def get_used_storage_capacity(self) -> int:
        return 0

    def get_uf(self) -> float:
        return 0.0
