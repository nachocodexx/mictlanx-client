import math
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional


@dataclass
class BallStats:
    """Local access statistics for one ``(bucket_id, ball_id)`` seen by this client."""

    num_gets: int = 0
    hits: int = 0
    misses: int = 0
    num_puts: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    first_access: Optional[float] = None
    last_access: Optional[float] = None
    score: float = 0.0     # decayed get score, valid at ``t_score``
    t_score: float = 0.0


class AccessStats:
    """Per-object access tracking for a single client.

    Every get adds 1 to an exponentially decayed score::

        score = score * 2 ** (-dt / half_life) + 1

    Under a steady rate of ``r`` gets/s the score converges to ``r * tau``
    with ``tau = half_life / ln 2``, so :meth:`freq` (``score / tau``) is
    roughly the recent gets per second.  Puts are counted but do not affect
    the score.  Stats are independent of the cache and survive eviction.

    Args:
        half_life_s: Seconds for an idle object's score to halve. Defaults to ``600``.
        clock: Monotonic time source; injectable for tests.
    """

    def __init__(self, half_life_s: float = 600.0, clock: Callable[[], float] = time.monotonic):
        if half_life_s <= 0:
            raise ValueError("half_life_s must be > 0")
        self.half_life_s = half_life_s
        self.tau         = half_life_s / math.log(2)
        self.clock       = clock
        self._stats: Dict[str, BallStats] = {}

    @staticmethod
    def key(bucket_id: str, ball_id: str) -> str:
        """Build the stats/cache key for a ball."""
        return f"{bucket_id}/{ball_id}"

    def _touch(self, key: str, now: float) -> BallStats:
        s = self._stats.get(key)
        if s is None:
            s = BallStats(first_access=now, t_score=now)
            self._stats[key] = s
        s.last_access = now
        return s

    def _decayed(self, s: BallStats, now: float) -> float:
        return s.score * 2 ** (-(now - s.t_score) / self.half_life_s)

    def record_get(self, key: str, hit: bool, nbytes: int = 0):
        """Record a read of ``key`` (cache hit or download)."""
        now = self.clock()
        s   = self._touch(key, now)
        s.num_gets   += 1
        s.bytes_read += nbytes
        if hit:
            s.hits += 1
        else:
            s.misses += 1
        s.score   = self._decayed(s, now) + 1
        s.t_score = now

    def record_put(self, key: str, nbytes: int = 0):
        """Record a put (or replica) of ``key``."""
        s = self._touch(key, self.clock())
        s.num_puts      += 1
        s.bytes_written += nbytes

    def get(self, key: str) -> BallStats:
        """Return the stats for ``key`` (an empty ``BallStats`` if never seen)."""
        return self._stats.get(key) or BallStats()

    def score(self, key: str) -> float:
        """Decayed get score of ``key`` at the current time."""
        s = self._stats.get(key)
        return 0.0 if s is None else self._decayed(s, self.clock())

    def freq(self, key: str) -> float:
        """Approximate recent gets per second of ``key``."""
        return self.score(key) / self.tau

    def rank(self, key: str) -> float:
        """Time-invariant eviction rank: ``log2(score) + t_score / half_life``.

        Ordering by rank equals ordering by current decayed score at any
        instant, but the value only changes when the key is read, so it can
        live in a heap.  Keys never read rank ``-inf``.
        """
        s = self._stats.get(key)
        if s is None or s.score <= 0:
            return float("-inf")
        return math.log2(s.score) + s.t_score / self.half_life_s

    def keys(self) -> List[str]:
        """Keys with recorded stats."""
        return list(self._stats.keys())

    def reset(self):
        """Forget all stats."""
        self._stats.clear()
