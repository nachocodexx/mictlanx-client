from typing import TYPE_CHECKING, Dict, List, Optional
from mictlanx.caching.stats import BallStats
from mictlanx.interfaces.responses import Metadata

if TYPE_CHECKING:
    from mictlanx.objects.bucket import Bucket
    from mictlanx.filters import IOFilter

# Tags written by the SDK itself; hidden from ``Ball.tags`` and exposed as fields instead.
INTERNAL_TAGS = frozenset({
    "index", "num_chunks", "full_checksum", "mictlanx_filters", "chunk_size",
    "group_id", "updated_at", "mictlanx_get_filters", "mictlanx_data_checksum",
})


class Ball:
    """Handle to a stored ball: its full metadata plus this client's live access stats.

    A ``Ball`` never holds the object's bytes; use ``await bucket.get(ball_id)``
    (served from the client cache when possible).  Balls are immutable: putting
    the same data again creates a replica, different data raises
    :class:`~mictlanx.errors.BallConflictError`.
    """

    def __init__(
        self,
        bucket: "Bucket",
        ball_id: str,
        size: int,
        checksum: str,
        data_checksum: str,
        num_chunks: int,
        content_type: str,
        producer_id: str,
        filters: List[str],
        updated_at: Optional[int],
        tags: Dict[str, str],
        chunks: List[Metadata],
    ):
        self.bucket        = bucket
        self.bucket_id     = bucket.bucket_id
        self.ball_id       = ball_id
        self.size          = size            # stored bytes (after put-side filters)
        self.checksum      = checksum        # SHA-256 of the stored bytes
        self.data_checksum = data_checksum   # SHA-256 of the original (unfiltered) bytes
        self.num_chunks    = num_chunks
        self.content_type  = content_type
        self.producer_id   = producer_id
        self.filters       = filters         # put-side filter names, in put order
        self.updated_at    = updated_at      # wall-clock ms of the put, if recorded
        self.tags          = tags            # user tags only
        self.chunks        = chunks

    @classmethod
    def from_chunks(cls, bucket: "Bucket", ball_id: str, chunks: List[Metadata]) -> "Ball":
        """Build a ``Ball`` from the chunk metadata records of one ball."""
        unique: Dict[str, Metadata] = {}
        for c in chunks:
            unique.setdefault(c.key, c)
        ordered = sorted(unique.values(), key=lambda c: int(c.tags.get("index", 0)) if str(c.tags.get("index", "0")).isdigit() else 0)
        first   = ordered[0]
        tags    = first.tags
        checksum   = tags.get("full_checksum", first.checksum)
        updated_at = tags.get("updated_at", "")
        return cls(
            bucket        = bucket,
            ball_id       = ball_id,
            size          = sum(c.size for c in ordered),
            checksum      = checksum,
            data_checksum = tags.get("mictlanx_data_checksum", checksum),
            num_chunks    = int(tags.get("num_chunks", len(ordered))),
            content_type  = first.content_type,
            producer_id   = first.producer_id,
            filters       = [f for f in tags.get("mictlanx_filters", "").split(",") if f],
            updated_at    = int(updated_at) if updated_at.isdigit() else None,
            tags          = {k: v for k, v in tags.items() if k not in INTERNAL_TAGS},
            chunks        = ordered,
        )

    # --- Live local access stats -------------------------------------------------

    @property
    def stats(self) -> BallStats:
        """This client's access stats for the ball (never touches the network)."""
        return self.bucket.stats(self.ball_id)

    @property
    def num_gets(self) -> int:
        return self.stats.num_gets

    @property
    def num_puts(self) -> int:
        return self.stats.num_puts

    @property
    def hits(self) -> int:
        return self.stats.hits

    @property
    def misses(self) -> int:
        return self.stats.misses

    @property
    def last_access(self) -> Optional[float]:
        return self.stats.last_access

    @property
    def freq(self) -> float:
        """Approximate recent gets per second (decays with the client's half-life)."""
        return self.bucket.freq(self.ball_id)

    # --- Shortcuts to the bucket ---------------------------------------------------

    async def replicate(self, n: int = 1, filters: Optional[List["IOFilter"]] = None, get_filters: Optional[List["IOFilter"]] = None) -> "Ball":
        """Put the same data ``n`` more times. See :meth:`Bucket.replicate`."""
        return await self.bucket.replicate(self.ball_id, n=n, filters=filters, get_filters=get_filters)

    async def delete(self) -> None:
        """Delete the ball from its bucket."""
        await self.bucket.delete_ball(self.ball_id)

    async def refresh(self) -> "Ball":
        """Reload the metadata from the storage system (in place)."""
        fresh = await self.bucket.get_metadata(self.ball_id)
        self.__dict__.update(fresh.__dict__)
        return self

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Ball):
            return NotImplemented
        return (self.bucket_id, self.ball_id, self.checksum) == (other.bucket_id, other.ball_id, other.checksum)

    def __hash__(self) -> int:
        return hash((self.bucket_id, self.ball_id, self.checksum))

    def __repr__(self) -> str:
        return f"Ball(bucket_id={self.bucket_id!r}, ball_id={self.ball_id!r}, size={self.size}, checksum={self.checksum[:12]!r}, num_gets={self.num_gets})"
