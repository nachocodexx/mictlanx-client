import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple, Union
from xolo.utils.utils import Utils as XoloUtils
import mictlanx.errors as EX
from mictlanx.caching.stats import AccessStats, BallStats
from mictlanx.retry import RetryPolicy
from mictlanx.utils.index import Utils
from mictlanx.objects.ball import Ball
from mictlanx.objects.context import current_client, _unwrap, _is_not_found

if TYPE_CHECKING:
    from mictlanx.asyncx import AsyncClient
    from mictlanx.filters import IOFilter

BytesLike = Union[bytes, bytearray, memoryview]
# (ball_id, data) | (ball_id, data, tags) | {"ball_id": ..., "data": ..., "tags": ...}
PutItem = Union[Tuple[str, BytesLike], Tuple[str, BytesLike, Dict[str, str]], Dict[str, Any]]


@dataclass
class PutManyResult:
    """Outcome of :meth:`Bucket.put_many`: stored balls and per-item failures."""

    ok: List[Ball] = field(default_factory=list)
    failed: List[Tuple[str, Exception]] = field(default_factory=list)


class Bucket:
    """Async handle to a bucket, built only on :class:`~mictlanx.asyncx.AsyncClient`.

    Methods raise :class:`~mictlanx.errors.MictlanXError` subclasses instead of
    returning ``Result``.  Reads always go through the client's cache.

    Args:
        bucket_id: Bucket identifier.
        client: Client to use.  When omitted, the client bound by the
            surrounding ``async with AsyncClient(...)`` is used.
    """

    def __init__(self, bucket_id: str, client: Optional["AsyncClient"] = None):
        self.bucket_id = bucket_id
        self._client   = client
        # Metadata lookups must not back off on "not found" (a new ball is the common case).
        self._lookup_policy = RetryPolicy(retries=3, initial_delay=0.5, backoff_factor=2.0, max_delay=5.0, retry_on=lambda e: not _is_not_found(e))

    @property
    def client(self) -> "AsyncClient":
        return self._client if self._client is not None else current_client()

    def _key(self, ball_id: str) -> str:
        return AccessStats.key(Utils.sanitize_str(self.bucket_id), Utils.sanitize_str(ball_id))

    # --- Reads -----------------------------------------------------------------------

    async def get(self, ball_id: str, force: bool = False, filters: Optional[List["IOFilter"]] = None, **kwargs) -> bytes:
        """Return the ball's bytes, served from the client cache when still valid.

        Args:
            ball_id: Ball identifier.
            force: Skip the cache and download again.
            filters: Get-side filter chain (inverse of the put-side chain).
            **kwargs: Extra arguments forwarded to :meth:`AsyncClient.get`.
        """
        res = _unwrap(await self.client.get(bucket_id=self.bucket_id, ball_id=ball_id, force=force, filters=filters, cache=True, **kwargs))
        mv  = res.data
        if type(mv.obj) is bytes and mv.nbytes == len(mv.obj):
            return mv.obj  # the cached bytes object itself: no copy
        return mv.tobytes()

    async def get_metadata(self, ball_id: str) -> Ball:
        """Return the ball's full metadata as a :class:`Ball`.

        Raises:
            NotFoundError: if the ball does not exist.
        """
        try:
            meta = _unwrap(await self.client.get_metadata(bucket_id=self.bucket_id, ball_id=ball_id, restart_policy=self._lookup_policy))
        except EX.MictlanXError as e:
            if _is_not_found(e) and not isinstance(e, EX.NotFoundError):
                raise EX.NotFoundError(f"{self.bucket_id}/{ball_id} not found") from e
            raise
        if not meta.chunks:
            raise EX.NotFoundError(f"{self.bucket_id}/{ball_id} not found")
        return Ball.from_chunks(self, ball_id, meta.chunks)

    async def exists(self, ball_id: str) -> bool:
        """Return ``True`` if the ball exists."""
        return await self._find(ball_id) is not None

    async def _find(self, ball_id: str) -> Optional[Ball]:
        try:
            return await self.get_metadata(ball_id)
        except EX.NotFoundError:
            return None

    async def balls(self) -> AsyncIterator[Ball]:
        """Iterate over every ball in the bucket (metadata only, no data)."""
        bucket = _unwrap(await self.client.get_bucket_metadata(bucket_id=self.bucket_id))
        for b in bucket.balls.values():
            if b.chunks:
                yield Ball.from_chunks(self, b.ball_id, b.chunks)

    # --- Writes ----------------------------------------------------------------------

    async def put(
        self,
        ball_id: str,
        data: BytesLike,
        tags: Optional[Dict[str, str]] = None,
        filters: Optional[List["IOFilter"]] = None,
        rf: int = 1,
        cache: bool = False,
        chunk_size: str = "256kb",
    ) -> Ball:
        """Store ``data`` as ``ball_id``; putting identical data again creates a replica.

        Args:
            ball_id: Ball identifier.
            data: Bytes to store.
            tags: User tags.
            filters: Put-side filter chain (e.g. ``[CompressFilter()]``).
            rf: Replication factor for each chunk.
            cache: Also keep ``data`` in the client cache.
            chunk_size: Chunk size as a humanfriendly string.

        Raises:
            BallConflictError: the ball exists with different data.
        """
        payload       = data if type(data) is bytes else bytes(data)
        data_checksum = XoloUtils.sha256(payload)
        existing      = await self._find(ball_id)
        if existing is not None and existing.data_checksum != data_checksum:
            raise EX.BallConflictError(
                f"{self.bucket_id}/{ball_id} already exists with checksum {existing.data_checksum[:12]}…, got {data_checksum[:12]}…"
            )
        ok = _unwrap(await self.client.put(
            bucket_id  = self.bucket_id,
            ball_id    = ball_id,
            value      = payload,
            tags       = {**(tags or {}), "mictlanx_data_checksum": data_checksum},
            filters    = filters,
            rf         = rf,
            cache      = cache,
            chunk_size = chunk_size,
        ))
        if ok is False:
            raise EX.MaxAvailabilityReachedError()
        return await self.get_metadata(ball_id)

    async def put_many(self, items: Iterable[PutItem], max_concurrency: int = 10, **kwargs) -> PutManyResult:
        """Put several balls concurrently; failures are collected, not raised.

        Args:
            items: ``(ball_id, data)``, ``(ball_id, data, tags)`` or
                ``{"ball_id", "data", "tags"}`` entries.
            max_concurrency: Maximum balls uploaded at once.
            **kwargs: Extra arguments forwarded to :meth:`put` (``filters``, ``rf``, ...).
        """
        semaphore = asyncio.Semaphore(max_concurrency)
        result    = PutManyResult()

        async def _one(item: PutItem):
            if isinstance(item, dict):
                ball_id, data, tags = item["ball_id"], item["data"], item.get("tags")
            else:
                ball_id, data, tags = item[0], item[1], (item[2] if len(item) > 2 else None)
            async with semaphore:
                try:
                    result.ok.append(await self.put(ball_id, data, tags=tags, **kwargs))
                except Exception as e:
                    result.failed.append((ball_id, e))

        await asyncio.gather(*(_one(i) for i in items))
        return result

    async def replicate(
        self,
        ball_id: str,
        n: int = 1,
        filters: Optional[List["IOFilter"]] = None,
        get_filters: Optional[List["IOFilter"]] = None,
    ) -> Ball:
        """Store ``n`` more replicas of an existing ball (a put of the same data).

        The data is read through the cache (downloaded if needed).  For balls
        stored with filters pass both chains: ``get_filters`` to read the
        original bytes and ``filters`` to store them again.
        """
        ball = await self.get_metadata(ball_id)
        data = await self.get(ball_id, filters=get_filters)
        for _ in range(n):
            ball = await self.put(ball_id, data, tags=ball.tags, filters=filters)
        return ball

    async def delete_ball(self, ball_id: str) -> None:
        """Delete a ball (its cached copy is dropped too; local stats are kept)."""
        _unwrap(await self.client.delete(ball_id=ball_id, bucket_id=self.bucket_id))

    async def delete(self) -> None:
        """Delete the whole bucket."""
        _unwrap(await self.client.delete_bucket(bucket_id=self.bucket_id))

    # --- Local stats (no network) ------------------------------------------------------

    def stats(self, ball_id: str) -> BallStats:
        """This client's access stats for ``ball_id``."""
        return self.client.stats.get(self._key(ball_id))

    def freq(self, ball_id: str) -> float:
        """Approximate recent gets per second of ``ball_id``."""
        return self.client.stats.freq(self._key(ball_id))

    def __repr__(self) -> str:
        return f"Bucket({self.bucket_id!r})"
