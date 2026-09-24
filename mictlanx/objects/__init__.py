"""High-level, exception-raising Bucket/Ball handles built on :class:`~mictlanx.asyncx.AsyncClient`.

Example::

    async with AsyncClient(uri=URI) as mx:
        bk = mx.bucket("bk1")            # or Bucket("bk1") inside the context
        b  = await bk.put("b1", b"hello")
        data = await bk.get("b1")        # bytes, cached
        print(b.num_gets, b.freq)
"""
from mictlanx.caching.stats import AccessStats, BallStats
from mictlanx.errors import BallConflictError, NoActiveClientError
from mictlanx.objects.ball import Ball
from mictlanx.objects.bucket import Bucket, PutManyResult
from mictlanx.objects.context import current_client

__all__ = [
    "AccessStats",
    "Ball",
    "BallConflictError",
    "BallStats",
    "Bucket",
    "NoActiveClientError",
    "PutManyResult",
    "current_client",
]
