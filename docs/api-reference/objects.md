# Bucket & Ball

`mictlanx.objects` is a small, high-level layer on top of [`AsyncClient`](async-client.md). It works with **buckets** and **balls** instead of `Result` values:

- Methods **raise** [`MictlanXError`](errors.md) subclasses instead of returning `Result`.
- A `Ball` holds the ball's **full metadata** plus this client's **live access stats** ([Access Stats](stats.md)). It never holds the data; use `await bucket.get(ball_id)`.
- Reads (`Bucket.get`) always go through the client's [cache](caching.md).
- **Balls are immutable.** Putting the same data again creates a replica. Putting different data under an existing `ball_id` raises `BallConflictError`.

## Getting a bucket

There are two ways to get a `Bucket` handle:

```python
from mictlanx import AsyncClient
from mictlanx.objects import Bucket

async with AsyncClient(uri=URI) as mx:
    bk   = mx.bucket("bk1")     # explicit: bound to this client
    same = Bucket("bk1")        # implicit: resolves the client of the surrounding `async with`
```

`async with AsyncClient(...)` binds the client to a context variable, so `Bucket("bk1")` finds it without passing `client=`. Outside that block, `Bucket("bk1")` without `client=` raises `NoActiveClientError`.

## Quickstart

```python
from mictlanx import AsyncClient
from mictlanx.objects import BallConflictError

async with AsyncClient(uri=URI) as mx:
    bk   = mx.bucket("bk1")
    ball = await bk.put("b1", b"hello", tags={"owner": "me"})   # -> Ball (metadata, no data)
    data = await bk.get("b1")                                    # -> bytes (cached)
    meta = await bk.get_metadata("b1")                           # -> Ball

    print(ball.size, ball.checksum, ball.tags)
    print(ball.num_gets, ball.hits, ball.misses, ball.freq)      # local, live stats

    await ball.replicate(2)                  # replication = put the same data again

    try:
        await bk.put("b1", b"other data")    # balls are immutable
    except BallConflictError:
        ...

    async for b in bk.balls():               # list the bucket (metadata only)
        print(b.ball_id, b.size, b.tags)

    result = await bk.put_many([("a", b"1"), ("b", b"2", {"k": "v"})])
    print(len(result.ok), "stored,", len(result.failed), "failed")
```

Runnable versions are in `examples/new_api/`:

| Script | Shows |
|---|---|
| `01_quickstart.py` | Put/get with explicit and implicit buckets |
| `02_metadata_and_listing.py` | `get_metadata`, `exists`, `balls()` |
| `03_access_stats.py` | `num_gets`, `hits`, `misses`, `freq` |
| `04_cache_behaviour.py` | Cache hits vs. downloads, `force=True` |
| `05_replicate_and_conflicts.py` | `replicate()` and `BallConflictError` |
| `06_put_many.py` | Concurrent puts with `put_many` |
| `07_filters.py` | Put-side / get-side filter chains |

## Bucket

::: mictlanx.objects.Bucket

## Ball

::: mictlanx.objects.Ball

## PutManyResult

::: mictlanx.objects.PutManyResult

## current_client

::: mictlanx.objects.current_client
