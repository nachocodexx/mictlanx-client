# Access Stats

Every `AsyncClient` keeps local, per-ball access statistics in `client.stats`, an `AccessStats` instance. `Ball` handles from [`mictlanx.objects`](objects.md) expose them as properties (`num_gets`, `hits`, `misses`, `num_puts`, `last_access`, `freq`).

- **Local only.** Stats count what *this client* did (its own gets and puts). They are never sent to or read from the storage system.
- **Always on.** They are recorded whether or not the cache is used, and are kept after a ball is evicted from the cache or deleted.
- **Keyed by ball**, as `"{bucket_id}/{ball_id}"` (the same key the cache uses).

## Frequency and half-life

Besides lifetime counters, each ball has an exponentially decayed **score**. Every get adds 1:

```
score = score * 2 ** (-dt / half_life) + 1
```

`freq` is roughly the **recent gets per second**. If nobody reads the ball, its score halves every `half_life`. `num_gets`, by contrast, is a lifetime count.

The half-life defaults to `10m`. Set it with `AsyncClient(half_life=...)` or `MICTLANX_CLIENT_HALF_LIFE` (see [Environment Variables](../environment-variables.md)).

## Relation to LFU eviction

The default cache policy is **LFU with decay**. It evicts the ball with the lowest `AccessStats.rank()`. That rank orders balls the same way as their current decayed score, but it only changes when a ball is read, so a ball that was popular long ago eventually gets evicted. See [Caching](caching.md).

## Example

```python
async with AsyncClient(uri=URI) as mx:
    bk = mx.bucket("bk1")
    await bk.put("b1", b"hello")
    for _ in range(3):
        await bk.get("b1")

    ball = await bk.get_metadata("b1")
    print(ball.num_gets, ball.hits, ball.misses)   # 3, 2, 1
    print(f"{ball.freq:.4f} gets/s")

    # Raw access through the client
    s = mx.stats.get(mx.stats.key("bk1", "b1"))
    print(s.bytes_read, s.bytes_written)
```

See `examples/new_api/03_access_stats.py`.

## AccessStats

::: mictlanx.caching.stats.AccessStats

## BallStats

::: mictlanx.caching.stats.BallStats
