# Caching

## When the cache is used

- **Opt-in for `AsyncClient.get()`.** `get()` only uses the cache when you pass `cache=True`, or when the client was created with `cache_default=True` (env `MICTLANX_CLIENT_CACHE_DEFAULT=1`). The default is off, so benchmarks measure real downloads.
- **Always on for [`Bucket.get()`](objects.md).** The `mictlanx.objects` handles always read through the cache.
- **Checked against fresh metadata.** A cached copy is served only if its `full_checksum` matches the metadata fetched on that call, so a cache hit still costs one metadata request but no download.
- **Stores post-filter bytes.** The cache holds the bytes *after* the get-side filters. A copy is served only for a `get()` with the same filter chain.
- **`force=True`** always downloads.
- **Budget.** The default policy is **LFU with decay**, with a 1GB budget (`eviction_policy`, `capacity_storage`). LFU eviction uses the decayed frequencies from [Access Stats](stats.md). Memory is only used by objects that are actually cached.

```python
res = await client.get(bucket_id="bk1", ball_id="b1", cache=True)   # download + store
res = await client.get(bucket_id="bk1", ball_id="b1", cache=True)   # metadata check + cache hit
res = await client.get(bucket_id="bk1", ball_id="b1", force=True)   # always downloads
```

## CacheFactory

::: mictlanx.caching.CacheFactory

## LRUCache

::: mictlanx.caching.LRUCache

## LFUCache

::: mictlanx.caching.LFUCache

## NoCache

::: mictlanx.caching.NoCache
