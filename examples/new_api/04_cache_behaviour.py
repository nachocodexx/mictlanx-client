#!/usr/bin/env python3
"""Cache behaviour: miss vs hit, force=True, cache-on-put, and the plain client.

AsyncClient does NOT cache by default (cache_default=False); Bucket handles do.
"""
import asyncio
import os
import time

from mictlanx import AsyncClient

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")


async def timed(label, coro):
    t = time.monotonic()
    result = await coro
    print(f"{label:<28} {(time.monotonic() - t) * 1000:8.1f} ms")
    return result


async def main():
    async with AsyncClient(uri=URI, client_id="example-new-api-cache", debug=False, capacity_storage="256MB") as mx:
        bk      = mx.bucket("new-api-cache")
        payload = os.urandom(8 * 1024 * 1024)
        await bk.put("big", payload)

        await timed("bucket get (miss)", bk.get("big"))
        await timed("bucket get (hit)", bk.get("big"))
        await timed("bucket get (force=True)", bk.get("big", force=True))

        # Cache on put: the very first read is already a hit.
        await bk.put("warm", payload[:1024 * 1024], cache=True)
        await timed("get after put(cache=True)", bk.get("warm"))

        # Plain AsyncClient.get: no cache unless asked.
        await timed("client.get (no cache)", mx.get("new-api-cache", "big"))
        await timed("client.get (cache=True)", mx.get("new-api-cache", "big", cache=True))

        stats = bk.stats("big")
        print(f"\nbig: gets={stats.num_gets} hits={stats.hits} misses={stats.misses}")
        print(f"cache: {len(mx.cache)} entries, uf={mx.cache.get_uf():.3f}")


if __name__ == "__main__":
    asyncio.run(main())
