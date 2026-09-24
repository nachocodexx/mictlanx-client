#!/usr/bin/env python3
"""Filters with the Bucket API: the cache keeps the already-decoded bytes.

put() takes the put-side chain; get() takes the inverse chain in reverse order.
"""
import asyncio
import os

from mictlanx import AsyncClient
from mictlanx.filters import CompressFilter, DecompressFilter, EncryptFilter, DecryptFilter

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")

# Demo key only — load real keys from an env var or a secret manager.
KEY = bytes.fromhex("320cf6027dd11c40bd4a29cfb636e0db45ed144b75266b226d9359dfe62fcecc")


async def main():
    put_filters = [CompressFilter(), EncryptFilter(KEY)]
    get_filters = [DecryptFilter(KEY), DecompressFilter()]
    data        = b"MictlanX filter pipeline demo. " * 2000

    async with AsyncClient(uri=URI, client_id="example-new-api-filters", debug=False) as mx:
        bk   = mx.bucket("new-api-filters")
        ball = await bk.put("secret", data, filters=put_filters)
        print(f"stored {ball.size} bytes for {len(data)} original bytes, filters={ball.filters}")

        assert await bk.get("secret", filters=get_filters) == data   # download + decrypt + decompress
        assert await bk.get("secret", filters=get_filters) == data   # cache hit: no filters run again
        s = bk.stats("secret")
        print(f"gets={s.num_gets} hits={s.hits} misses={s.misses}")

        # Replicating a filtered ball needs both chains.
        await ball.replicate(1, filters=put_filters, get_filters=get_filters)
        print("num_puts:", ball.num_puts)


if __name__ == "__main__":
    asyncio.run(main())
