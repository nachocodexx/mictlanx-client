#!/usr/bin/env python3
"""Batch upload with put_many: successes and failures are returned, not raised."""
import asyncio
import os

from mictlanx import AsyncClient

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")


async def main():
    async with AsyncClient(uri=URI, client_id="example-new-api-put-many", debug=False) as mx:
        bk = mx.bucket("new-api-put-many")
        await bk.put("item-0", b"original")       # item-0 below will conflict

        items = [("item-0", b"changed")]                                    # (ball_id, data)
        items += [(f"item-{i}", os.urandom(4096), {"i": str(i)}) for i in range(1, 6)]  # (ball_id, data, tags)
        items += [{"ball_id": "item-6", "data": b"dict form", "tags": {"form": "dict"}}]

        result = await bk.put_many(items, max_concurrency=4)
        print("stored :", sorted(b.ball_id for b in result.ok))
        for ball_id, err in result.failed:
            print("failed :", ball_id, "->", type(err).__name__, err)


if __name__ == "__main__":
    asyncio.run(main())
