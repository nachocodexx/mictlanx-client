#!/usr/bin/env python3
"""Replication (a put of the same data) and immutability (BallConflictError)."""
import asyncio
import os

from mictlanx import AsyncClient
from mictlanx.objects import BallConflictError

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")


async def main():
    async with AsyncClient(uri=URI, client_id="example-new-api-replicate", debug=False) as mx:
        bk   = mx.bucket("new-api-replicate")
        ball = await bk.put("doc", b"version 1", tags={"owner": "examples"})

        await ball.replicate(n=2)                 # two more puts of the same bytes
        print("num_puts after replicate(2):", ball.num_puts)

        await bk.put("doc", b"version 1")         # same data again: just another replica
        print("num_puts after identical put:", ball.num_puts)

        try:
            await bk.put("doc", b"version 2")     # different data: rejected
        except BallConflictError as e:
            print("conflict:", e.message)


if __name__ == "__main__":
    asyncio.run(main())
