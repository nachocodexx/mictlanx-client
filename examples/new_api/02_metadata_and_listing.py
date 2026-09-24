#!/usr/bin/env python3
"""Inspect a ball's metadata and list every ball in a bucket."""
import asyncio
import os

from mictlanx import AsyncClient

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")


async def main():
    async with AsyncClient(uri=URI, client_id="example-new-api-metadata", debug=False) as mx:
        bk = mx.bucket("new-api-listing")
        for i in range(3):
            await bk.put(f"frame-{i}", os.urandom(64 * 1024), tags={"video": "demo", "frame": str(i)})

        ball = await bk.get_metadata("frame-0")
        print("ball_id      :", ball.ball_id)
        print("size         :", ball.size)
        print("checksum     :", ball.checksum)
        print("num_chunks   :", ball.num_chunks)
        print("content_type :", ball.content_type)
        print("updated_at   :", ball.updated_at)
        print("tags (user)  :", ball.tags)

        print("\nAll balls in", bk.bucket_id)
        async for b in bk.balls():
            print(f"  {b.ball_id:<10} {b.size:>8} bytes  tags={b.tags}")


if __name__ == "__main__":
    asyncio.run(main())
