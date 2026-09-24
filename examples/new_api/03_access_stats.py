#!/usr/bin/env python3
"""Local access stats: num_gets, hits/misses and the decayed frequency.

A short ``half_life`` makes the decay visible in a few seconds.
"""
import asyncio
import os

from mictlanx import AsyncClient

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")


def show(label, ball):
    print(f"{label:<22} num_gets={ball.num_gets:<3} hits={ball.hits:<3} misses={ball.misses:<3} freq={ball.freq:.3f} gets/s")


async def main():
    async with AsyncClient(uri=URI, client_id="example-new-api-stats", debug=False, half_life="5s") as mx:
        bk   = mx.bucket("new-api-stats")
        ball = await bk.put("popular", b"x" * 1024)

        for _ in range(10):
            await bk.get("popular")
        show("after 10 gets", ball)

        await asyncio.sleep(5)
        show("5s later (1 half-life)", ball)
        await asyncio.sleep(5)
        show("10s later", ball)
        print("num_puts:", ball.num_puts, "| lifetime num_gets never decays; freq does.")


if __name__ == "__main__":
    asyncio.run(main())
