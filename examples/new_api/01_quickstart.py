#!/usr/bin/env python3
"""Bucket/Ball API quickstart: put and get with explicit and implicit buckets.

Run against a local VSS (``bash deploy_router.sh``). Override the URI with
the ``MICTLANX_URI`` env var.
"""
import asyncio
import os

from mictlanx import AsyncClient
from mictlanx.objects import Bucket

URI = os.environ.get("MICTLANX_URI", "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0")


async def main():
    async with AsyncClient(uri=URI, client_id="example-new-api-quickstart", debug=False) as mx:
        # Explicit: a bucket bound to this client.
        bk   = mx.bucket("new-api-quickstart")
        ball = await bk.put("greeting", b"Hello, MictlanX!", tags={"lang": "en"})
        print("PUT ", ball)

        data = await bk.get("greeting")          # bytes
        print("GET ", data)

        # Implicit: inside `async with`, Bucket() finds the active client.
        same = Bucket("new-api-quickstart")
        print("GET (implicit bucket)", await same.get("greeting"))


if __name__ == "__main__":
    asyncio.run(main())
