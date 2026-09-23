#!/usr/bin/env python3
"""Upload an object through the IO-filter pipeline: compress, then encrypt.

Pairs with `05_get_with_filters.py`, which downloads and reverses the same
chain. Run this script first, then run `05_get_with_filters.py`.
"""
import asyncio
import argparse

from mictlanx import AsyncClient
from mictlanx.filters import CompressFilter, EncryptFilter

URI       = "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0"
BUCKET_ID = "examples-filters-bucket"
BALL_ID   = "examples-filters-ball"

# Demo key only (AES-256-GCM, 32 bytes) — never hardcode encryption keys in
# real code. Load them from an env var or a secret manager instead. This
# same value must be used by 05_get_with_filters.py to decrypt what we put.
DEMO_KEY_HEX = "320cf6027dd11c40bd4a29cfb636e0db45ed144b75266b226d9359dfe62fcecc"


async def main():
    p = argparse.ArgumentParser(description="Upload an object through the IO-filter pipeline (compress, then encrypt)")
    p.add_argument("--bucket_id", default=BUCKET_ID, help="Bucket namespace")
    p.add_argument("--ball_id",   default=BALL_ID,   help="Logical object id (used as the key/group id)")
    args = p.parse_args()

    data = (b"MictlanX filter pipeline demo. " * 200)
    key  = bytes.fromhex(DEMO_KEY_HEX)

    client = AsyncClient(
        uri       = URI,
        client_id = "example-put-with-filters",
        debug     = True,
    )

    # Filters run in list order: compress first, then encrypt — compressing
    # ciphertext afterward would gain nothing, so this is the recommended
    # order. get() must supply the exact reverse chain of inverses.
    res = await client.put(
        bucket_id = args.bucket_id,
        ball_id   = args.ball_id,
        value     = data,
        # filters   = [CompressFilter(), EncryptFilter(key)],
    )

    if res.is_ok:
        print(f"PUT OK: {len(data)} bytes uploaded (compressed+encrypted before chunking)")
        print(f"Now run: python3 examples/client/05_get_with_filters.py --bucket_id {args.bucket_id} --ball_id {args.ball_id}")
    else:
        print("PUT FAILED:", res.unwrap_err())


if __name__ == "__main__":
    asyncio.run(main())
