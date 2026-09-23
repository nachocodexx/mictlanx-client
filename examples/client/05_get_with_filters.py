#!/usr/bin/env python3
"""Download an object through the IO-filter pipeline: decrypt, then decompress.

Pairs with `04_put_with_filters.py`, which uploaded the object with
`filters=[CompressFilter(), EncryptFilter(key)]`. Run that script first.

Reversing a filter chain: put(filters=[f1, f2]) applies f1 then f2, so
get() must supply the inverses in the opposite order: [f2_inverse, f1_inverse].
Here that means [DecryptFilter(key), DecompressFilter()].

If you pass the wrong filters (wrong order, missing, or extra), the SDK
raises mictlanx.errors.FilterMismatchError before touching the data — it
checks a provenance tag recorded at put() time rather than silently
returning garbage. A wrong key (or tampered ciphertext) instead raises
mictlanx.errors.FilterExecutionError from the AES-GCM auth-tag check. See
mictlanx/filters/__init__.py for details.
"""
import asyncio
import argparse

from mictlanx import AsyncClient
from mictlanx.filters import DecompressFilter, DecryptFilter

URI       = "mictlanx://mictlanx-router-0@localhost:63666/?protocol=http&api_version=4&http2=0"
BUCKET_ID = "examples-filters-bucket"
BALL_ID   = "examples-filters-ball"

# Must match the key used in 04_put_with_filters.py.
DEMO_KEY_HEX = "320cf6027dd11c40bd4a29cfb636e0db45ed144b75266b226d9359dfe62fcecc"


async def main():
    p = argparse.ArgumentParser(description="Download an object through the IO-filter pipeline (decrypt, then decompress)")
    p.add_argument("--bucket_id", default=BUCKET_ID, help="Bucket namespace")
    p.add_argument("--ball_id",   default=BALL_ID,   help="Logical object id (used as the key/group id)")
    args = p.parse_args()

    key = bytes.fromhex(DEMO_KEY_HEX)

    client = AsyncClient(
        uri       = URI,
        client_id = "example-get-with-filters",
        debug     = True,
    )

    res = await client.get(
        bucket_id = args.bucket_id,
        ball_id   = args.ball_id,
        filters   = [DecryptFilter(key), DecompressFilter()],
    )

    if res.is_ok:
        recovered = bytes(res.unwrap().data)
        print(f"GET OK: recovered {len(recovered)} bytes")
        print(recovered[:80].decode("utf-8", errors="replace") + "...")
    else:
        print("GET FAILED:", res.unwrap_err())


if __name__ == "__main__":
    asyncio.run(main())
