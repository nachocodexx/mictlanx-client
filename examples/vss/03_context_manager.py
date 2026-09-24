#!/usr/bin/env python3
"""Use VirtualStorageSpace as an async context manager, then talk to the
deployed cluster with the normal high-level AsyncClient.

`async with VirtualStorageSpace(...)` calls up() on enter and down() on
exit -- including when the block raises -- so the cluster this script
deploys is always cleaned up.

Requires a running Docker daemon and the `vss` extra
(`pip install mictlanx[vss]`).
"""
import asyncio
import argparse

from mictlanx import AsyncClient
from mictlanx.vss import VirtualStorageSpace

VSS_ID = "examples-vss-03"
BUCKET_ID = "examples-vss-bucket"
BALL_ID = "examples-vss-ball"


async def main():
    p = argparse.ArgumentParser(description="Deploy a VSS via `async with`, then put/get through it")
    p.add_argument("--vss_id", default=VSS_ID, help="Namespace prefix for the containers this deploys")
    args = p.parse_args()

    data = b"Hello from VirtualStorageSpace's async context manager! " * 100

    async with VirtualStorageSpace(peers=2, vss_id=args.vss_id) as vs:
        print(f"VSS '{vs.vss_id}' deployed with {vs.size} peers: {vs.peer_ids()}")

        client = AsyncClient(
            uri=vs.uri,
            client_id="example-vss-context-manager",
            debug=True,
        )

        put_res = await client.put(bucket_id=BUCKET_ID, ball_id=BALL_ID, value=data)
        if put_res.is_ok:
            print(f"PUT OK: {len(data)} bytes uploaded")
        else:
            print("PUT FAILED:", put_res.unwrap_err())
            return

        get_res = await client.get(bucket_id=BUCKET_ID, ball_id=BALL_ID)
        if get_res.is_ok:
            recovered = bytes(get_res.unwrap().data)
            print(f"GET OK: recovered {len(recovered)} bytes, matches={recovered == data}")
        else:
            print("GET FAILED:", get_res.unwrap_err())

    # vs.down() has already run here, even if an exception was raised above.
    print("VSS torn down.")


if __name__ == "__main__":
    asyncio.run(main())
