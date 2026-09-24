#!/usr/bin/env python3
"""Deploy a local MictlanX cluster (router + summoner + rm + peers) from
Python, using VirtualStorageSpace, then tear it down.

Unlike the rest of `examples/`, this script needs no pre-existing cluster
(no `deploy_router.sh` required) -- it deploys and removes one itself via
the Docker Engine API. Requires a running Docker daemon and the `vss` extra
(`pip install mictlanx[vss]`); the first run pulls the router/summoner/
rm/peer images, which can take a while.
"""
import asyncio
import argparse

from mictlanx.vss import VirtualStorageSpace

VSS_ID = "examples-vss-01"


async def main():
    p = argparse.ArgumentParser(description="Deploy a 2-peer local VSS, then tear it down")
    p.add_argument("--peers", type=int, default=2, help="Initial peer pool size")
    p.add_argument("--vss_id", default=VSS_ID, help="Namespace prefix for the containers this deploys")
    args = p.parse_args()

    vs = VirtualStorageSpace(
        peers  = args.peers,
        vss_id = args.vss_id
    )

    print(f"Deploying VSS '{vs.vss_id}' (router={vs.router_id}, summoner={vs.summoner_id}, rm={vs.rm_id})...")
    res = await vs.up()

    if res.is_err:
        print("UP FAILED:", res.unwrap_err())
        return

    print(f"UP OK: {vs.size} peers running -> {vs.peer_ids()}")
    print(f"Router reachable at http://localhost:{vs.router_port}")

    # Keep the VSS running until the user asks to tear it down.
    while True:
        try:
            cmd = await asyncio.to_thread(input, "Type 'down' to tear down the VSS: ")
        except (EOFError, KeyboardInterrupt):
            break
        if cmd.strip().lower() == "down":
            break

    print("Tearing down...")
    down_res = await vs.down()
    if down_res.is_ok:
        print("DOWN OK")
    else:
        print("DOWN FAILED:", down_res.unwrap_err())


if __name__ == "__main__":
    asyncio.run(main())
