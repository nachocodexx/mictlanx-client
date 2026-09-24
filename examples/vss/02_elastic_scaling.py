#!/usr/bin/env python3
"""Scale a local VSS's peer pool up and down at runtime.

Deploys with a single peer, then grows the pool with `expand()`, grows it
again with `elastic()` (an alias -- `vs.elastic(n=1) == vs.expand(n=1)`),
prints per-peer disk stats after each change, then shrinks it back down
with `retract()` before tearing everything down.

Requires a running Docker daemon and the `vss` extra
(`pip install mictlanx[vss]`); run `01_deploy.py` first if you just want to
confirm Docker/images are working, since this script does more steps.
"""
import asyncio
import argparse

from mictlanx.vss import VirtualStorageSpace

VSS_ID = "examples-vss-01"


def print_stats(label, stats_result):
    if stats_result.is_err:
        print(f"  [{label}] stats FAILED: {stats_result.unwrap_err()}")
        return
    for peer_id, s in stats_result.unwrap().items():
        print(f"  [{label}] {peer_id}: used_disk={s.used_disk} total_disk={s.total_disk}")

async def wait_for_user(x:str="VSS"):
    while True:
        try:
            cmd = await asyncio.to_thread(input, f"Type 'down' to tear down the {x}: ")
        except (EOFError, KeyboardInterrupt):
            break
        if cmd.strip().lower() == "down":
            break

async def main():
    p = argparse.ArgumentParser(description="Grow and shrink a local VSS's peer pool at runtime")
    p.add_argument("--vss_id", default=VSS_ID, help="Namespace prefix for the containers this deploys")
    args = p.parse_args()

    vs = VirtualStorageSpace(peers=1, vss_id=args.vss_id)

    print(f"Deploying VSS '{vs.vss_id}' with 1 peer...")
    up_res = await vs.up()
    if up_res.is_err:
        print("UP FAILED:", up_res.unwrap_err())
        return
    print(f"UP OK: {vs.peer_ids()}")
    print_stats("after up", await vs.stats())

    print("Expanding by 3 peers...")
    expand_res = await vs.expand(n=3)
    if expand_res.is_err:
        print("EXPAND FAILED:", expand_res.unwrap_err())
        await vs.down()
        return
    print(f"EXPAND OK: new peers {expand_res.unwrap()} (size={vs.size})")
    print_stats("after expand", await vs.stats())

    print("Growing by 1 more via elastic() (same call as expand()):")
    elastic_res = await vs.elastic(n=1)
    print(f"ELASTIC OK: new peer {elastic_res.unwrap()} (size={vs.size})")


    await wait_for_user("VSS after elastic()")
    print("Retracting 2 peers (LIFO -- most recently added go first)...")
    retract_res = await vs.retract(n=2)
    if retract_res.is_err:
        print("RETRACT FAILED:", retract_res.unwrap_err())
    else:
        print(f"RETRACT OK: removed {retract_res.unwrap()} (size={vs.size})")
    print_stats("after retract", await vs.stats())

        # Keep the VSS running until the user asks to tear it down.
    await wait_for_user()

    print("Tearing down...")
    down_res = await vs.down()
    if down_res.is_ok:
        print("DOWN OK")
    else:
        print("DOWN FAILED:", down_res.unwrap_err())

    # print("Tearing down...")
    # await vs.down()
    # print("DOWN OK")


if __name__ == "__main__":
    asyncio.run(main())
