#!/usr/bin/env python3
import asyncio
import argparse
import os
from mictlanx import AsyncClient  # adjust import if your path differs

def parse_tags(items):
    """
    --tag k=v --tag x=y  -> {"k": "v", "x": "y"}
    """
    out = {}
    for it in items or []:
        if "=" in it:
            k, v = it.split("=", 1)
            out[k.strip()] = v.strip()
    return out

async def main():
    p = argparse.ArgumentParser(description="Upload a file using MictlanX AsyncClient")
    # p.add_argument("--uri", required=True,
                #    help="mictlanx://router_id@host:port[,router_id@host:port]/?protocol=...&api_version=...")
    p.add_argument("--bucket_id", required=True, help="Bucket namespace, e.g. mictlanx")
    p.add_argument("--ball_id",   required=True, help="Logical object id (used as the key/group id)")
    p.add_argument("--path",      required=True, help="Local path to the file to upload")
    p.add_argument("--chunk_size", default="1MB", help="Chunk size (e.g. 256kb, 1MB)")
    p.add_argument("--rf", type=int, default=1, help="Replication factor hint")
    p.add_argument("--tag", action="append", default=[], help="Extra metadata tags k=v (repeatable)")
    p.add_argument("--client_id", default="client-example-put", help="Client id for logging")

    args = p.parse_args()

    # Build tags
    tags = parse_tags(args.tag)
    base = os.path.basename(args.path)
    name, ext = os.path.splitext(base)
    tags.setdefault("fullname", base)
    if ext.startswith("."):
        tags.setdefault("extension", ext[1:])

    
    
    # from mictlanx import AsyncClient

    # Port 60666 -> 63666.

    uri = "mictlanx://mictlanx-router-0@localhost:60666?/api_version=4&protocol=http"

    client = AsyncClient(
        uri             = uri,
        client_id       = args.client_id,
        debug           = True,
        log_output_path = "/log",
    )

    
    # with open("/source/01.pdf","rb") as f:
    #     data = f.read()

    # client.put(
    #     bucket_id = "cubeton",
    #     ball_id   = "mipelota",
    #     value     = data
    # )


    # client.put_hdf5()

    res = await client.put_file(
        bucket_id   = args.bucket_id,
        ball_id     = args.ball_id,
        path        = args.path,
        chunk_size  = args.chunk_size,
        rf          = args.rf,
        tags        = tags,
    )

    if res.is_ok:
        print("PUT OK")
    else:
        print("PUT FAILED:", res.unwrap_err())

if __name__ == "__main__":
    asyncio.run(main())
