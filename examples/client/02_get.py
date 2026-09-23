#!/usr/bin/env python3
import asyncio
import argparse
import os
from mictlanx import AsyncClient  # adjust import if your path differs

async def main():
    p = argparse.ArgumentParser(description="Download using MictlanX AsyncClient")
    p.add_argument("--bucket_id", required=True, help="Bucket namespace")
    p.add_argument("--ball_id",   required=True, help="Logical object id used at PUT time")
    p.add_argument("--chunk_size", default="1MB", help="Chunk size hint for GET")
    p.add_argument("--to_file", action="store_true",
                   help="If set, stream directly to disk (get_to_file)")
    p.add_argument("--outdir", default="./downloads",
                   help="Directory to write the file when --to_file is used")
    p.add_argument("--client_id", default="client-example-get", help="Client id for logging")

    args = p.parse_args()

    uri = "mictlanx://mictlanx-router-0@localhost:60666?/api_version=4&protocol=http"
    client = AsyncClient(uri=uri, client_id=args.client_id, debug=True)

    if args.to_file:
        # Stream to disk
        os.makedirs(args.outdir,exist_ok=True)
        res = await client.get_to_file(
            bucket_id  = args.bucket_id,
            ball_id    = args.ball_id,
            output_path= args.outdir,
            chunk_size = args.chunk_size,
        )
        if res.is_ok:
            print("Saved to:", res.unwrap())
        else:
            print("GET_TO_FILE FAILED:", res.unwrap_err())
    else:
        # Read to memory
        res = await client.get(
            bucket_id  = args.bucket_id,
            ball_id    = args.ball_id,
            chunk_size = args.chunk_size,
        )
        if res.is_ok:
            response = res.unwrap()
            data = response.data.tobytes()
            print("GET OK, bytes:", len(data))
            # do something with `data`
        else:
            print("GET FAILED:", res.unwrap_err())

if __name__ == "__main__":
    asyncio.run(main())
