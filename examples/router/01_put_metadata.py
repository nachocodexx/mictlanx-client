from mictlanx.services import AsyncRouter
import hashlib
import asyncio
import argparse
import mictlanx.errors as EX

async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to upload metadata with bucket, key, and ball_id"
    )

    # Define arguments
    parser.add_argument("--bucket_id", required=True, help="ID of the bucket")
    parser.add_argument("--key", required=True, help="Key or path of the object")
    parser.add_argument("--ball_id", required=True, help="ID of the ball")
    parser.add_argument("--rf", required=True, help="Number of replicas")
    # parser.add_argument("--rf", type=int, default=1, help="Replication factor (default=1)")
    
    args = parser.parse_args()

    router = AsyncRouter(
        router_id   = "mictlanx-router-0",
        ip_addr     = "localhost",
        port        = 63666,
        protocol    = "http",
        api_version = 4,
    )
    bucket_id = args.bucket_id          # logical namespace
    key       = args.key       # your logical object name
    ball_id   = args.ball_id
    rf        = args.rf

    body      = b"Hello from AsyncRouter"
    checksum  = hashlib.sha256(body).hexdigest()  # integrity guard

    meta_res = await router.put_metadata(
        bucket_id    = bucket_id,
        ball_id      = ball_id,
        key          = key,
        size         = len(body),
        checksum     = checksum,
        producer_id  = "client-0",
        content_type = "text/plain",
        tags         = {"fullname": "hello.txt", "extension": "txt"},
        replication_factor= rf
    )
    if meta_res.is_err:
        e = meta_res.unwrap_err()
        print(e)
        if e.error_code == EX.MaxAvailabilityReachedError.error_code:
            print("Maximum replication factor reached.")
            return 0
        print("PUT_METADATA failed:", e)
        return -1
    response = meta_res.unwrap()
    tasks_ids = response.tasks_ids
    print("task_id:", tasks_ids)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())