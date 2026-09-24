from mictlanx.services import AsyncRouter
import asyncio
import argparse

async def example_run():
    parser = argparse.ArgumentParser(
        description="Example script to upload data with task_id, and key"
    )

    # Define arguments
    parser.add_argument("--task_id", required=True, help="Task ID from put_metadata (step 1)")
    parser.add_argument("--ball_id", required=True, help="Key the ball")
    
    args = parser.parse_args()

    router = AsyncRouter(
        router_id   = "mictlanx-router-0",
        ip_addr     = "localhost",
        port        = 63666,
        protocol    = "http",
        api_version = 4,
    )
    task_id = args.task_id          # logical namespace
    ball_id     = args.ball_id       # your logical object name
    body    = b"Hello from AsyncRouter"
    put_data_result = await router.put_data(
        task_id      = task_id,
        ball_id          = ball_id,
        value        = body,
        content_type = "text/plain",
    )
    if put_data_result.is_err:
        print("PUT_DATA failed:", put_data_result.unwrap_err())
        return
    print("PUT_DATA SUCCESSFULLY")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(example_run())