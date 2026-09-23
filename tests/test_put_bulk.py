import os
import dotenv
import pytest
from mictlanx import AsyncClient
import mictlanx.interfaces as InterfaceX
from uuid import uuid4

# Load environment variables from .env at the start
MICTLANX_ENV_FILE = os.environ.get("MICTLANX_ENV_FILE", ".env.test")
if os.path.exists(MICTLANX_ENV_FILE):
    dotenv.load_dotenv(MICTLANX_ENV_FILE)


@pytest.mark.asyncio
async def test_put_bulk(async_client: AsyncClient):
    """Tests the bulk put functionality with multiple small files."""
    bucket_id = f"test-bulk-bucket-{uuid4().hex}"
    keys      = [f"bulk_test_file_{i}" for i in range(5)]
    contents  = [f"Content of file {i}".encode('utf-8') for i in range(5)]
    bulk_id   = uuid4().hex
    max_concurrency = 3

    balls = []
    for key, content in zip(keys, contents):
        balls.append(
            InterfaceX.BallK(
                bucket_id       = bucket_id,
                ball_id         = f"ball_{key}",
                source          = content,
                chunk_size      = "1MB",
                tags            = {"test_bulk": "true"},
                rf              = 1,
                timeout         = 30,
                max_tries       = 3,
                max_concurrency = 2,
                max_backoff     = 60
            )
        )

    result = await async_client.put_bulk(bulk_id=bulk_id,balls=balls, max_concurrency=max_concurrency)
    print("RESULT", result)
    assert result.is_ok, f"Bulk put failed: {result.unwrap_err()}"

    x = await async_client.await_bulk(bulk_id=bulk_id,remove_on_completion=False)
    assert x.is_ok, f"Awaiting bulk put failed: {x.unwrap_err()}"
    response = x.unwrap()
    print("BULK PUT RESPONSE:", response)
    assert len(response["successes"]) == len(balls), "Not all files were uploaded successfully."
    assert len(response["failures"]) == 0, "Some files failed to upload."