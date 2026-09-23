import pytest
import dotenv
import asyncio

import pytest_asyncio

from mictlanx.asyncx import AsyncClient
import mictlanx.interfaces as InterfaceX

dotenv.load_dotenv(".env.test")
# --- Fixtures ---

@pytest_asyncio.fixture
async def setup_test_bucket_with_balls(async_client: AsyncClient, unique_id: str):
    """
    Fixture to create a new bucket and PUT 3 test objects into it.
    The 'key' is a simple string, and path info is in 'tags'.
    """
    bucket_id = f"pytest-meta-bucket-{unique_id}"
    
    # Define 3 objects with simple string keys
    test_balls = [
        {"key": "file_a", "data": b"data for file a"},
        {"key": "pic_b", "data": b"data for file b"},
        {"key": "report_c", "data": b"data for file c"},
    ]
    
    print(f"\n[Fixture Setup] Putting {len(test_balls)} objects into bucket '{bucket_id}'")
    
    put_tasks = []
    for ball in test_balls:
        # Construct path and file info for tags
        is_pic = (ball['key'] == 'pic_b')
        extension = "png" if is_pic else "txt"
        rel_path = "images/" if is_pic else "docs/"
        fullname = f"{ball['key']}.{extension}"
        full_path = f"{rel_path}{fullname}"

        put_tasks.append(
            async_client.put(
                bucket_id=bucket_id,
                ball_id=ball["key"],  # Use the simple string key
                value=ball["data"],
                rf=1,
                chunk_size="10kb",
                tags={
                    "extension": extension,
                    "bucket_relative_path": rel_path,
                    "fullname": fullname,
                    "full_path": full_path,
                },
            )
        )
    
    put_results = await asyncio.gather(*put_tasks)
    
    # Check that all puts were successful
    for i, result in enumerate(put_results):
        assert result.is_ok, \
            f"FIXTURE SETUP FAILED: Could not put object {test_balls[i]['key']}: {result.unwrap_err()}"
    
    # Yield the info to the test
    yield (bucket_id, test_balls)
    
    # Teardown: Clean up the bucket
    print(f"\n[Fixture Teardown] Deleting bucket '{bucket_id}'")
    # try:
    delete_result = await async_client.delete_bucket(bucket_id=bucket_id, force=True)
    assert delete_result.is_ok, f"FIXTURE TEARDOWN FAILED: {delete_result.unwrap_err()}"
    # except Exception as e:
        # print(f"FIXTURE TEARDOWN EXCEPTION: {e}")

# --- Tests ---

@pytest.mark.asyncio
async def test_get_metadata_by_key(async_client: AsyncClient, setup_test_bucket_with_balls):
    """
    Tests getting a single object's metadata by its 'full_path' tag.
    """
    bucket_id, test_balls = setup_test_bucket_with_balls
    
    # Select one of the known keys to fetch, based on the NEW fixture
    expected_key = "report_c_0" 
    
    print(f"\n[test_get_metadata_by_key] Getting metadata for key '{expected_key}'")
    
    # 1. EXECUTE: Get the metadata by its key (which we assume is the full_path)
    result = await async_client.get_metadata_by_key(bucket_id=bucket_id, key=expected_key)
    print("result:", result)
    # 2. ASSERT: Check for success
    assert result.is_ok, f"get_metadata_by_key failed: {result.unwrap_err()}"
    
    # 3. ASSERT: Check the metadata content
    _metadata = result.unwrap()
    metadata = _metadata.metadata
    assert metadata.key == expected_key
    assert metadata.tags.get("extension") == "txt"
    assert metadata.tags.get("bucket_relative_path") == "docs/"
    assert metadata.tags.get("fullname") == "report_c.txt"
    assert metadata.tags.get("full_path") == "docs/report_c.txt"
    
    print("[test_get_metadata_by_key] Metadata assertions passed.")

@pytest.mark.asyncio
async def test_get_metadata_by_ball_id(async_client: AsyncClient, setup_test_bucket_with_balls):
    """
    Tests getting a single object's metadata by its ball_id.
    This test first fetches by key to get a valid ball_id to test with.
    """
    bucket_id, test_balls = setup_test_bucket_with_balls
    
    
    ball_id = "pic_b"
    # 2. EXECUTE: Get the metadata using the found ball_id
    # print(f"[test_get_metadata_by_ball_id] EXECUTE: Getting metadata for ball_id '{known_ball_id}'")
    ball_id_result = await async_client.get_metadata(bucket_id=bucket_id, ball_id=ball_id)
    
    # 3. ASSERT: Check for success
    assert ball_id_result.is_ok, f"get_metadata failed: {ball_id_result.unwrap_err()}"
    
    # 4. ASSERT: Check that the metadata returned is the same
    new_metadata: InterfaceX.Metadata = ball_id_result.unwrap()
    
    assert new_metadata.ball_id == ball_id
    # assert new_metadata.full_path == key_to_find
    # Check if the whole object matches (if __eq__ is implemented)
    # assert new_metadata == known_metadata, "Metadata from ball_id did not match metadata from key"
    
    print("[test_get_metadata_by_ball_id] Metadata assertions passed.")