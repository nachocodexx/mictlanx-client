from mictlanx.asyncx import AsyncClient
import pytest
import asyncio

# --- Tests ---

@pytest.mark.asyncio
async def test_delete_object(async_client: AsyncClient, unique_id: str):
    """
    Tests that a newly created object can be successfully deleted.
    This test is self-contained: Setup -> Execute -> Assert.
    """
    bucket_id = f"pytest-bucket-{unique_id}"
    ball_id = f"pytest-ball-{unique_id}"
    test_data = f"This is the test data for ball {ball_id}".encode('utf-8')
    
    print(f"\n[test_delete_object] SETUP: Creating object '{ball_id}' in bucket '{bucket_id}'")
    
    # 1. SETUP: Create the object we intend to delete
    put_result = await async_client.put(
        bucket_id=bucket_id,
        ball_id=ball_id,
        value=test_data,
        rf=1,
        chunk_size="1MB" # Use a sensible default
    )
    print(f"[test_delete_object] Put result: {put_result}")
    assert put_result.is_ok, f"SETUP FAILED: Could not create object: {put_result.unwrap_err()}"

    # 2. EXECUTE: Run the delete operation
    print(f"[test_delete_object] EXECUTE: Deleting object '{ball_id}'")
    delete_result = await async_client.delete(
        bucket_id=bucket_id,
        ball_id=ball_id
    )
    print(f"[test_delete_object] Delete result: {delete_result}")

    # 3. ASSERT: Check that the deletion was reported as successful
    assert delete_result.is_ok, f"delete failed: {delete_result.unwrap_err()}"



@pytest.mark.asyncio
async def test_put_n_balls_in_bucket(async_client: AsyncClient, unique_id: str):
    """
    Tests putting N (e.g., 5) different objects into a single bucket
    concurrently.
    
    It also ensures the bucket is cleaned up afterward.
    """
    N_BALLS = 5  # Define how many "N" is
    bucket_id = f"pytest-n-balls-bucket-{unique_id}"
    
    print(f"\n[test_put_n_balls] SETUP: Targeting bucket '{bucket_id}' for {N_BALLS} objects")

    try:
        # 1. SETUP: Create a list of all 'put' tasks
        put_tasks = []
        for i in range(N_BALLS):
            ball_id = f"ball-{i}-of-{N_BALLS}-{unique_id}"
            test_data = f"Test data for {ball_id}".encode('utf-8')
            
            # Create the coroutine and add it to the list
            # We don't 'await' it yet
            put_tasks.append(
                async_client.put(
                    bucket_id=bucket_id,
                    ball_id=ball_id,
                    value=test_data,
                    rf=1,
                    chunk_size="1MB" # Use a sensible default
                )
            )

        # 2. EXECUTE: Run all 'put' operations concurrently
        print(f"[test_put_n_balls] EXECUTE: Putting {N_BALLS} objects concurrently...")
        results = await asyncio.gather(*put_tasks)
        print(f"[test_put_n_balls] All puts complete. Results: {results}")

        # 3. ASSERT: Check that all 'put' operations were successful
        assert len(results) == N_BALLS, "Did not get the expected number of results"
        
        for i, result in enumerate(results):
            assert result.is_ok, f"Put operation for ball index {i} failed: {result.unwrap_err()}"
        
        print(f"[test_put_n_balls] ASSERT: All {N_BALLS} objects created successfully.")

    finally:
        # 4. TEARDOWN: Clean up the bucket, no matter what happened
        print(f"[test_put_n_balls] TEARDOWN: Deleting bucket '{bucket_id}'")
        delete_result = await async_client.delete_bucket(
            bucket_id=bucket_id, 
            force=True
        )
        print(f"[test_put_n_balls] Teardown result: {delete_result}")
        # We assert here so we know if cleanup failed,
        # but it won't hide the original test failure (if any).
        assert delete_result.is_ok, "TEARDOWN FAILED: Could not delete bucket"