import pytest
from mictlanx.services import AsyncRouter, AsyncPeer
from typing import AsyncGenerator
import mictlanx.interfaces.responses as ResponseModels
from option import Err
import asyncio
import dotenv
import os
import hashlib

dotenv.load_dotenv(".env.test")

MICTLANX_ROUTER_PORT = int(os.environ.get("MICTLANX_ROUTER_PORT", "63666"))


@pytest.fixture
def router() -> AsyncRouter:
    return AsyncRouter(
        router_id   = "mictlanx-router-0",
        ip_addr     = "localhost",
        port        = MICTLANX_ROUTER_PORT,
        protocol    = "http",
        http2       = False,
        api_version = 4
    )


@pytest.fixture
def sample_data():
    """Returns a random bytes payload (approx 1MB) and its checksum."""
    data = os.urandom(1024 * 1024)  # 1MB
    checksum = hashlib.sha256(data).hexdigest()
    return data, checksum

@pytest.fixture
def clean_download_folder(tmp_path):
    """Provides a temporary folder for downloads."""
    folder = tmp_path / "downloads"
    folder.mkdir()
    return str(folder)

# ==========================================
# --- Helper for Data Streaming ---
# ==========================================

async def bytes_generator(data: bytes, chunk_size: int = 65536) -> AsyncGenerator[bytes, None]:
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

# ==========================================
# --- Connectivity & Stats Tests ---
# ==========================================

@pytest.mark.asyncio
async def test_get_stats(router: AsyncRouter):
    """Verifies the Router is reachable and returning stats."""
    res = await router.get_stats()
    assert res.is_ok, f"Failed to get stats: {res.unwrap_err()}"
    stats = res.unwrap()
    assert isinstance(stats, dict)

@pytest.mark.asyncio
async def test_add_peers(router: AsyncRouter):
    """
    Tests adding a peer to the mesh. 
    Note: We send a 'dummy' peer. The router might accept it or fail if unreachable.
    We check IS_OK because usually routers accept the list asynchronously.
    """
    dummy_peer = AsyncPeer(
        peer_id="test-peer-integration",
        ip_addr="127.0.0.1",
        port=9999,
        protocol="http"
    )
    
    res = await router.add_peers([dummy_peer])
    assert res.is_ok, f"Failed to add peer: {res.unwrap_err()}"

# ==========================================
# --- Full Data Lifecycle Test ---
# ==========================================

@pytest.mark.asyncio
async def test_full_object_lifecycle(router: AsyncRouter, unique_id, sample_data, clean_download_folder):
    """
    Performs a complete CRUD cycle:
    1. PUT Metadata
    2. PUT Data (chunked)
    3. GET Metadata
    4. GET Data (to file)
    5. DELETE Object
    """
    data, checksum = sample_data
    bucket = f"testbucket{unique_id}"
    key = f"file{unique_id}"
    ball_id = f"ball{unique_id}"
    
    print(f"\nStarting Lifecycle for {bucket}/{key}")

    # ------------------------------------------------
    # 1. PUT Metadata
    # ------------------------------------------------
    res_meta = await router.put_metadata(
        key                = key,
        size               = len(data),
        checksum           = checksum,
        producer_id        = "pytest-runner",
        content_type       = "application/octet-stream",
        ball_id            = ball_id,
        bucket_id          = bucket,
        replication_factor = 1
    )
    assert res_meta.is_ok, f"Put Metadata failed: {res_meta.unwrap_err()}"
    # ------------------------------------------------
    # 2. PUT Data (Chunked)
    # ------------------------------------------------
    task_id = res_meta.unwrap().tasks_ids[0]
    # We use the ball_id/task_id registered in metadata
    res_put = await router.put_chunked(
        task_id=task_id,
        chunks=bytes_generator(data)
    )
    
    assert res_put.is_ok, f"Put Chunked failed: {res_put.unwrap_err()}"
    
    # put_resp = res_put.unwrap()
    # assert put_resp. == task_id
    # print("Data uploaded.")

    # ------------------------------------------------
    # 3. GET Metadata (Verification)
    # ------------------------------------------------
    print(f"GET Metadata for verification {bucket}/{key}...")
    attempts = 5
    i = 0 
    res_get_meta = Err(Exception("Initial placeholder"))
    while i < attempts and res_get_meta.is_err:
        res_get_meta = await router.get_metadata(bucket, key)
        i+=1
        await asyncio.sleep(1)  # Wait a bit before retrying
        print(f"Attempt {i}/{attempts} to fetch metadata",res_get_meta)
    assert res_get_meta.is_ok
    # fetched_meta = res_get_meta.unwrap()
    # assert fetched_meta.checksum == checksum
    # print("Metadata verified.")

    # ------------------------------------------------
    # 4. GET Data (Download to File)
    # ------------------------------------------------
    filename = f"downloaded-{unique_id}.bin"
    res_download = await router.get_to_file(
        bucket_id        = bucket,
        key              = key,
        sink_folder_path = clean_download_folder,
        filename         = filename
    )
    
    assert res_download.is_ok
    download_path = res_download.unwrap()
    
    # Verify content on disk
    with open(download_path, "rb") as f:
        downloaded_content = f.read()
    
    assert len(downloaded_content) == len(data)

    # ------------------------------------------------
    # 5. GET Bucket Metadata
    # ------------------------------------------------
    res_bkt = await router.get_bucket_metadata(bucket)
    assert res_bkt.is_ok
    # Bucket should exist now
    
    # ------------------------------------------------
    # 6. DELETE Object
    # ------------------------------------------------
    res_del = await router.delete(bucket, key)
    assert res_del.is_ok
    print("Object deleted.")

    # ------------------------------------------------
    # 7. Verify Deletion (Expect 404)
    # ------------------------------------------------
    res_check = await router.get_metadata(bucket, key)
    assert res_check.is_err
    # Depending on your error wrapping, check if it's a 404 or NotFoundError
    err = res_check.unwrap_err()
    print(f"Verification (Expected Error): {err}")

# ==========================================
# --- Edge Cases & Specific Endpoints ---
# ==========================================

@pytest.mark.asyncio
async def test_put_data_direct(router: AsyncRouter, unique_id):
    """Tests the direct `put_data` method (non-chunked/form-data)."""
    bucket = "test-direct"
    key = f"direct-{unique_id}"
    ball_id = f"ball-direct-{unique_id}"
    content = b"Short string data"
    
    # Must create metadata first usually
    res = await router.put_metadata(
        key=key, size=len(content), checksum="abc", producer_id="test",
        content_type="text/plain", ball_id=ball_id, bucket_id=bucket
    )
    assert res.is_ok, f"Put Metadata failed: {res.unwrap_err()}"
    task_id = res.unwrap().tasks_ids[0]
    res = await router.put_data(
        task_id=task_id,
        ball_id=key,
        value=content,
        content_type="text/plain"
    )

    assert res.is_ok, f"Put Data failed: {res.unwrap_err()}"

@pytest.mark.skip(reason="This test assumes the router supports fetching by checksum, which may not be implemented yet.")
@pytest.mark.asyncio
async def test_get_by_checksum_to_file(router: AsyncRouter, unique_id, sample_data, clean_download_folder):
    """Tests downloading a file purely by its checksum."""
    data, checksum = sample_data
    bucket    = "test-checksum"
    key       = f"chk-{unique_id}"
    ball_id   = f"ball-chk-{unique_id}"
    
    # Setup: Upload file first
    res = await router.put_metadata(
        key          = key,
        size         = len(data),
        checksum     = checksum,
        producer_id  = "test",
        content_type = "app/bin",
        ball_id      = ball_id,
        bucket_id    = bucket
    )
    assert res.is_ok, f"Put Metadata failed: {res.unwrap_err()}"
    task_id = res.unwrap().tasks_ids[0]
    res = await router.put_data(
        task_id      = task_id,
        ball_id          = key,
        value        = data,
        content_type = "app/bin"
    )
    assert res.is_ok, f"Put Data failed: {res.unwrap_err()}"
    # await router.put_chunked(task_id, bytes_generator(data))
    
    # Test: Download by Checksum
    res = await router.get_by_checksum_to_file(
        checksum=checksum,
        sink_folder_path=clean_download_folder,
        filename=f"chk_down_{unique_id}.bin"
    )
    
    assert res.is_ok
    assert os.path.exists(res.unwrap())

@pytest.mark.asyncio
async def test_get_chunks_metadata(router: AsyncRouter, unique_id, sample_data):
    """Verifies retrieval of chunk/ball mapping details."""
    data, checksum = sample_data
    bucket = "test-chunks"
    key = f"meta-chk-{unique_id}"
    ball_id = f"ball-meta-{unique_id}"

    # Setup
    res = await router.put_metadata(key=key, size=len(data), checksum=checksum, producer_id="test",
                              content_type="app/bin", ball_id=ball_id, bucket_id=bucket)
    assert res.is_ok, f"Put Metadata failed: {res.unwrap_err()}"
    task_id = res.unwrap().tasks_ids[0]
    res = await router.put_chunked(task_id, bytes_generator(data))
    assert res.is_ok, f"Put Chunked failed: {res.unwrap_err()}"

    # Test
    res = await router.get_chunks_metadata(ball_id=ball_id, bucket_id=bucket)
    
    assert res.is_ok
    meta = res.unwrap()
    assert isinstance(meta, ResponseModels.BallMetadata)
    # Depending on your model, check fields:
    # assert len(meta.chunks) > 0

@pytest.mark.asyncio
async def test_error_404_not_found(router: AsyncRouter, unique_id):
    """Ensure a non-existent key returns an Err result."""
    res = await router.get_metadata(f"nonexistent-bucket-{unique_id}", f"missing-key-{unique_id}")

    assert res.is_err, "Expected an error for non-existent key, but got success."
    # err = res.unwrap_err()
    # Assuming httpx.HTTPStatusError or your custom MictlanXError
    # assert "404" in str(err) or "Not Found" in str(err)

@pytest.mark.asyncio
async def test_disable_key(router: AsyncRouter, unique_id):
    """Test the disable endpoint."""
    bucket = "test-disable"
    key = f"key-{unique_id}"
    ball_id = f"ball-{unique_id}"
    
    # Setup
    res = await router.put_metadata(key=key, size=10, checksum="abc", producer_id="test",
                              content_type="text", ball_id=ball_id, bucket_id=bucket)
    assert res.is_ok, f"Put Metadata failed: {res.unwrap_err()}"
    res = await router.disable(bucket, key)
    assert res.is_ok
    assert res.unwrap() is True