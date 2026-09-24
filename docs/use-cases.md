## Ball Streaming 
### Simple data streaming (Video/GIF)

Represent a video (or GIF) as ```N``` balls that all share ```ball_id = "video123"```:
<div align=center>
  <img src="/mictlanx-client/assets/frames.png" width="350" \>
  <p>Fig. Balls (Data + Metadata) storing frames of a Video/GIF.<p\>
</div>

Each ball = one frame:
- Data: The image bytes for that frame.
- Metadata: frame_number(ordering)

This example is implemented in ```examples/peer/04_put_frames.py```.

You can run it directly from the CLI with your chosen arguments:
```bash
python3 examples/peer/04_put_frames.py --bucket_id videos --ball_id video123 --frames_dir ./examples/data/frames
```

Output
```sh
[128/170] PUT OK -> key=video123_127
[129/170] PUT OK -> key=video123_128
[130/170] PUT OK -> key=video123_129
[131/170] PUT OK -> key=video123_130
[132/170] PUT OK -> key=video123_131
[133/170] PUT OK -> key=video123_132
[134/170] PUT OK -> key=video123_133
[135/170] PUT OK -> key=video123_134
[136/170] PUT OK -> key=video123_135
[137/170] PUT OK -> key=video123_136
[138/170] PUT OK -> key=video123_137
[139/170] PUT OK -> key=video123_138
[140/170] PUT OK -> key=video123_139
[141/170] PUT OK -> key=video123_140
[142/170] PUT OK -> key=video123_141
```

### Example: Video/GIF - Download frames

Once the frames have been stored as balls with the same `ball_id`, you can retrieve them all together.  
The `get_by_ball_id` call returns the list of balls (frames) that belong to the group.  
You then sort them using the `frame_number` tag in their metadata and stream each frame in sequence.

<div align=center>
  <img src="/mictlanx-client/assets/get_frames.png" width="200" >
  <p>Fig. Downloading and rendering frames grouped by <code>ball_id</code>.<p>
</div>

**How it works:**
1. **Query by ball_id** → `peer.get_by_ball_id(bucket_id, ball_id)` returns all frame-balls.  
2. **Order frames** → sort them using the `frame_number` tag to preserve sequence.  
3. **Stream data** → fetch each frame's bytes with `get_streaming`.  
4. **Preview** → display frames in order with a configurable delay (`--delay_ms`) or reconstruct into an animation.

This example is implemented in `examples/peer/05_get_frames.py`.

You can run it directly from the CLI:

```bash
python3 examples/peer/05_get_frames.py --bucket_id videos --ball_id video123 --delay_ms 40
```

---

## Bulk Operations

Upload many objects in one coordinated job using `put_bulk` + `await_bulk`.  Each item can be raw bytes or a file path — the client picks `put` or `put_file` automatically.

### How it works

```
put_bulk(bulk_id, balls)   ← register a batch of upload tasks
await_bulk(bulk_id)        ← wait for all tasks, collect successes/failures
```

Multiple calls to `put_bulk` with the same `bulk_id` **append** more tasks to the same job, so you can fill the queue incrementally before waiting.

### Example

```python
import asyncio
from mictlanx import AsyncClient
from mictlanx.interfaces import BallK

URI = "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0"

async def main():
    client = AsyncClient(uri=URI, client_id="bulk-client")

    # Mix of in-memory bytes and file paths in the same batch
    balls: list[BallK] = [
        {
            "source"    : b"Hello, object 1",
            "bucket_id" : "my-bucket",
            "ball_id"   : "obj-1",
            "tags"      : {"author": "alice"},
        },
        {
            "source"    : b"Hello, object 2",
            "bucket_id" : "my-bucket",
            "ball_id"   : "obj-2",
        },
        {
            "source"    : "/data/report.pdf",   # file path → uses put_file
            "bucket_id" : "my-bucket",
            "ball_id"   : "report-2026",
            "chunk_size": "1MB",
        },
    ]

    # Register the batch under a named job
    reg = await client.put_bulk(
        bulk_id         = "job-2026-05",
        balls           = balls,
        max_concurrency = 5,   # max parallel file uploads
    )
    assert reg.is_ok, reg.unwrap_err()

    # Wait for all tasks and retrieve results
    result = await client.await_bulk(
        bulk_id              = "job-2026-05",
        remove_on_completion = True,
    )

    if result.is_ok:
        response = result.unwrap()
        print(f"Uploaded : {len(response.successes)} objects")
        print(f"Failed   : {len(response.failures)}  objects")
        for item, err in response.failures:
            print(f"  FAILED ball_id={item['ball_id']}: {err.message}")
    else:
        print("Bulk job error:", result.unwrap_err())

asyncio.run(main())
```

### When to use this

| Scenario | Recommendation |
|---|---|
| Ingest 100s of small files | `put_bulk` with `max_concurrency=10` |
| Mixed bytes + file uploads | `put_bulk` handles both transparently |
| Fault-tolerant pipeline | Inspect `response.failures` and retry selectively |

---

## Big Files

Upload and download large files (GBs) directly from/to disk without loading the entire payload into memory.

### Upload with `put_file`

`put_file` reads the file in streaming chunks, computes the checksum incrementally, and uploads all chunks in parallel with retries.

```python
import asyncio
from mictlanx import AsyncClient

URI = "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0"

async def main():
    client = AsyncClient(uri=URI, client_id="bigfile-client")

    result = await client.put_file(
        bucket_id       = "datasets",
        ball_id         = "imagenet-2026",
        path            = "/data/imagenet.tar.gz",
        chunk_size      = "4MB",        # larger chunks → fewer round-trips
        rf              = 2,            # keep 2 replicas
        max_concurrency = 4,            # parallel chunk uploads
        tags            = {
            "fullname"   : "imagenet.tar.gz",
            "extension"  : "tar.gz",
            "description": "ImageNet 2026 dataset",
        },
    )

    if result.is_ok:
        print("Upload complete")
    else:
        print("Upload failed:", result.unwrap_err())

asyncio.run(main())
```

### Download with `get_to_file`

`get_to_file` fetches all chunks in parallel and writes them to disk in order using an async writer loop — no in-memory assembly required.  If a local file with the same SHA-256 already exists the download is skipped automatically (cache hit).

```python
async def download():
    client = AsyncClient(uri=URI, client_id="bigfile-client")

    path_result = await client.get_to_file(
        bucket_id       = "datasets",
        ball_id         = "imagenet-2026",
        output_path     = "/downloads",
        chunk_size      = "4MB",
        max_paralell_gets = 4,
    )

    if path_result.is_ok:
        print("Saved to:", path_result.unwrap())
    else:
        print("Download failed:", path_result.unwrap_err())
```

### Tuning tips

| Parameter | Guidance |
|---|---|
| `chunk_size` | Larger (e.g. `"4MB"`) → fewer metadata round-trips; smaller (e.g. `"256kb"`) → better parallelism on slow links |
| `max_concurrency` / `max_paralell_gets` | Match to your network bandwidth; `4`–`8` is a good starting point |
| `rf` | Set to `2` or higher for datasets you cannot afford to lose |
| `timeout` | Increase beyond the default `120s` for very large chunks on slow connections |

---

## Data Synchronization

Use MictlanX as a shared object store so that multiple independent processes or services can exchange data without direct network connections between them.

### Pattern: Producer → Store → Consumer

```
Producer (writes)         MictlanX VSS         Consumer (reads)
─────────────────    →   ─────────────   →    ─────────────────
put(bucket, ball_id, data)                     get(bucket, ball_id)
```

Because each object is identified by `(bucket_id, ball_id)` and integrity is verified with SHA-256, the consumer always gets exactly what the producer wrote — even across machines or restarts.

### Example

**Producer** — writes a processed result and signals it is ready via a known ball_id:

```python
import asyncio, pickle
from mictlanx import AsyncClient

URI = "mictlanx://mictlanx-router-0@localhost:60666/?protocol=http&api_version=4&http2=0"

async def produce():
    client = AsyncClient(uri=URI, client_id="producer")

    result = {"scores": [0.91, 0.87, 0.95], "model": "v3"}
    payload = pickle.dumps(result)

    ok = await client.put(
        bucket_id  = "pipeline",
        ball_id    = "inference-result-run42",
        value      = payload,
        tags       = {"status": "ready", "run": "42"},
    )
    assert ok.is_ok

asyncio.run(produce())
```

**Consumer** — polls for the ball_id and processes it when available:

```python
import asyncio, pickle
from mictlanx import AsyncClient
from mictlanx.retry import raf, RetryPolicy

async def consume():
    client = AsyncClient(uri=URI, client_id="consumer")

    # Retry up to 10 times with 2 s initial delay (exponential backoff)
    policy = RetryPolicy(retries=10, initial_delay=2.0, backoff_factor=1.5)

    res = await raf(
        func   = lambda: client.get(bucket_id="pipeline", ball_id="inference-result-run42"),
        policy = policy,
    )

    if res.is_ok:
        data = res.unwrap().data.tobytes()
        result = pickle.loads(data)
        print("Received:", result)
    else:
        print("Object not available:", res.unwrap_err())

asyncio.run(consume())
```

### When to use this

| Scenario | Recommendation |
|---|---|
| ML pipeline stages (train → eval → deploy) | Each stage writes its output under a versioned ball_id |
| Microservices sharing large payloads | Avoid embedding blobs in message queues; pass only the ball_id |
| Cross-machine result handoff | Both sides connect to the same VSS; no direct link needed |
| Checkpoint / resume | Write intermediate state with a fixed ball_id; consumer reads on restart |

## Elastic scaling

With `VirtualStorageSpace` you can grow and shrink a local VSS's peer pool while clients are connected. This is useful for testing how an application behaves when capacity changes, or for experiments on data placement and replication.

### How it works

1. `up()` deploys the router, summoner, rm and the initial peers.
2. `expand(n)` (alias `elastic(n)`) summons `n` new peer containers, meshes them with the existing peers and registers them with the router.
3. `retract(n)` removes the `n` most recently added peers (LIFO).
4. `stats()` reports per-peer usage after each change.

### Example

```python
import asyncio
from mictlanx import AsyncClient
from mictlanx.vss import VirtualStorageSpace

async def main():
    async with VirtualStorageSpace(peers=1, vss_id="elastic-demo") as vs:
        async with AsyncClient(uri=vs.uri) as mx:
            bk = mx.bucket("elastic")
            await bk.put("before", b"x" * 1024)

            res = await vs.expand(n=3)
            print("new peers:", res.unwrap(), "size:", vs.size)

            await bk.put("after", b"y" * 1024)   # the router can now place data on the new peers

            stats = await vs.stats()
            for peer_id, s in stats.unwrap().items():
                print(peer_id, s.used_disk, "/", s.total_disk)

            await vs.retract(n=2)
            print("size after retract:", vs.size)

asyncio.run(main())
```

The full version is in `examples/vss/02_elastic_scaling.py`. It requires Docker and `pip install "mictlanx[vss]"`.

### When to use this

| Scenario | Recommendation |
|---|---|
| Integration tests | Deploy a throwaway VSS per test session with `async with` so it is always cleaned up |
| Capacity experiments | `expand()` during a workload and watch `stats()` |
| Failure / churn testing | `retract()` peers and check your application's retries and reads |
