# Virtual Storage Space

`mictlanx.vss.VirtualStorageSpace` deploys a complete local VSS (one router, a summoner, an rm service and a pool of storage peers) from Python through the Docker Engine API, and lets you grow or shrink the peer pool at runtime.

It is meant for local development, tests and experiments. It replaces running `deploy_router.sh` by hand.

## Requirements

- A running Docker daemon.
- The `vss` extra, which installs `docker` (docker-py):

```sh
pip install "mictlanx[vss]"
# or
poetry install --extras vss
```

Creating a `VirtualStorageSpace` never requires `docker`. Only `up()` and the methods that depend on it do. Without it they return `Err(DockerNotAvailableError)`.

## Lifecycle

| Call | Effect |
|---|---|
| `VirtualStorageSpace(peers=2, ...)` | Only stores the desired state; no containers are created |
| `await vs.up()` | Creates the network, router, summoner, rm and `peers` peers, then waits until healthy |
| `await vs.expand(n)` / `await vs.elastic(n)` | Adds `n` peers, meshes them with the existing ones and registers them with the router (runs `up()` first if needed) |
| `await vs.retract(n)` | Removes the `n` most recently added peers (LIFO) |
| `await vs.stats()` | Router-reported stats for each peer |
| `await vs.down()` | Removes every container this VSS created |
| `async with VirtualStorageSpace(...) as vs:` | `up()` on enter, `down()` on exit, **even if the block raises** |

Like the rest of the SDK, these methods return `Result`. `vs.uri` is a ready-to-use `mictlanx://` URI for [`AsyncClient`](async-client.md).

## Example

```python
from mictlanx import AsyncClient
from mictlanx.vss import VirtualStorageSpace

async with VirtualStorageSpace(peers=2, vss_id="my-vss") as vs:
    print(vs.size, vs.peer_ids())

    async with AsyncClient(uri=vs.uri) as mx:
        bk = mx.bucket("bk1")
        await bk.put("b1", b"hello")
        print(await bk.get("b1"))

    await vs.expand(n=2)     # grow the pool while it runs
    await vs.retract(n=1)    # shrink it again (LIFO)
# all containers are removed here
```

!!! note "Retracted peers and the router"
    The router has no endpoint for deregistering a peer. A retracted peer's container is removed, but the router may keep referencing it for a while.

Several VSS instances can run on one host as long as they use different `vss_id`s and ports (`router_port`, `summoner_port`, `rm_port`, `peer_base_port`).

Runnable examples are in `examples/vss/`: `01_deploy.py` (up/down), `02_elastic_scaling.py` (expand/elastic/retract) and `03_context_manager.py` (`async with` + `AsyncClient`).

## VirtualStorageSpace

::: mictlanx.vss.VirtualStorageSpace
