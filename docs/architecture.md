
# Architecture 🏗️

`MictlanX` is a distributed storage system designed for high availability. Its architecture separates the control plane (state and coordination) from the data plane (allocations and locations). At high level, the system is composed of interconnected Virtual Storage Spaces (VSS), which provide a unified storage fabric.

The architecture is centered around three main components: `Storage Peers`, `Routers`, and the `Storage Replica Management (SPM)` service.

The system is constructed from three primary service components:

- `Storage Peer`: This is the foundational storage node. It is a lightweight service responsible for the physical I/O of data chunks on a storage device (e.g., SSD, HDD). It exposes a simple API (put, get, delete) for the data it manages and operates without knowledge of the broader cluster topology or data replication strategy. It is the workhorse of the data plane.

- `Router`: The Router is a gateway that serves as the primary entry point for client read and write operations. It abstracts the complexity of the underlying Storage Peers from the client. Its key responsibilities include request handling, directing traffic, and executing the data placement strategy by consulting the SPM to determine which Peers should store or serve data replicas.

- `Storage Replica Management (SPM)`: The SPM is the  control plane of the system. It does not handle high-throughput data I/O. Instead, its sole purpose is to maintain the state and metadata of the entire cluster. This includes tracking cluster availability, maintaining the replica location map, monitoring node health, and orchestrating recovery operations when a node fails.

<div align=center>
  <img src="/mictlanx-client/assets/vss_full.png" width="500" \>
  <p>Fig. Virtual Storage Space conceptual architecture.<p\>
</div>



The Virtual Storage Space (VSS): The Operational Unit


These individual components are logically grouped to form a self-sufficient, fault-tolerant operational unit known as a Virtual Storage Space (VSS).

A VSS is defined as the combination of:

A single SPM service (the control plane).

A set of Routers (the data gateways).

A pool of Storage Peers managed by that SPM.

This modular design creates a self-contained domain that can manage its own state, data, and health.


The MictlanX architecture achieves massive horizontal scalability by federating multiple VSS instances. Each hexagon in the conceptual diagram represents a complete, independent VSS.

These VSS units are interconnected to form a single, unified storage fabric. This allows data processing systems to produce  and consume data across a large-scale and high available storage system that can grow incrementally by adding new VSS modules as capacity or performance needs increase.

<div align=center>
  <img src="/mictlanx-client/assets/conceptual_architecture.png" width="350" \>
  <p>Fig2. A high-level view of interconnected Virtual Storage Spaces (VSS) with I/O ports.<p\>
</div>



## Client SDK layers

The client SDK exposes the VSS at several levels of abstraction, each built on the one below:

| Layer | Class | Use when |
|---|---|---|
| Objects | `Bucket` / `Ball` ([`mictlanx.objects`](api-reference/objects.md)) | Simple, exception-raising bucket/ball API with caching and local access stats |
| High-level | [`AsyncClient`](api-reference/async-client.md) | Chunking, retries, load balancing across routers, caching, `Result` values |
| Mid-level | [`AsyncRouter`](api-reference/router.md) | Router-managed access with peer selection and failover |
| Low-level | [`AsyncPeer`](api-reference/peer.md) | Direct access to a single storage peer |

For local development, [`VirtualStorageSpace`](api-reference/vss.md) deploys a complete VSS (router, summoner, rm and peers) with Docker and scales its peer pool at runtime.
