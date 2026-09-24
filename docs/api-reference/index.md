# API Reference 📖

Complete reference for all public classes in the MictlanX Client SDK.

| Class | Page | Description |
|---|---|---|
| `AsyncClient` | [AsyncClient](async-client.md) | High-level client — chunking, retries, load balancing, caching |
| `AsyncPeer` | [AsyncPeer](peer.md) | Direct HTTP client for a single storage peer |
| `AsyncRouter` | [AsyncRouter](router.md) | Router-mediated access with peer selection and failover |
| `MictlanXURI` | [Utilities](utilities.md) | Parse and build `mictlanx://` connection strings |
| `Chunk` / `Chunks` | [Utilities](utilities.md) | Split bytes/files/arrays into fixed-size chunks |
| `RouterLoadBalancer` | [Load Balancing](load-balancing.md) | Least-connections balancer across a router pool |
| `CacheFactory` / `LRUCache` / `LFUCache` / `NoCache` | [Caching](caching.md) | Byte-budget-bounded in-memory caches |
| `Bucket` / `Ball` / `PutManyResult` | [Bucket & Ball](objects.md) | Exception-raising bucket/ball handles on top of `AsyncClient` |
| `AccessStats` / `BallStats` | [Access Stats](stats.md) | Local per-ball access counters and decayed frequency (`client.stats`) |
| `VirtualStorageSpace` | [Virtual Storage Space](vss.md) | Deploy and scale a local VSS from Python (Docker) |
| `MictlanXError` + subclasses | [Errors](errors.md) | Exception hierarchy |
