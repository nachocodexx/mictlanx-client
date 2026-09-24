<p align="center">
  <img width="200" src="/mictlanx-client/assets/logo.png" />
</p>

<div align=center>
	<h1>MictlanX <span style="font-weight:normal;"> Client</span></h1>
</div>
<p align="center">
  <!-- Choose one: PyPI or TestPyPI -->
  <a href="https://test.pypi.org/project/mictlanx/">
    <img alt="TestPyPI" src="https://img.shields.io/badge/TestPyPI-mictlanx-blue">
  </a>
  <a href="./LICENSE">
    <img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg">
  </a>
  <img alt="Status: Alpha" src="https://img.shields.io/badge/status-alpha-orange">
  <img alt="Python" src="https://img.shields.io/badge/python-3.9%2B-blue">
</p>

<!-- #  MictlanX  -->

**MictlanX Client** is a Python SDK for a decentralized, router-backed object storage system.  
It lets you PUT/GET large objects reliably across a pool of storage peers through one or more routers, handling chunked transfers, concurrency, retries, and integrity checks for you.

**Highlights**

- **Chunked uploads/downloads** for large files with bounded memory use.

- **Concurrent I/O** for higher throughput.

- **Router-aware**: work with one or many routers; easy to rotate or fail over.

- **End-to-end integrity**: SHA-256 checksum verification on writes/reads.

- **Metadata & tags**: attach and query key/value tags per object (ball) and bucket.

- **Client-side caching** to speed up repeated reads.

- **Bucket/Ball API**: an exception-raising, cache-backed layer (`mictlanx.objects`) for everyday use.

- **Access stats**: local per-ball counters and a decayed access frequency (`client.stats`).

- **Local VSS from Python**: deploy and elastically scale a router + peers with Docker (`mictlanx.vss`).

- **Simple API & examples** to get productive fast.

> MictlanX Client targets object storage use cases (store, fetch, list, and replicate objects).  
> It’s alpha software—interfaces may evolve between minor versions.

