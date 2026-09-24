
from typing import List, Dict, Optional, Tuple, AsyncGenerator
import time as T
from datetime import datetime
import asyncio
import functools
import inspect
from uuid import uuid4
import httpx
import mictlanx.interfaces as InterfaceX
from mictlanx.caching import CacheFactory
from mictlanx.caching.stats import AccessStats
import humanfriendly as HF
from mictlanx.logger import Log
from option import Ok, Some, Result, Err
from mictlanx.utils.index import Utils
from mictlanx.utils.segmentation import Chunks, Chunk
import os
import mictlanx.errors as EX
from mictlanx.asyncx.lb import RouterLoadBalancer
from mictlanx.asyncx.utils import AsyncClientUtils
from xolo.utils.utils import Utils as XoloUtils
from mictlanx.utils.uri import MictlanXURI
from mictlanx.retry import raf, RetryPolicy
from mictlanx.services import AsyncRouter
from mictlanx.types import VerifyType
from mictlanx.asyncx.bulk import _BulkJob
from mictlanx.auth import AuthorizationService
from mictlanx.filters import IOFilter
from mictlanx.objects import Bucket

try:
    from tqdm import tqdm
except ImportError:
    class tqdm:  # type: ignore[no-redef]
        def __init__(self, *a, **kw): pass
        def update(self, *a, **kw): pass
        def set_postfix(self, *a, **kw): pass
        def close(self): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass


def _require_auth(fn):
    """Ensure the client is authenticated before running a public method.

    When ``self.authz`` is ``None`` (the default) this is a no-op passthrough.
    Otherwise it calls :meth:`AsyncClient._ensure_authenticated` first; a
    failure there short-circuits the call and returns ``Err(...)`` without
    ever invoking ``fn``.

    Async generators (e.g. :meth:`AsyncClient.get_chunks`) are wrapped as
    async generators so ``async for`` keeps working; since a generator cannot
    return ``Err``, an auth failure is raised instead.
    """
    def _as_mictlanx_error(e: Exception) -> EX.MictlanXError:
        # Preserve the original error type (e.g. AuthenticationError) —
        # from_exception() only maps by error_code and would otherwise
        # collapse an already-typed MictlanXError into UnknownError.
        return e if isinstance(e, EX.MictlanXError) else EX.MictlanXError.from_exception(e)

    if inspect.isasyncgenfunction(fn):
        @functools.wraps(fn)
        async def gen_wrapper(self: "AsyncClient", *args, **kwargs):
            try:
                await self._ensure_authenticated()
            except Exception as e:
                raise _as_mictlanx_error(e)
            async for item in fn(self, *args, **kwargs):
                yield item
        return gen_wrapper

    @functools.wraps(fn)
    async def wrapper(self: "AsyncClient", *args, **kwargs):
        try:
            await self._ensure_authenticated()
        except Exception as e:
            return Err(_as_mictlanx_error(e))
        return await fn(self, *args, **kwargs)
    return wrapper


class AsyncClient():
    """High-level async client for the MictlanX decentralized storage system.

    Handles chunked transfers, parallel uploads/downloads, exponential-backoff
    retries, SHA-256 integrity verification, and least-connections load
    balancing across a pool of routers.  Configure via a ``mictlanx://`` URI.
    """

    def __init__(
            self,
            uri: str | None = None,
            client_id: Optional[str] = None,
            debug: bool | None = None,
            max_workers: int | None = None,
            log_output_path: str | None = None,
            log_when: str | None = None,
            log_interval: int | None = None,
            eviction_policy: str | None = None,
            capacity_storage: str | None = None,
            verify: VerifyType | None = None,
            enable_logging: bool | None = None,
            use_rich: bool | None = None,
            log_level: int | None = None,
            error_log: bool | None = None,
            authz: Optional[AuthorizationService] = None,
            cache_default: bool | None = None,
            half_life: str | None = None,
    ):
        """Initialise the client and connect it to one or more routers.

        Args:
            uri: ``mictlanx://`` connection string parsed by
                :class:`~mictlanx.utils.uri.MictlanXURI`.  Defaults to the
                ``MICTLANX_CLIENT_URI`` env var.
            client_id: Unique identifier for this client instance; used as
                the logger name and ``producer_id`` on uploads.  Defaults to
                the ``MICTLANX_CLIENT_ID`` env var, or a random hex string.
            debug: When ``True`` log messages are echoed to the console.
                Defaults to the ``MICTLANX_CLIENT_DEBUG`` env var (``1``).
            max_workers: Upper bound on the thread-pool size (capped at
                ``os.cpu_count()``). Defaults to the
                ``MICTLANX_CLIENT_MAX_WORKERS`` env var (``12``).
            log_output_path: Directory for rotating log files. Defaults to
                the ``MICTLANX_LOG_PATH`` env var (resolved inside ``Log``).
            log_when: Rotation time unit passed to
                :class:`~logging.handlers.TimedRotatingFileHandler`.
                Defaults to the ``MICTLANX_LOG_ROTATION_WHEN`` env var
                (resolved inside ``Log``).
            log_interval: Rotation interval. Defaults to the
                ``MICTLANX_LOG_ROTATION_INTERVAL`` env var (resolved inside
                ``Log``).
            eviction_policy: Cache eviction strategy — ``"LRU"`` or
                ``"LFU"``. Defaults to the
                ``MICTLANX_CLIENT_EVICTION_POLICY`` env var (``"LFU"``).
            capacity_storage: Maximum cache size as a humanfriendly string
                (e.g. ``"512MB"``, ``"2GB"``). Defaults to the
                ``MICTLANX_CLIENT_CAPACITY_STORAGE`` env var (``"1GB"``).
            verify: SSL verification passed to ``httpx``. ``False`` disables
                verification; ``True`` uses system CAs; a ``str`` is treated
                as a CA-bundle path; an :class:`ssl.SSLContext` is used
                directly. Defaults to the ``MICTLANX_CLIENT_VERIFY`` env var
                (``0`` = ``False``).
            enable_logging: When ``False`` all logging is suppressed and no
                log directory is created. Defaults to the inverse of the
                ``MICTLANX_LOG_DISABLED`` env var.
            use_rich: When ``True`` the console handler uses RichHandler for
                coloured output. Defaults to the ``MICTLANX_LOG_RICH``
                env var.
            log_level: Minimum log level (e.g. ``logging.INFO``). Defaults to
                the ``MICTLANX_LOG_LEVEL`` env var (``DEBUG`` if unset).
            authz: Optional :class:`~mictlanx.auth.AuthorizationService`.
                When ``None`` (the default) no authorization checks are
                performed — current behaviour is unchanged. When set, the
                client authenticates lazily on the first public method call
                and re-verifies before every subsequent call.
            cache_default: Whether :meth:`get` uses the in-memory cache when
                its ``cache`` argument is not given. Defaults to the
                ``MICTLANX_CLIENT_CACHE_DEFAULT`` env var (``0`` = off). The
                :mod:`mictlanx.objects` handles always enable it.
            half_life: Decay half-life of the access frequency used by
                :attr:`stats` and LFU eviction, as a humanfriendly timespan
                (e.g. ``"10m"``). Defaults to the ``MICTLANX_CLIENT_HALF_LIFE``
                env var (``"10m"``).
        """
        _bool = lambda v: v.lower() in ("1", "true", "yes")

        if uri              is None: uri              = os.environ.get("MICTLANX_CLIENT_URI")
        if uri              is None: raise ValueError("uri is required — pass it directly or set MICTLANX_CLIENT_URI")
        if client_id        is None: client_id        = os.environ.get("MICTLANX_CLIENT_ID")
        if debug            is None: debug            = _bool(os.environ.get("MICTLANX_CLIENT_DEBUG", "1"))
        if max_workers      is None: max_workers      = int(os.environ.get("MICTLANX_CLIENT_MAX_WORKERS", "12"))
        if eviction_policy  is None: eviction_policy  = os.environ.get("MICTLANX_CLIENT_EVICTION_POLICY", "LFU")
        if capacity_storage is None: capacity_storage = os.environ.get("MICTLANX_CLIENT_CAPACITY_STORAGE", "1GB")
        if verify           is None: verify           = _bool(os.environ.get("MICTLANX_CLIENT_VERIFY", "0"))
        if error_log        is None: error_log        = _bool(os.environ.get("MICTLANX_LOG_ERROR_LOG", "1"))
        if cache_default    is None: cache_default    = _bool(os.environ.get("MICTLANX_CLIENT_CACHE_DEFAULT", "0"))
        if half_life        is None: half_life        = os.environ.get("MICTLANX_CLIENT_HALF_LIFE", "10m")
        self.cache_default = cache_default
        self.stats         = AccessStats(half_life_s=HF.parse_timespan(half_life))
        self.cache         = CacheFactory.create(eviction_policy=eviction_policy, capacity_storage=HF.parse_size(capacity_storage), scorer=self.stats.rank)
        self._ctx_tokens   = []

        self.client_id = client_id if client_id is not None else uuid4().hex
        # Peers
        routers = MictlanXURI.parse(uri = uri)
        self.__routers = list(map(AsyncRouter.from_router,routers))
        self.rlb       = RouterLoadBalancer(routers=self.__routers)
        self.default_retry_policy = RetryPolicy(retries=5, initial_delay=1.0, backoff_factor=2.0, max_delay=10.0)
        import logging as _lg
        _console_level = (_lg.CRITICAL + 1) if debug is False else None
        
        disabled = (not enable_logging) if enable_logging is not None else None
        self.__log = Log(
            name                  = self.client_id,
            disabled              = disabled,
            when                  = log_when,
            interval              = log_interval,
            path                  = log_output_path,
            output_path           = "{}/{}.log".format(log_output_path, self.client_id) if log_output_path is not None else None,
            log_level             = log_level,
            use_rich              = use_rich,
            console_handler_level = _console_level,
        )
        self.verify = verify
        max_workers      = os.cpu_count() if max_workers > os.cpu_count() else max_workers
        self.bulk_jobs: Dict[str, _BulkJob] = {}
        self.bulk_jobs_lock = asyncio.Lock()
        self.authz = authz
        self._auth_lock = asyncio.Lock()

    async def __aenter__(self) -> "AsyncClient":
        """Bind this client as the active one for :mod:`mictlanx.objects` handles.

        Inside ``async with AsyncClient(...)``, ``Bucket("bk1")`` resolves to
        this client without passing it explicitly.
        """
        from mictlanx.objects.context import _current_client
        self._ctx_tokens.append(_current_client.set(self))
        return self

    async def __aexit__(self, *exc) -> None:
        from mictlanx.objects.context import _current_client
        if self._ctx_tokens:
            _current_client.reset(self._ctx_tokens.pop())

    def bucket(self, bucket_id: str) -> Bucket:
        """Return a :class:`~mictlanx.objects.Bucket` handle bound to this client."""
        return Bucket(bucket_id, client=self)

    @staticmethod
    def _now_ms() -> str:
        return str(int(T.time() * 1000))

    async def _ensure_authenticated(self):
        """Authenticate ``self.authz`` if needed, or raise if that fails.

        No-op when ``self.authz`` is ``None``. Otherwise: a cheap
        :meth:`~mictlanx.auth.AuthorizationService.verify` check first; only
        if that fails does it acquire ``self._auth_lock`` and call
        :meth:`~mictlanx.auth.AuthorizationService.authenticate`. The lock is
        re-checked with a second ``verify`` after acquisition so that a burst
        of concurrent callers (e.g. ``put_bulk``'s per-item tasks) triggers
        at most one ``authenticate`` call, not one per caller.
        """
        if self.authz is None:
            return
        result = await self.authz.verify()
        if result.is_ok:
            return
        async with self._auth_lock:
            result = await self.authz.verify()
            if result.is_ok:
                return
            auth_result = await self.authz.authenticate()
            if auth_result.is_err:
                _e = auth_result.unwrap_err()
                self.__log.error({
                    "event": "AUTHZ.AUTHENTICATE.FAILED",
                    "message": _e.message,
                    "error_type": type(_e).__name__,
                    "status_code": _e.status_code,
                })
                raise _e
    # PUT METHODS
    @_require_auth
    async def put_chunks(self,bucket_id:str, ball_id:str, chunks:Chunks, tags:Dict[str,str]={}, rf:int =1, timeout:int=120, max_tries:int=5, max_concurrency:int=2,max_backoff:int = 5)->Result[bool, EX.MictlanXError]:
        """Upload a pre-chunked :class:`Chunks` object to a bucket.

        Computes an overall SHA-256 checksum from the chunk stream, then
        uploads all chunks concurrently (bounded by ``max_concurrency``) with
        exponential-backoff retry on failure.

        Args:
            bucket_id: Destination bucket identifier.
            ball_id: Ball identifier used as the chunk key prefix.
            chunks: :class:`Chunks` object whose chunks are uploaded.
            tags: Extra metadata tags stored alongside each chunk.
            rf: Replication factor for each chunk. Defaults to ``1``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            max_tries: Maximum upload attempts per chunk. Defaults to ``5``.
            max_concurrency: Maximum simultaneous chunk uploads. Defaults to ``2``.
            max_backoff: Cap on exponential-backoff sleep in seconds. Defaults to ``5``.

        Returns:
            ``Ok(True)`` when all chunks are uploaded successfully,
            ``Ok(False)`` when the max-availability limit is reached, or
            ``Err(MictlanXError)`` on failure.
        """
        try:
            t1         = T.monotonic()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            updated_at = self._now_ms()
            router     = self.rlb.get_router()
            gen_bytes  = chunks.to_generator()

            (checksum, size) = XoloUtils.sha256_stream(gen_bytes)
            num_chunks = len(chunks)
            semaphore  = asyncio.Semaphore(max_concurrency)

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "rf": rf,
                "timeout": timeout,
                "max_tries": max_tries,
                "max_concurrency": max_concurrency,
                "max_backoff": max_backoff,
                "tags": tags,
            }

            async def upload_chunk(chunk: Chunk, attempt=1) -> Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                __exception = None
                while attempt <= max_tries:
                    chunk_t = T.monotonic()
                    try:
                        async with semaphore:
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                client_id=self.client_id,
                                ball_id=_ball_id,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={**tags, "num_chunks": str(num_chunks), "full_checksum": checksum, "updated_at": updated_at},
                            )
                        if res.is_ok:
                            self.__log.debug({
                                "event": "PUT.CHUNK",
                                "message": "chunk uploaded",
                                "bucket_id": _bucket_id,
                                "ball_id": _ball_id,
                                "key": chunk.chunk_id,
                                "ok": True,
                                "response_time_ms": round((T.monotonic() - chunk_t) * 1000, 2),
                                "input": _input,
                                "context": {"attempt": attempt, "num_chunks": num_chunks},
                            })
                            return (None, Ok(res.unwrap()))
                        else:
                            __exception = res.unwrap_err()
                            raise __exception

                    except Exception as e:
                        backoff = min(2 ** attempt, max_backoff)
                        self.__log.warning({
                            "event": "PUT.CHUNK.RETRY",
                            "message": f"chunk upload failed on attempt {attempt}/{max_tries}",
                            "bucket_id": _bucket_id,
                            "ball_id": _ball_id,
                            "key": chunk.chunk_id,
                            "function": "upload_chunk",
                            "error_type": type(e).__name__,
                            "input": _input,
                            "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                        })
                        await asyncio.sleep(backoff)
                        attempt += 1

                return (chunk, Err(__exception))

            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)

            failures = [res for res in results if res[1].is_err]
            if len(failures) > 0:
                (_, err) = failures[-1]
                _e = err.unwrap_err()
                raise _e

            self.__log.info({
                "event": "PUT",
                "message": "put completed",
                "bucket_id": _bucket_id,
                "ball_id": _ball_id,
                "router": router.router_id,
                "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                "input": _input,
                "context": {"num_chunks": num_chunks, "checksum": checksum, "size": size},
            })
            self.stats.record_put(AccessStats.key(_bucket_id, _ball_id), nbytes=size)
            return Ok(True)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            if isinstance(_e, EX.MaxAvailabilityReachedError):
                self.__log.warning({
                    "event": "MAX.AVAILABILITY.REACHED",
                    "message": "no peers available",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "function": "put_chunks",
                    "error_type": type(_e).__name__,
                    "input": _input,
                    "context": {},
                })
                return Ok(False)
            self.__log.error({
                "event": "PUT.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "function": "put_chunks",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {"failures": len(failures) if 'failures' in dir() else 0},
            })
            return Err(e)
    
    @_require_auth
    async def put_file(self,bucket_id:str, ball_id:str, path:str, tags:Dict[str,str]={}, chunk_size:str="256kb", rf:int =1, timeout:int=120, max_tries:int=5, max_concurrency:int=2,max_backoff:int =5)->Result[bool, EX.MictlanXError]:
        """Upload a file from disk by splitting it into chunks.

        Reads ``path`` from the filesystem, splits it into ``chunk_size``
        chunks, and uploads them concurrently with exponential-backoff retry.

        Args:
            bucket_id: Destination bucket identifier.
            ball_id: Ball identifier used as the chunk key prefix.
            path: Absolute or relative path to the file to upload.
            tags: Extra metadata tags stored alongside each chunk.
            chunk_size: Humanfriendly size string (e.g. ``"256kb"``, ``"4MB"``).
                Defaults to ``"256kb"``.
            rf: Replication factor for each chunk. Defaults to ``1``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            max_tries: Maximum upload attempts per chunk. Defaults to ``5``.
            max_concurrency: Maximum simultaneous chunk uploads. Defaults to ``2``.
            max_backoff: Cap on exponential-backoff sleep in seconds. Defaults to ``5``.

        Returns:
            ``Ok(True)`` on success, ``Ok(False)`` when max-availability is
            reached, or ``Err(MictlanXError)`` on failure.

        Raises:
            UnknownError: If the file cannot be read or chunked.
        """
        try:
            t1          = T.monotonic()
            _bucket_id  = Utils.sanitize_str(bucket_id)
            _ball_id    = Utils.sanitize_str(ball_id)
            updated_at = self._now_ms()
            router      = self.rlb.get_router()
            _chunk_size = HF.parse_size(chunk_size)
            (_, checksum, size) = XoloUtils.extract_path_sha256_size(path=path)

            op_chunks = Chunks.from_file(path=path, group_id=ball_id, chunk_size=Some(_chunk_size))
            if op_chunks.is_none:
                raise EX.UnknownError(message=f"Failed to read the file: {path}")
            chunks = op_chunks.unwrap()
            num_chunks = len(chunks)
            semaphore = asyncio.Semaphore(max_concurrency)

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "path": path,
                "chunk_size": chunk_size,
                "rf": rf,
                "timeout": timeout,
                "max_tries": max_tries,
                "max_concurrency": max_concurrency,
                "max_backoff": max_backoff,
                "tags": tags,
            }

            async def upload_chunk(chunk: Chunk, attempt=1) -> Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                __exception = None
                while attempt <= max_tries:
                    chunk_t = T.monotonic()
                    try:
                        async with semaphore:
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                client_id=self.client_id,
                                ball_id=_ball_id,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={**tags, "num_chunks": str(num_chunks), "full_checksum": checksum, "updated_at": updated_at},
                            )
                        if res.is_ok:
                            self.__log.debug({
                                "event": "PUT.CHUNK",
                                "message": "chunk uploaded",
                                "bucket_id": _bucket_id,
                                "ball_id": _ball_id,
                                "key": chunk.chunk_id,
                                "ok": True,
                                "response_time_ms": round((T.monotonic() - chunk_t) * 1000, 2),
                                "input": _input,
                                "context": {"attempt": attempt, "num_chunks": num_chunks},
                            })
                            return (None, res)
                        else:
                            __exception = res.unwrap_err()
                            raise __exception
                    except Exception as e:
                        backoff = min(2 ** attempt, max_backoff)
                        self.__log.warning({
                            "event": "PUT.CHUNK.RETRY",
                            "message": f"chunk upload failed on attempt {attempt}/{max_tries}",
                            "bucket_id": _bucket_id,
                            "ball_id": _ball_id,
                            "key": chunk.chunk_id,
                            "function": "upload_chunk",
                            "error_type": type(e).__name__,
                            "input": _input,
                            "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                        })
                        await asyncio.sleep(backoff)
                        attempt += 1

                return (chunk, __exception)

            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)

            failures = [res for res in results if res[1].is_err]
            if len(failures) > 0:
                (_, err) = failures[-1]
                e = err.unwrap_err()
                raise e

            self.__log.info({
                "event": "PUT",
                "message": "put completed",
                "bucket_id": _bucket_id,
                "ball_id": _ball_id,
                "router": router.router_id,
                "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                "input": _input,
                "context": {"num_chunks": num_chunks, "checksum": checksum, "size": size},
            })
            self.stats.record_put(AccessStats.key(_bucket_id, _ball_id), nbytes=size)
            return Ok(True)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            if isinstance(_e, EX.MaxAvailabilityReachedError):
                self.__log.warning({
                    "event": "MAX.AVAILABILITY.REACHED",
                    "message": "no peers available",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "function": "put_file",
                    "error_type": type(_e).__name__,
                    "input": _input,
                    "context": {},
                })
                return Ok(False)
            self.__log.error({
                "event": "PUT.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "function": "put_file",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(e)
        

    @_require_auth
    async def put(self, bucket_id: str, ball_id: str, value: bytes, chunk_size: str = "256kb", rf: int = 1, timeout: int = 120,max_tries:int = 5,max_concurrency:int =10,max_backoff:int=5,tags:Dict[str,str]={},filters:Optional[List[IOFilter]]=None,cache:bool=False)->Result[bool,EX.MictlanXError]:
        """Upload raw bytes to a bucket by splitting them into chunks.

        Splits ``value`` into ``chunk_size`` chunks, computes a SHA-256
        checksum, and uploads all chunks concurrently with exponential-backoff
        retry on failure.

        Args:
            bucket_id: Destination bucket identifier.
            ball_id: Ball identifier used as the chunk key prefix.
            value: Raw bytes to upload.
            chunk_size: Humanfriendly size string (e.g. ``"256kb"``).
                Defaults to ``"256kb"``.
            rf: Replication factor for each chunk. Defaults to ``1``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            max_tries: Maximum upload attempts per chunk. Defaults to ``5``.
            max_concurrency: Maximum simultaneous chunk uploads. Defaults to ``10``.
            max_backoff: Cap on exponential-backoff sleep in seconds. Defaults to ``5``.
            tags: Extra metadata tags stored alongside each chunk.
            filters: Ordered list of :class:`~mictlanx.filters.IOFilter`
                transforms applied to ``value`` before chunking (e.g.
                ``[EncryptFilter(key), CompressFilter()]``). Defaults to
                ``None`` (no filters — identical to today's behavior). The
                checksum and the ``mictlanx_filters`` provenance tag are
                computed from the filtered bytes; :meth:`get` must be given
                the correctly-ordered inverse chain to recover the original
                bytes.
            cache: Also store ``value`` (the unfiltered bytes that :meth:`get`
                returns) in the in-memory cache after a successful upload.
                Defaults to ``False``.

        Returns:
            ``Ok(True)`` on success, ``Ok(False)`` when max-availability is
            reached, or ``Err(MictlanXError)`` on failure.
        """
        try:
            t1         = T.monotonic()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            original   = value

            _filters = filters or []
            for f in _filters:
                try:
                    value = f.filter(value)
                except Exception as e:
                    raise EX.FilterExecutionError(f"{f.name} filter raised {type(e).__name__}: {e}") from e

            checksum   = XoloUtils.sha256(value=value)
            chunks_op  = Chunks.from_bytes(data=value, group_id=_ball_id, chunk_size=Some(HF.parse_size(chunk_size)), chunk_prefix=Some(_ball_id))
            router     = self.rlb.get_router()
            if not chunks_op.is_some:
                raise ValueError("No valid chunks to upload.")

            chunks     = chunks_op.unwrap()
            num_chunks = len(chunks)
            chunk_tags = {**tags, "num_chunks": str(num_chunks), "full_checksum": checksum, "mictlanx_filters": ",".join(f.name for f in _filters), "updated_at": self._now_ms()}
            semaphore  = asyncio.Semaphore(max_concurrency)
            progress_bar = tqdm(total=len(value))
            bytes_sent = 0

            _input = {
                "bucket_id": bucket_id,
                "ball_id": _ball_id,
                "chunk_size": chunk_size,
                "rf": rf,
                "timeout": timeout,
                "max_tries": max_tries,
                "max_concurrency": max_concurrency,
                "max_backoff": max_backoff,
                "tags": tags,
                "value_size": len(value),
                "filters": [f.name for f in _filters],
            }

            async def upload_chunk(chunk: Chunk, attempt=1) -> Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                nonlocal bytes_sent
                __exception = None
                while attempt <= max_tries:
                    chunk_t = T.monotonic()
                    try:
                        async with semaphore:
                            res = await AsyncClientUtils.put_chunk(
                                router     = router,
                                ball_id    = _ball_id,
                                client_id  = self.client_id,
                                bucket_id  = _bucket_id,
                                key        = chunk.chunk_id,
                                chunk      = chunk,
                                rf         = rf,
                                timeout    = timeout,
                                metadata   = chunk_tags,
                                chunk_size = "1MB",
                            )
                        if res.is_ok:
                            elapsed_ms = round((T.monotonic() - chunk_t) * 1000, 2)
                            bytes_sent += chunk.size
                            elapsed_s = T.monotonic() - t1
                            throughput = bytes_sent / elapsed_s / 1_048_576 if elapsed_s > 0 else 0
                            progress_bar.update(chunk.size)
                            progress_bar.set_postfix({
                                "chunk": chunk.chunk_id,
                                "resp": f"{elapsed_ms}ms",
                                "sent": f"{bytes_sent // 1024}KB",
                                "speed": f"{throughput:.2f}MB/s",
                                "time": datetime.now().strftime("%H:%M:%S"),
                            })
                            self.__log.debug({
                                "event": "PUT.CHUNK",
                                "message": "chunk uploaded",
                                "bucket_id": _bucket_id,
                                "ball_id": _ball_id,
                                "key": chunk.chunk_id,
                                "ok": True,
                                "response_time_ms": elapsed_ms,
                                "input": _input,
                                "context": {"attempt": attempt, "num_chunks": num_chunks, "bytes_sent": bytes_sent},
                            })
                            return (None, Ok(res.unwrap()))
                        else:
                            __exception = res.unwrap_err()
                            raise __exception
                    except Exception as e:
                        backoff = min(2 ** attempt, max_backoff)
                        self.__log.warning({
                            "event": "PUT.CHUNK.RETRY",
                            "message": f"chunk upload failed on attempt {attempt}/{max_tries}",
                            "bucket_id": _bucket_id,
                            "ball_id": _ball_id,
                            "key": chunk.chunk_id,
                            "function": "upload_chunk",
                            "error_type": type(e).__name__,
                            "input": _input,
                            "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                        })
                        await asyncio.sleep(backoff)
                        attempt += 1

                return (chunk, Err(__exception))

            upload_tasks = [upload_chunk(chunk) for chunk in chunks.iter()]
            results = await asyncio.gather(*upload_tasks)
            progress_bar.close()

            failures = [res for res in results if res[1].is_err]
            if len(failures) > 0:
                (_, last_failure) = failures[-1]
                e = last_failure.unwrap_err()
                raise e

            elapsed_s = T.monotonic() - t1
            throughput = bytes_sent / elapsed_s / 1_048_576 if elapsed_s > 0 else 0
            self.__log.info({
                "event": "PUT",
                "message": "put completed",
                "bucket_id": _bucket_id,
                "ball_id": _ball_id,
                "router": router.router_id,
                "response_time_ms": round(elapsed_s * 1000, 2),
                "input": _input,
                "context": {
                    "num_chunks": num_chunks,
                    "checksum": checksum,
                    "bytes_sent": bytes_sent,
                    "throughput_mbs": round(throughput, 3),
                },
            })
            key = AccessStats.key(_bucket_id, _ball_id)
            self.stats.record_put(key, nbytes=len(value))
            if cache:
                self.cache.put(key, original, InterfaceX.Metadata(
                    key          = f"{_ball_id}_0",
                    size         = len(value),
                    checksum     = checksum,
                    tags         = {**chunk_tags, "index": "0", "mictlanx_get_filters": ",".join(reversed([f.name for f in _filters]))},
                    content_type = "application/octet-stream",
                    producer_id  = self.client_id,
                    ball_id      = _ball_id,
                    bucket_id    = _bucket_id,
                ))
            return Ok(True)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            if isinstance(e, EX.MaxAvailabilityReachedError):
                self.__log.warning({
                    "event": "MAX.AVAILABILITY.REACHED",
                    "message": "no peers available",
                    "bucket_id": _bucket_id,
                    "ball_id": _ball_id,
                    "function": "put",
                    "error_type": type(_e).__name__,
                    "input": _input,
                    "context": {},
                })
                return Ok(False)
            self.__log.error({
                "event": "PUT.ERROR",
                "message": _e.message,
                "bucket_id": _bucket_id,
                "ball_id": _ball_id,
                "function": "put",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {"failures": len(failures) if 'failures' in dir() else 0},
            })
            return Err(e)

    @_require_auth
    async def put_with_metadata(
        self,
        bucket_id: str,
        ball_id: str,
        value: bytes,
        chunk_size: str = "256kb",
        rf: int = 1,
        timeout: int = 120,
        max_tries: int = 5,
        max_concurrency: int = 10,
        max_backoff: int = 5,
        tags: Dict[str, str] = {},
    ) -> Result["InterfaceX.Ball", EX.MictlanXError]:
        """Upload bytes and return the assembled :class:`Ball` metadata.

        Identical to :meth:`put` but resolves the uploaded ball's chunk
        metadata after a successful upload and returns it as a
        :class:`Ball` object, so callers can inspect chunk keys, sizes,
        checksums, and tags without a separate fetch.

        Args:
            bucket_id: Destination bucket identifier.
            ball_id: Ball identifier.
            value: Raw bytes to upload.
            chunk_size: Target size per chunk. Defaults to ``"256kb"``.
            rf: Replication factor. Defaults to ``1``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            max_tries: Maximum upload attempts per chunk. Defaults to ``5``.
            max_concurrency: Maximum parallel chunk uploads. Defaults to ``10``.
            max_backoff: Maximum backoff cap in seconds. Defaults to ``5``.
            tags: Additional metadata tags merged into every chunk. Defaults to ``{}``.

        Returns:
            ``Ok(Ball)`` with all chunk metadata populated, or
            ``Err(MictlanXError)`` on failure.
        """
        put_result = await self.put(
            bucket_id=bucket_id,
            ball_id=ball_id,
            value=value,
            chunk_size=chunk_size,
            rf=rf,
            timeout=timeout,
            max_tries=max_tries,
            max_concurrency=max_concurrency,
            max_backoff=max_backoff,
            tags=tags,
        )
        if put_result.is_err:
            return Err(put_result.unwrap_err())
        return await self.get_metadata(bucket_id=bucket_id, ball_id=ball_id)

    @_require_auth
    async def put_single_chunk(self, bucket_id: str, ball_id: str, chunk: Chunk, chunk_size: str = "256kb", rf: int = 1, timeout: int = 120,max_tries:int = 5,max_concurrency:int =10,max_backoff:int=5,tags:Dict[str,str]={})->Result[bool,EX.MictlanXError]:
        """Upload a single pre-built :class:`Chunk` to a bucket.

        Uploads one chunk under the given ``ball_id``, with exponential-backoff
        retry on failure.

        Args:
            bucket_id: Destination bucket identifier.
            ball_id: Ball identifier the chunk belongs to.
            chunk: :class:`Chunk` instance to upload.
            chunk_size: Streaming buffer size as a humanfriendly string.
                Defaults to ``"256kb"``.
            rf: Replication factor. Defaults to ``1``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            max_tries: Maximum upload attempts. Defaults to ``5``.
            max_concurrency: Semaphore width (unused for a single chunk, kept
                for API consistency). Defaults to ``10``.
            max_backoff: Cap on exponential-backoff sleep in seconds. Defaults to ``5``.
            tags: Extra metadata tags stored with the chunk.

        Returns:
            ``Ok(True)`` on success, ``Ok(False)`` when max-availability is
            reached, or ``Err(MictlanXError)`` on failure.
        """
        try:
            t1           = T.monotonic()
            _bucket_id   = Utils.sanitize_str(bucket_id)
            _ball_id     = Utils.sanitize_str(ball_id)
            updated_at = self._now_ms()
            router       = self.rlb.get_router()
            semaphore    = asyncio.Semaphore(max_concurrency)
            progress_bar = tqdm(total=chunk.size)
            bytes_sent   = 0

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "chunk_id": chunk.chunk_id,
                "chunk_size": chunk_size,
                "rf": rf,
                "timeout": timeout,
                "max_tries": max_tries,
                "max_concurrency": max_concurrency,
                "max_backoff": max_backoff,
                "tags": tags,
            }

            async def upload_chunk(chunk: Chunk, attempt=1) -> Tuple[Chunk, Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]]:
                """Uploads a chunk and retries if it fails."""
                nonlocal bytes_sent
                __exception = None
                while attempt <= max_tries:
                    chunk_t = T.monotonic()
                    try:
                        async with semaphore:
                            res = await AsyncClientUtils.put_chunk(
                                router=router,
                                ball_id=_ball_id,
                                client_id=self.client_id,
                                bucket_id=_bucket_id,
                                key=chunk.chunk_id,
                                chunk=chunk,
                                rf=rf,
                                timeout=timeout,
                                metadata={**tags, "updated_at": updated_at},
                                chunk_size="1MB",
                            )
                        if res.is_ok:
                            elapsed_ms = round((T.monotonic() - chunk_t) * 1000, 2)
                            bytes_sent += chunk.size
                            elapsed_s = T.monotonic() - t1
                            throughput = bytes_sent / elapsed_s / 1_048_576 if elapsed_s > 0 else 0
                            progress_bar.update(chunk.size)
                            progress_bar.set_postfix({
                                "resp": f"{elapsed_ms}ms",
                                "sent": f"{bytes_sent // 1024}KB",
                                "speed": f"{throughput:.2f}MB/s",
                                "time": datetime.now().strftime("%H:%M:%S"),
                            })
                            self.__log.debug({
                                "event": "PUT.CHUNK",
                                "message": "chunk uploaded",
                                "bucket_id": _bucket_id,
                                "ball_id": ball_id,
                                "key": chunk.chunk_id,
                                "ok": True,
                                "response_time_ms": elapsed_ms,
                                "input": _input,
                                "context": {"attempt": attempt},
                            })
                            return (None, Ok(res.unwrap()))
                        else:
                            __exception = res.unwrap_err()
                            raise __exception
                    except Exception as e:
                        backoff = min(2 ** attempt, max_backoff)
                        self.__log.warning({
                            "event": "PUT.CHUNK.RETRY",
                            "message": f"chunk upload failed on attempt {attempt}/{max_tries}",
                            "bucket_id": _bucket_id,
                            "ball_id": ball_id,
                            "key": chunk.chunk_id,
                            "function": "upload_chunk",
                            "error_type": type(e).__name__,
                            "input": _input,
                            "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                        })
                        await asyncio.sleep(backoff)
                        attempt += 1

                return (chunk, Err(__exception))

            upload_tasks = [upload_chunk(chunk=chunk)]
            results = await asyncio.gather(*upload_tasks)
            progress_bar.close()

            failures = [res for res in results if res[1].is_err]
            if len(failures) > 0:
                messages = "\n".join([str(res.unwrap_err()) for _, res in failures])
                self.__log.error({
                    "event": "PUT.CHUNKS.ERROR",
                    "message": "single chunk upload failed",
                    "bucket_id": _bucket_id,
                    "ball_id": ball_id,
                    "key": chunk.chunk_id,
                    "function": "put_single_chunk",
                    "error_type": "PutChunksError",
                    "input": _input,
                    "context": {"raw": messages},
                })
                return Err(EX.PutChunksError(message=messages))

            self.__log.info({
                "event": "PUT",
                "message": "put completed",
                "bucket_id": _bucket_id,
                "ball_id": _ball_id,
                "key": chunk.chunk_id,
                "router": router.router_id,
                "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                "input": _input,
                "context": {"bytes_sent": bytes_sent},
            })
            return Ok(True)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            if isinstance(_e, EX.MaxAvailabilityReachedError):
                self.__log.warning({
                    "event": "MAX.AVAILABILITY.REACHED",
                    "message": "no peers available",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": chunk.chunk_id,
                    "function": "put_single_chunk",
                    "error_type": type(_e).__name__,
                    "input": _input,
                    "context": {},
                })
                return Ok(False)
            self.__log.error({
                "event": "PUT.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": chunk.chunk_id,
                "function": "put_single_chunk",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(e)

    
    
    @_require_auth
    async def put_bulk(
        self,
        bulk_id:str, 
        balls: List[InterfaceX.BallK],
        max_concurrency: int = 10,
        stop_on_failure: bool = False
    ) -> Result[bool, EX.MictlanXError]:
        """Register a batch of upload items under a named bulk job.

        Creates (or resumes) a :class:`_BulkJob` identified by ``bulk_id``,
        then enqueues one async task per item.  Each task calls :meth:`put`
        (bytes) or :meth:`put_file` (path string) depending on the item's
        ``source`` field.  Use :meth:`await_bulk` to wait for completion and
        collect results.

        Args:
            bulk_id: Unique identifier for this bulk job.  Calling
                ``put_bulk`` again with the same ``bulk_id`` appends more
                tasks to the existing job.
            balls: List of :class:`BallK` typed-dicts describing what to
                upload.  Each dict must have ``source`` (``bytes`` or file
                path ``str``), ``bucket_id``, and ``ball_id``; optional keys
                include ``tags``, ``rf``, ``chunk_size``, ``timeout``,
                ``max_tries``, ``max_concurrency``, and ``max_backoff``.
            max_concurrency: Maximum items uploaded in parallel. Defaults to
                ``10``.
            stop_on_failure: Reserved — not yet enforced in this
                implementation.  Defaults to ``False``.

        Returns:
            ``Ok(True)`` after tasks are registered, or
            ``Err(MictlanXError)`` if a setup error occurs.
        """
        try:
            async with self.bulk_jobs_lock:
                job = self.bulk_jobs.get(bulk_id)
                if not job:
                    self.__log.info(f"Creating new bulk job '{bulk_id}' with max_concurrency={max_concurrency}")
                    job = _BulkJob(
                        bulk_id         = bulk_id,
                        max_concurrency = max_concurrency,
                        logger          = self.__log
                    )
                    self.bulk_jobs[bulk_id] = job
            # --- 2. Define the Task Processor ---
            async def _process_item(item: InterfaceX.BallK) -> Result[InterfaceX.BallKDTO, Tuple[InterfaceX.BallKDTO, EX.MictlanXError]]:
                """
                Coroutine wrapper for a single 'put' or 'put_file' operation.
                Manages semaphore and error/success reporting.
                """
                # Use the job's specific semaphore
                async with job.semaphore:
                    try:
                        source    = item['source']
                        bucket_id = item['bucket_id']
                        ball_id   = item['ball_id']
                        
                          # Get optional params with defaults from the item dict
                        tags                 = item.get('tags', {})
                        rf                   = item.get('rf', 1)
                        chunk_size           = item.get('chunk_size', '256kb')
                        timeout              = item.get('timeout', 120)
                        max_tries            = item.get('max_tries', 5)
                        max_concurrency_file = item.get('max_concurrency', 10)  # Concurrency for chunks
                        max_backoff          = item.get('max_backoff', 5)

                        if isinstance(source, str):
                            # It's a file path, use put_file
                            result = await self.put_file(
                                bucket_id=bucket_id,
                                ball_id=ball_id,
                                path=source,
                                tags=tags,
                                chunk_size=chunk_size,
                                rf=rf,
                                timeout=timeout,
                                max_tries=max_tries,
                                max_concurrency=max_concurrency_file,
                                max_backoff=max_backoff
                            )
                        elif isinstance(source, bytes):
                            # It's in-memory data, use put
                            result = await self.put(
                                bucket_id=bucket_id,
                                ball_id=ball_id,
                                value=source,
                                tags=tags,
                                chunk_size=chunk_size,
                                rf=rf,
                                timeout=timeout,
                                max_tries=max_tries,
                                max_concurrency=max_concurrency_file,
                                max_backoff=max_backoff
                            )
                        else:
                            raise EX.ValidationError(
                                f"Invalid source type for ball_id '{ball_id}': "
                                f"{type(source)}. Must be 'str' (path) or 'bytes'."
                            )

                        if result.is_ok:
                            return Ok(InterfaceX.BallKDTO.from_ballk(item))
                        else:
                            # Propagate the error from put/put_file
                            raise result.unwrap_err()

                    except Exception as e:
                        _e = EX.MictlanXError.from_exception(e)
                        self.__log.warning({
                            "event": "PUT.BULK.ITEM.FAILURE",
                            "bucket_id": item.get('bucket_id'),
                            "ball_id": item.get('ball_id'),
                            "error": _e.message
                        })
                        # Return Err with item and error
                        return Err((InterfaceX.BallKDTO.from_ballk(item), _e))

            # --- 3. Create and Add Tasks to the Job ---
            new_tasks = []
            for ball in balls:
                # Create the task and add it to our temp list
                new_tasks.append(asyncio.create_task(_process_item(ball)))
            
            # Add all new tasks to the job in a thread-safe way
            await job.add_tasks(new_tasks)
            
            self.__log.info(f"Registered {len(new_tasks)} new tasks for bulk_id '{bulk_id}'. Total tasks: {len(job.tasks)}")
            return Ok(True)

        except Exception as e:
            # Catches setup errors (e.g., if 'balls' is not a list)
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "PUT.BULK.EXCEPTION",
                "name": _e.get_name(),
                "message": _e.message,
                "status": _e.status_code, 
            })
            return Err(_e)

    @_require_auth
    async def await_bulk(
        self,
        bulk_id: str,
        remove_on_completion: bool = False
    ) -> Result[InterfaceX.BulkPutResponse, EX.MictlanXError]:
        """Wait for all tasks in a bulk job and return aggregated results.

        Blocks until every task registered under ``bulk_id`` (via
        :meth:`put_bulk`) has completed, then returns a
        :class:`BulkPutResponse` summarising successes and failures.

        Args:
            bulk_id: The bulk job identifier passed to :meth:`put_bulk`.
            remove_on_completion: When ``True``, the job is removed from
                memory after results are returned, freeing resources.
                Defaults to ``False``.

        Returns:
            ``Ok(BulkPutResponse)`` with ``successes`` and ``failures``
            lists, or ``Err(MictlanXError)`` if the job is not found or
            an unexpected error occurs while waiting.
        """
        # --- 1. Get the Job ---
        async with self.bulk_jobs_lock:
            job = self.bulk_jobs.get(bulk_id)
        
        if not job:
            self.__log.warning(f"No bulk job found with id '{bulk_id}'")
            return Err(EX.NotFoundError(f"No bulk job found with id '{bulk_id}'"))

        # --- 2. Await Completion ---
        try:
            response = await job.wait_for_completion()
            
            # --- 3. (Optional) Cleanup ---
            if remove_on_completion:
                async with self.bulk_jobs_lock:
                    popped_job = self.bulk_jobs.pop(bulk_id, None)
                    if popped_job:
                        self.__log.info(f"Removed completed bulk job '{bulk_id}' from memory.")
            
            return Ok(response)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "AWAIT.BULK.FATAL.ERROR",
                "bulk_id": bulk_id,
                "name": _e.get_name(),
                "message": _e.message,
                "status": _e.status_code, 
            })
            return Err(_e)

    # GET METHODS
    @_require_auth
    async def get_chunk(
        self,
        bucket_id: str,
        ball_id: str,
        index: int,
        max_parallel_gets: int = 10,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",
        timeout: int = 120,
        http2: bool = False,
        max_retries: int = 15,
        delay: float = 1.0,
        backoff_factor: float = 0.5,
        force: bool = False,
        max_backoff:int =5,
        max_delay:int = 30,
        jitter:bool = True
    )->Result[Tuple[Chunk, InterfaceX.Metadata], EX.MictlanXError]:
        """Download a single numbered chunk from a ball.

        Fetches the metadata for ``{ball_id}_{index}`` first (with retry),
        then streams the chunk bytes, returning both together.

        Args:
            bucket_id: Source bucket identifier.
            ball_id: Ball identifier whose chunk is fetched.
            index: Zero-based chunk index.
            max_parallel_gets: Semaphore width for concurrent fetches.
                Defaults to ``10``.
            headers: Extra HTTP headers sent with requests.
            chunk_size: Streaming read-buffer size as a humanfriendly string.
                Defaults to ``"256kb"``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            http2: Enable HTTP/2 for the underlying ``httpx`` client.
                Defaults to ``False``.
            max_retries: Maximum fetch attempts before giving up. Defaults to
                ``15``.
            delay: Initial backoff delay in seconds. Defaults to ``1.0``.
            backoff_factor: Multiplier applied to ``delay`` each retry.
                Defaults to ``0.5``.
            force: Pass a ``Force-Get`` header to bypass server-side caching.
                Defaults to ``False``.
            max_backoff: Cap on backoff sleep in seconds. Defaults to ``5``.
            max_delay: Hard ceiling on retry delay for the metadata
                :class:`RetryPolicy`. Defaults to ``30``.
            jitter: Add random jitter to retry delays. Defaults to ``True``.

        Returns:
            ``Ok((Chunk, Metadata))`` on success or raises
            :class:`MictlanXError` on unrecoverable failure.

        Raises:
            MictlanXError: Propagated on unrecoverable fetch errors.
        """
        try:
            t1         = T.monotonic()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            headers["Force-Get"] = str(headers.get("Force", str(int(force))))

            router       = self.rlb.get_router()
            chunk_key    = f"{_ball_id}_{index}"
            retry_policy = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay, jitter=jitter)

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "index": index,
                "chunk_size": chunk_size,
                "timeout": timeout,
                "max_retries": max_retries,
                "delay": delay,
                "backoff_factor": backoff_factor,
                "force": force,
                "max_backoff": max_backoff,
            }

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=bucket_id, key=chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({
                    "event": "GET.METADATA.FAILED.ATTEMPT",
                    "message": "metadata fetch attempt failed",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": chunk_key,
                    "input": _input,
                    "context": {"attempt": i, "max_attempts": retry_policy.retries},
                }),
                on_error   = lambda i, e: self.__log.error({
                    "event": "GET.METADATA.ERROR",
                    "message": str(e.message),
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": chunk_key,
                    "function": "get_chunk",
                    "error_type": type(e).__name__,
                    "status_code": e.status_code,
                    "input": _input,
                    "context": {"attempt": i},
                }),
            )

            if metadata_result.is_err:
                raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())
            metadata = metadata_result.unwrap()
            pbar = tqdm(total=1)

            async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:
                semaphore = asyncio.Semaphore(max_parallel_gets)

                async def fetch_chunk_with_retry():
                    attempt = 0
                    while attempt < max_retries:
                        try:
                            async with semaphore:
                                t2 = T.monotonic()
                                res = await AsyncClientUtils.get_chunk(
                                    client=client,
                                    router=router,
                                    bucket_id=_bucket_id,
                                    key=chunk_key,
                                    chunk_size=chunk_size,
                                    headers=headers,
                                )
                                elapsed_ms = round((T.monotonic() - t2) * 1000, 2)
                                if res.is_ok:
                                    pbar.set_postfix({
                                        "chunk": 1,
                                        "resp": f"{elapsed_ms}ms",
                                        "time": datetime.now().strftime("%H:%M:%S"),
                                    })
                                    pbar.update(n=1)
                                    self.__log.debug({
                                        "event": "GET.CHUNK",
                                        "message": "chunk fetched",
                                        "bucket_id": bucket_id,
                                        "ball_id": ball_id,
                                        "key": chunk_key,
                                        "ok": True,
                                        "response_time_ms": elapsed_ms,
                                        "input": _input,
                                        "context": {"chunk_size": chunk_size},
                                    })
                                    return res
                                else:
                                    raise EX.GetChunkError()

                        except Exception as e:
                            attempt += 1
                            current_backoff = delay * (backoff_factor ** (attempt - 1))
                            backoff = min(current_backoff, max_backoff) if max_backoff > 0 else current_backoff
                            await asyncio.sleep(backoff)
                            self.__log.warning({
                                "event": "GET.CHUNK.RETRY",
                                "message": f"chunk fetch failed on attempt {attempt}/{max_retries}",
                                "bucket_id": bucket_id,
                                "ball_id": ball_id,
                                "key": chunk_key,
                                "function": "fetch_chunk_with_retry",
                                "error_type": type(e).__name__,
                                "input": _input,
                                "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                            })
                    return Err(EX.GetChunkError(f"Failed to fetch chunk after {max_retries} attempts"))

                futures = [fetch_chunk_with_retry()]

                for coro in asyncio.as_completed(futures):
                    result = await coro
                    if result.is_ok:
                        (metadata, data) = result.unwrap()
                        pbar.close()
                        self.__log.info({
                            "event": "GET",
                            "message": "get completed",
                            "bucket_id": _bucket_id,
                            "ball_id": ball_id,
                            "key": chunk_key,
                            "router": router.router_id,
                            "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                            "input": _input,
                            "context": {"chunk_size": chunk_size},
                        })
                        c = Chunk.from_bytes(group_id=_ball_id, index=index, data=data.tobytes(), metadata={**metadata.tags}, chunk_id=Some(chunk_key))
                        return Ok((c, metadata))
                    else:
                        e = result.unwrap_err()
                        self.__log.error({
                            "event": "CHUNK.FAILURE",
                            "message": e.message,
                            "bucket_id": _bucket_id,
                            "ball_id": ball_id,
                            "key": chunk_key,
                            "function": "get_chunk",
                            "error_type": type(e).__name__,
                            "status_code": e.status_code,
                            "input": _input,
                            "context": {},
                        })
                        return Err(e)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "GET.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": f"{ball_id}_{index}",
                "function": "get_chunk",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            raise _e
    @_require_auth
    async def get_chunks(self,
        bucket_id: str,
        ball_id: str,
        max_parallel_gets: int = 10,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",
        timeout: int = 120,
        http2: bool = False,
        max_retries: int = 15,
        delay: float = 1.0,
        backoff_factor: float = 0.5,
        force: bool = False,
        max_backoff:int =5,
        chunk_index:int = 0,
        max_delay:int =30,
        jitter:bool = True,
        order:bool = False
    ) -> AsyncGenerator[Tuple[InterfaceX.Metadata, memoryview], None]:
        """Async-generate all chunks of a ball as ``(Metadata, memoryview)`` pairs.

        Fetches the first chunk's metadata to discover ``num_chunks``, then
        downloads all chunks concurrently.  By default chunks are yielded as
        they arrive (fastest); set ``order=True`` to yield them in index order.

        Args:
            bucket_id: Source bucket identifier.
            ball_id: Ball identifier (chunk keys are ``{ball_id}_{i}``).
            max_parallel_gets: Maximum concurrent chunk downloads. Defaults to
                ``10``.
            headers: Extra HTTP headers sent with each request.
            chunk_size: Read-buffer size as a humanfriendly string. Defaults
                to ``"256kb"``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            http2: Enable HTTP/2 on the underlying client. Defaults to
                ``False``.
            max_retries: Maximum attempts per chunk. Defaults to ``15``.
            delay: Initial backoff delay in seconds. Defaults to ``1.0``.
            backoff_factor: Delay multiplier per retry. Defaults to ``0.5``.
            force: Send ``Force-Get`` header to bypass caching. Defaults to
                ``False``.
            max_backoff: Ceiling on backoff sleep in seconds. Defaults to ``5``.
            chunk_index: Starting chunk index used to discover metadata.
                Defaults to ``0``.
            max_delay: Hard ceiling on retry delay for the metadata
                :class:`RetryPolicy`. Defaults to ``30``.
            jitter: Randomise retry delays. Defaults to ``True``.
            order: When ``True``, wait for all chunks then yield in order.
                When ``False`` (default), yield as soon as each chunk arrives.

        Yields:
            ``(Metadata, memoryview)`` tuples, one per chunk.

        Raises:
            MictlanXError: On metadata fetch failure, missing chunks, or
                per-chunk fetch exhaustion.
        """
        try:
            t1         = T.monotonic()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            headers["Force-Get"] = str(headers.get("Force", str(int(force))))

            router            = self.rlb.get_router()
            initial_chunk_key = f"{_ball_id}{'_'+str(chunk_index) if chunk_index >= 0 else ''}"
            retry_policy      = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay, jitter=jitter)

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "chunk_size": chunk_size,
                "timeout": timeout,
                "max_retries": max_retries,
                "delay": delay,
                "backoff_factor": backoff_factor,
                "force": force,
                "max_backoff": max_backoff,
                "chunk_index": chunk_index,
                "order": order,
            }

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=initial_chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({
                    "event": "GET.METADATA.FAILED.ATTEMPT",
                    "message": "metadata fetch attempt failed",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": initial_chunk_key,
                    "input": _input,
                    "context": {"attempt": i, "max_attempts": retry_policy.retries},
                }),
                on_error   = lambda i, e: self.__log.error({
                    "event": "GET.METADATA.ERROR",
                    "message": str(e.message),
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": initial_chunk_key,
                    "function": "get_chunks",
                    "error_type": type(e).__name__,
                    "status_code": e.status_code,
                    "input": _input,
                    "context": {"attempt": i},
                }),
            )

            if metadata_result.is_err:
                raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())

            metadata   = metadata_result.unwrap()
            num_chunks = int(metadata.metadata.tags.get("num_chunks"))
            if num_chunks <= 0:
                raise EX.ValidationError(message=f"No valid number of chunks: {num_chunks}")

            pbar      = tqdm(total=num_chunks)
            rts_ms    = []
            bytes_received = 0

            async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:
                semaphore = asyncio.Semaphore(max_parallel_gets)

                async def fetch_chunk_with_retry(i: int):
                    nonlocal bytes_received
                    attempt = 0
                    while attempt < max_retries:
                        try:
                            async with semaphore:
                                t2        = T.monotonic()
                                chunk_key = f"{_ball_id}_{i}"
                                res       = await AsyncClientUtils.get_chunk(
                                    client=client,
                                    router=router,
                                    bucket_id=_bucket_id,
                                    key=chunk_key,
                                    chunk_size=chunk_size,
                                    headers=headers,
                                )
                                elapsed_ms = round((T.monotonic() - t2) * 1000, 2)
                                rts_ms.append(elapsed_ms)
                                if res.is_ok:
                                    elapsed_s  = T.monotonic() - t1
                                    throughput = bytes_received / elapsed_s / 1_048_576 if elapsed_s > 0 else 0
                                    pbar.set_postfix({
                                        "chunk": i,
                                        "resp": f"{elapsed_ms}ms",
                                        "speed": f"{throughput:.2f}MB/s",
                                        "time": datetime.now().strftime("%H:%M:%S"),
                                    })
                                    pbar.update(n=1)
                                    self.__log.debug({
                                        "event": "GET.CHUNK",
                                        "message": "chunk fetched",
                                        "bucket_id": bucket_id,
                                        "ball_id": ball_id,
                                        "key": chunk_key,
                                        "ok": True,
                                        "response_time_ms": elapsed_ms,
                                        "input": _input,
                                        "context": {"chunk_index": i, "num_chunks": num_chunks},
                                    })
                                    return res
                                else:
                                    raise EX.GetChunkError()

                        except Exception as e:
                            attempt += 1
                            current_backoff = delay * (backoff_factor ** (attempt - 1))
                            backoff = min(current_backoff, max_backoff) if max_backoff > 0 else current_backoff
                            await asyncio.sleep(backoff)
                            self.__log.warning({
                                "event": "GET.CHUNK.RETRY",
                                "message": f"chunk fetch failed on attempt {attempt}/{max_retries}",
                                "bucket_id": bucket_id,
                                "ball_id": ball_id,
                                "key": f"{_ball_id}_{i}",
                                "function": "fetch_chunk_with_retry",
                                "error_type": type(e).__name__,
                                "input": _input,
                                "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                            })
                    return Err(EX.GetChunkError(f"Failed to fetch chunk {i} after {max_retries} attempts"))

                futures   = [fetch_chunk_with_retry(i) for i in range(num_chunks)]
                completed = 0

                if not order:
                    for coro in asyncio.as_completed(futures):
                        result = await coro
                        if result.is_ok:
                            completed += 1
                            yield result.unwrap()
                        else:
                            err = result.unwrap_err()
                            self.__log.error({
                                "event": "CHUNK.FAILURE",
                                "message": err.message,
                                "bucket_id": _bucket_id,
                                "ball_id": ball_id,
                                "function": "get_chunks",
                                "error_type": type(err).__name__,
                                "status_code": err.status_code,
                                "input": _input,
                                "context": {},
                            })
                else:
                    all_results = await asyncio.gather(*futures)
                    for i, result in enumerate(all_results):
                        if result.is_ok:
                            completed += 1
                            yield result.unwrap()
                        else:
                            err = result.unwrap_err()
                            self.__log.error({
                                "event": "CHUNK.FAILURE",
                                "message": err.message,
                                "bucket_id": _bucket_id,
                                "ball_id": ball_id,
                                "key": f"{_ball_id}_{i}",
                                "function": "get_chunks",
                                "error_type": type(err).__name__,
                                "status_code": err.status_code,
                                "input": _input,
                                "context": {"chunk_index": i},
                            })
                            raise EX.GetChunkError(f"Failed to fetch chunk {i} in ordered 'get_chunks'")

                pbar.close()

                if completed != num_chunks:
                    raise EX.NotFoundError(f"Some chunks were missing: expected = {num_chunks}, chunks={completed}")

                if len(rts_ms) == 0:
                    raise EX.UnknownError(message=f"{_bucket_id}@{ball_id} not found", status_code=404)

                self.__log.info({
                    "event": "GET",
                    "message": "get completed",
                    "bucket_id": _bucket_id,
                    "ball_id": ball_id,
                    "router": router.router_id,
                    "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                    "input": _input,
                    "context": {"num_chunks": num_chunks, "max_chunk_ms": max(rts_ms)},
                })

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "GET.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "function": "get_chunks",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            raise _e
    
    @_require_auth
    async def get(self,
        bucket_id:str,
        ball_id:str,
        max_paralell_gets:int = 10,
        headers:Dict[str,str]={},
        chunk_size:str="256kb",
        timeout:int = 120,
        http2:bool = False,
        max_retries:int = 5,
        delay:float = 1,
        backoff_factor:float =.5,
        force:bool = False,
        max_backoff:int =5,
        chunk_index:int = 0,
        max_delay:int =30,
        jitter:bool = True,
        filters:Optional[List[IOFilter]] = None,
        cache:Optional[bool] = None,
    )->Result[InterfaceX.AsyncGetResponse, EX.MictlanXError]:
        """Download all chunks of a ball and reassemble them into bytes.

        Fetches the initial chunk's metadata to discover ``num_chunks``, then
        downloads all chunks concurrently, merges them in order, and verifies
        the SHA-256 checksum.

        Args:
            bucket_id: Source bucket identifier.
            ball_id: Ball identifier (chunk keys are ``{ball_id}_{i}``).
            max_paralell_gets: Maximum concurrent chunk downloads. Defaults to
                ``10``.
            headers: Extra HTTP headers.
            chunk_size: Read-buffer size as a humanfriendly string. Defaults
                to ``"256kb"``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            http2: Enable HTTP/2. Defaults to ``False``.
            max_retries: Maximum attempts per chunk. Defaults to ``5``.
            delay: Initial backoff delay in seconds. Defaults to ``1``.
            backoff_factor: Delay multiplier per retry. Defaults to ``0.5``.
            force: Send ``Force-Get`` header to bypass caching. Defaults to
                ``False``.
            max_backoff: Ceiling on backoff sleep in seconds. Defaults to ``5``.
            chunk_index: Starting chunk index for metadata discovery. Defaults
                to ``0``.
            max_delay: Hard ceiling on retry delay. Defaults to ``30``.
            jitter: Randomise retry delays. Defaults to ``True``.
            filters: Ordered list of :class:`~mictlanx.filters.IOFilter`
                transforms applied to the reassembled bytes, after the
                SHA-256 integrity check and after verifying the list matches
                the ``mictlanx_filters`` provenance tag recorded at
                :meth:`put` time. Must be the correctly-ordered inverse of
                the chain given to :meth:`put` (e.g. ``put(filters=[a, b])``
                pairs with ``get(filters=[b_inverse, a_inverse])``). Defaults
                to ``None`` (no filters — identical to today's behavior).
            cache: Serve from / store into the in-memory cache. ``None``
                (default) uses the client's ``cache_default``. A cached copy
                is only served when its checksum matches the fresh metadata
                and it was produced with the same ``filters``; ``force=True``
                always downloads.

        Returns:
            ``Ok(AsyncGetResponse)`` whose ``data`` field is a
            :class:`memoryview` of the reassembled object, or
            ``Err(MictlanXError)`` on failure.
        """
        try:
            t1                    = T.monotonic()
            _bucket_id            = Utils.sanitize_str(bucket_id)
            _ball_id              = Utils.sanitize_str(ball_id)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = str(headers.get("Accept-Encoding", "identity"))
            headers["Force-Get"]  = str(headers.get("Force", int(force)))
            router                = self.rlb.get_router()
            initial_chunk_key     = f"{_ball_id}{'_'+str(chunk_index) if chunk_index >= 0 else ''}"
            retry_policy          = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay, jitter=jitter)

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "chunk_size": chunk_size,
                "timeout": timeout,
                "max_retries": max_retries,
                "delay": delay,
                "backoff_factor": backoff_factor,
                "force": force,
                "max_backoff": max_backoff,
                "chunk_index": chunk_index,
            }

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=initial_chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({
                    "event": "GET.METADATA.FAILED.ATTEMPT",
                    "message": "metadata fetch attempt failed",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": initial_chunk_key,
                    "input": _input,
                    "context": {"attempt": i, "max_attempts": retry_policy.retries},
                }),
                on_error   = lambda i, e: self.__log.error({
                    "event": "GET.METADATA.ERROR",
                    "message": str(e.message),
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": initial_chunk_key,
                    "function": "get",
                    "error_type": type(e).__name__,
                    "status_code": e.status_code,
                    "input": _input,
                    "context": {"attempt": i},
                }),
            )

            rts_ms = []
            if metadata_result.is_ok:
                metadata   = metadata_result.unwrap()
                num_chunks = int(metadata.metadata.tags.get("num_chunks"))
                if num_chunks <= 0:
                    raise EX.ValidationError(message=f"No valid number of chunks: {num_chunks}")

                cache_key       = AccessStats.key(_bucket_id, _ball_id)
                use_cache       = self.cache_default if cache is None else cache
                get_filter_tag  = ",".join(f.name for f in (filters or []))
                if use_cache and not force:
                    cached = self.cache.get(cache_key)
                    if cached.is_some:
                        cached_meta, cached_data = cached.unwrap()
                        fresh_checksum = metadata.metadata.tags.get("full_checksum", "")
                        if fresh_checksum and cached_meta.tags.get("full_checksum") == fresh_checksum and cached_meta.tags.get("mictlanx_get_filters", "") == get_filter_tag:
                            self.stats.record_get(cache_key, hit=True, nbytes=cached_data.nbytes)
                            self.__log.debug({
                                "event": "GET.CACHE.HIT",
                                "message": "served from cache",
                                "bucket_id": _bucket_id,
                                "ball_id": _ball_id,
                                "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                                "input": _input,
                                "context": {"checksum": fresh_checksum, "size": cached_data.nbytes},
                            })
                            return Ok(InterfaceX.AsyncGetResponse(data=cached_data, metadatas=[cached_meta]))
                        self.cache.remove(cache_key)

                pbar = tqdm(total=num_chunks)
                async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:
                    semaphore = asyncio.Semaphore(max_paralell_gets)

                    async def fetch_chunk(i: int):
                        attempt = 0
                        while attempt < max_retries:
                            try:
                                async with semaphore:
                                    t2        = T.monotonic()
                                    chunk_key = f"{_ball_id}_{i}"
                                    res       = await AsyncClientUtils.get_chunk(
                                        client=client,
                                        router=router,
                                        bucket_id=_bucket_id,
                                        key=chunk_key,
                                        chunk_size=chunk_size,
                                        headers=headers,
                                    )
                                    elapsed_ms = round((T.monotonic() - t2) * 1000, 2)
                                    rts_ms.append(elapsed_ms)
                                    if res.is_ok:
                                        elapsed_s  = T.monotonic() - t1
                                        throughput = (i + 1) / elapsed_s / 1_048_576 if elapsed_s > 0 else 0
                                        pbar.set_postfix({
                                            "chunk": i,
                                            "resp": f"{elapsed_ms}ms",
                                            "speed": f"{throughput:.2f}MB/s",
                                            "time": datetime.now().strftime("%H:%M:%S"),
                                        })
                                        pbar.update(n=1)
                                        self.__log.debug({
                                            "event": "GET.CHUNK",
                                            "message": "chunk fetched",
                                            "bucket_id": bucket_id,
                                            "ball_id": ball_id,
                                            "key": chunk_key,
                                            "ok": True,
                                            "response_time_ms": elapsed_ms,
                                            "input": _input,
                                            "context": {"chunk_index": i, "num_chunks": num_chunks},
                                        })
                                        return res
                                    else:
                                        raise EX.GetChunkError()

                            except Exception as e:
                                attempt += 1
                                current_backoff = delay * (backoff_factor ** (attempt - 1))
                                backoff = min(current_backoff, max_backoff) if max_backoff > 0 else current_backoff
                                await asyncio.sleep(backoff)
                                self.__log.warning({
                                    "event": "GET.CHUNK.RETRY",
                                    "message": f"chunk fetch failed on attempt {attempt}/{max_retries}",
                                    "bucket_id": bucket_id,
                                    "ball_id": ball_id,
                                    "key": f"{_ball_id}_{i}",
                                    "function": "fetch_chunk",
                                    "error_type": type(e).__name__,
                                    "input": _input,
                                    "context": {"attempt": attempt, "backoff_ms": round(backoff * 1000, 2)},
                                })
                        return Err(EX.GetChunkError(f"Failed to fetch chunk {i} after {max_retries} attempts"))

                    futures = [fetch_chunk(i) for i in range(num_chunks)]
                    results = await asyncio.gather(*futures)

                responses: List[Tuple[InterfaceX.Metadata, memoryview]] = list(map(lambda x: x.unwrap(), filter(lambda x: x.is_ok, results)))
                if len(responses) == 0:
                    raise EX.NotFoundError("No chunks were found")
                elif len(responses) != num_chunks:
                    raise EX.NotFoundError(f"Some chunks were missing: expected = {num_chunks}, chunks={len(responses)}")

                pbar.close()
                remote_checksum = responses[0][0].tags.get("full_checksum", "")
                x        = await AsyncClientUtils.merge_chunks(chunks=responses)
                checksum = XoloUtils.sha256(x)
                if not remote_checksum == checksum:
                    self.__log.warning({
                        "event": "INTEGRITY.CHECK.FAILED",
                        "message": "checksum mismatch between remote and local",
                        "bucket_id": _bucket_id,
                        "ball_id": ball_id,
                        "function": "get",
                        "error_type": "IntegrityError",
                        "input": _input,
                        "context": {"remote_checksum": remote_checksum, "local_checksum": checksum},
                    })
                    raise EX.IntegrityError(message=f"Integrity check failed: {remote_checksum} != {checksum}")

                if len(rts_ms) == 0:
                    return Err(EX.UnknownError(message=f"{_bucket_id}@{ball_id} not found", status_code=404))

                self.__log.info({
                    "event": "GET",
                    "message": "get completed",
                    "bucket_id": _bucket_id,
                    "ball_id": ball_id,
                    "router": router.router_id,
                    "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                    "input": _input,
                    "context": {"num_chunks": num_chunks, "checksum": checksum, "max_chunk_ms": max(rts_ms)},
                })
                metadatas = list(map(lambda x: x[0], responses))

                _get_filters = filters or []
                stored_filter_tag   = responses[0][0].tags.get("mictlanx_filters", "")
                stored_filter_names = stored_filter_tag.split(",") if stored_filter_tag else []
                if not IOFilter.names_match(stored=stored_filter_names, get_filters=_get_filters):
                    given_reversed = list(reversed([f.name for f in _get_filters]))
                    self.__log.warning({
                        "event": "FILTER.MISMATCH",
                        "message": "get() filters do not match the filters recorded at put() time",
                        "bucket_id": _bucket_id,
                        "ball_id": ball_id,
                        "function": "get",
                        "error_type": "FilterMismatchError",
                        "input": _input,
                        "context": {"stored_filters": stored_filter_names, "given_filters_reversed": given_reversed},
                    })
                    return Err(EX.FilterMismatchError(
                        f"Filter mismatch for {_bucket_id}/{ball_id}: object was written with filters={stored_filter_names!r}, "
                        f"but get() filters(reversed)={given_reversed!r}"
                    ))

                if _get_filters:
                    data = x.tobytes()
                    for f in _get_filters:
                        try:
                            data = f.filter(data)
                        except Exception as e:
                            return Err(EX.FilterExecutionError(f"{f.name} filter raised {type(e).__name__}: {e}"))
                    x = memoryview(data)

                self.stats.record_get(cache_key, hit=False, nbytes=x.nbytes)
                if use_cache:
                    first = responses[0][0]
                    self.cache.put(cache_key, x, first.model_copy(update={"tags": {**first.tags, "mictlanx_get_filters": get_filter_tag}}))
                return Ok(InterfaceX.AsyncGetResponse(data=x, metadatas=metadatas))

            raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "GET.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "function": "get",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(EX.MictlanXError.from_exception(e))

    @_require_auth
    async def get_to_file(self,
        bucket_id: str,
        ball_id: str,
        output_path: str = "",
        fullname: str = "",
        max_paralell_gets: int = 10,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",
        timeout: int = 120,
        http2: bool = False,
        max_retries: int = 5,
        delay: float = 1,
        backoff_factor: float = .5,
        force: bool = False,
        chunk_index:int =0,
        max_delay:int =30,
        jitter:bool = True
    ) -> Result[str, EX.MictlanXError]:
        """Download a ball and write it directly to a file on disk.

        Fetches ``num_chunks`` in parallel and streams them sequentially to
        ``{output_path}/{fullname}`` using an async writer loop, avoiding
        holding the entire object in memory.  If a local file with the same
        checksum already exists it is returned immediately (cache hit).

        Args:
            bucket_id: Source bucket identifier.
            ball_id: Ball identifier to download.
            output_path: Directory where the output file is written.
                Created automatically if it does not exist.
            fullname: Override the output filename.  When empty the filename
                is derived from ball metadata (``fullname`` or ``extension``
                tags).
            max_paralell_gets: Maximum concurrent chunk downloads. Defaults to
                ``10``.
            headers: Extra HTTP headers.
            chunk_size: Read-buffer size as a humanfriendly string. Defaults
                to ``"256kb"``.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            http2: Enable HTTP/2. Defaults to ``False``.
            max_retries: Maximum attempts per chunk. Defaults to ``5``.
            delay: Initial backoff delay in seconds. Defaults to ``1``.
            backoff_factor: Delay multiplier per retry. Defaults to ``0.5``.
            force: Send ``Force-Get`` header to bypass caching. Defaults to
                ``False``.
            chunk_index: Starting chunk index for metadata discovery. Defaults
                to ``0``.
            max_delay: Hard ceiling on retry delay. Defaults to ``30``.
            jitter: Randomise retry delays. Defaults to ``True``.

        Returns:
            ``Ok(path)`` with the absolute path of the written file, or
            ``Err(MictlanXError)`` on failure.
        """
        try:
            os.makedirs(output_path, exist_ok=True)
            t1         = T.monotonic()
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            headers["Chunk-Size"] = chunk_size
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            headers["Force-Get"] = str(headers.get("Force", str(int(force))))
            router = self.rlb.get_router()

            initial_chunk_key = f"{_ball_id}{'_'+str(chunk_index) if chunk_index >= 0 else ''}"
            retry_policy      = RetryPolicy(retries=max_retries, initial_delay=delay, backoff_factor=backoff_factor, max_delay=max_delay, jitter=jitter)

            _input = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "output_path": output_path,
                "fullname": fullname,
                "chunk_size": chunk_size,
                "timeout": timeout,
                "max_retries": max_retries,
                "delay": delay,
                "backoff_factor": backoff_factor,
                "force": force,
                "chunk_index": chunk_index,
            }

            metadata_result = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=initial_chunk_key),
                policy     = retry_policy,
                on_attempt = lambda i: self.__log.debug({
                    "event": "GET.METADATA.FAILED.ATTEMPT",
                    "message": "metadata fetch attempt failed",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": initial_chunk_key,
                    "input": _input,
                    "context": {"attempt": i, "max_attempts": retry_policy.retries},
                }),
                on_error   = lambda i, e: self.__log.error({
                    "event": "GET.METADATA.ERROR",
                    "message": str(e.message),
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": initial_chunk_key,
                    "function": "get_to_file",
                    "error_type": type(e).__name__,
                    "status_code": e.status_code,
                    "input": _input,
                    "context": {"attempt": i},
                }),
            )

            if not metadata_result.is_ok:
                raise EX.MictlanXError.from_exception(metadata_result.unwrap_err())

            metadata      = metadata_result.unwrap()
            tmp_ext       = metadata.metadata.tags.get("extension", "")
            _fullname     = fullname if fullname else f"{_ball_id}{'.'+tmp_ext if tmp_ext else ''}"
            tmp_fullname  = metadata.metadata.tags.get("fullname", _fullname)
            full_checksum = metadata.metadata.tags.get("full_checksum", "")
            _path         = f"{output_path}/{tmp_fullname}"

            if os.path.exists(_path):
                (local_checksum, _) = XoloUtils.sha256_file(path=_path)
                if local_checksum == full_checksum:
                    self.__log.info({
                        "event": "GET",
                        "message": "cache hit — file already exists",
                        "bucket_id": _bucket_id,
                        "ball_id": ball_id,
                        "key": _ball_id,
                        "router": router.router_id,
                        "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                        "input": _input,
                        "context": {"path": _path, "hit": True},
                    })
                    return Ok(_path)
                else:
                    raise EX.FileAlreadyExists(message=f"File already exists: {_path}")

            num_chunks    = int(metadata.metadata.tags.get("num_chunks"))
            if num_chunks <= 0:
                raise EX.ValidationError(message=f"Invalid number of chunks: {num_chunks}")

            pbar           = tqdm(total=num_chunks)
            received_chunks = {}
            received_lock  = asyncio.Lock()
            next_to_write  = 0
            semaphore      = asyncio.Semaphore(max_paralell_gets)

            async with httpx.AsyncClient(http2=http2, trust_env=False, timeout=timeout, verify=self.verify, headers=headers) as client:

                async def fetch_chunk(i):
                    async with semaphore:
                        t2  = T.monotonic()
                        res = await AsyncClientUtils.get_chunk(
                            client=client, router=router,
                            bucket_id=_bucket_id, key=f"{_ball_id}_{i}",
                            chunk_size=chunk_size, headers=headers,
                        )
                        elapsed_ms = round((T.monotonic() - t2) * 1000, 2)
                        if res.is_ok:
                            chunk_i_metadata, chunk_i_data = res.unwrap()
                            index = int(chunk_i_metadata.tags.get("index", i))
                            async with received_lock:
                                received_chunks[index] = chunk_i_data.tobytes()
                            pbar.set_postfix({
                                "chunk": index,
                                "resp": f"{elapsed_ms}ms",
                                "time": datetime.now().strftime("%H:%M:%S"),
                            })
                        else:
                            raise EX.GetChunkError()

                async def writer_loop(f):
                    nonlocal next_to_write
                    while next_to_write < num_chunks:
                        async with received_lock:
                            while next_to_write in received_chunks:
                                f.write(received_chunks[next_to_write])
                                del received_chunks[next_to_write]
                                next_to_write += 1
                                pbar.update(1)
                        await asyncio.sleep(0.0001)

                with open(_path, "wb") as f:
                    fetchers = [fetch_chunk(i) for i in range(num_chunks)]
                    await asyncio.gather(writer_loop(f), *fetchers)

            pbar.close()
            self.__log.info({
                "event": "GET",
                "message": "get to file completed",
                "bucket_id": _bucket_id,
                "ball_id": ball_id,
                "key": _ball_id,
                "router": router.router_id,
                "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                "input": _input,
                "context": {"path": _path, "hit": False, "num_chunks": num_chunks},
            })
            return Ok(_path)

        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "GET.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": ball_id,
                "function": "get_to_file",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(_e)
        
    # --- METADATA METHODS ---
    @_require_auth
    async def get_metadata_by_key(self,bucket_id:str, key:str,timeout: int = 120,headers: Dict[str, str] = {}, retry_policy: RetryPolicy = None)->Result[InterfaceX.GetMetadataResponse,EX.MictlanXError]:
        """Fetch the metadata record for a single chunk key.

        Args:
            bucket_id: Bucket that contains the chunk.
            key: Exact chunk key (e.g. ``"ball_0"``).
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            headers: Extra HTTP headers.
            retry_policy: Override the default :class:`RetryPolicy`.  When
                ``None`` the client's ``default_retry_policy`` is used.

        Returns:
            ``Ok(GetMetadataResponse)`` or ``Err(MictlanXError)``.
        """
        try:
            _bucket_id = Utils.sanitize_str(bucket_id)
            _key       = Utils.sanitize_str(key)
            router     = self.rlb.get_router()
            _input = {"bucket_id": bucket_id, "ball_id": key, "key": key, "timeout": timeout}
            x = await raf(
                func       = lambda: router.get_metadata(bucket_id=_bucket_id, key=_key, timeout=timeout, headers=headers),
                policy     = self.default_retry_policy if retry_policy is None else retry_policy,
                on_attempt = lambda i: self.__log.debug({
                    "event": "GET.METADATA.BY.KEY.FAILED.ATTEMPT",
                    "message": "metadata by key fetch attempt failed",
                    "bucket_id": bucket_id,
                    "ball_id": key,
                    "key": key,
                    "input": _input,
                    "context": {"attempt": i, "max_attempts": 3},
                }),
            )
            return x
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "GET.METADATA.BY.KEY.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": key,
                "key": key,
                "function": "get_metadata_by_key",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(_e)
    @_require_auth
    async def get_metadata(self,bucket_id:str,ball_id:str,timeout: int = 120,headers: Dict[str, str] = {},restart_policy: RetryPolicy = None)->Result[InterfaceX.Ball,EX.MictlanXError]:
        """Fetch and assemble the full :class:`Ball` metadata for a ball.

        Retrieves all chunk metadata for ``ball_id`` from the router and
        builds a :class:`Ball` object (via :meth:`Ball.build`).

        Args:
            bucket_id: Bucket containing the ball.
            ball_id: Ball identifier whose chunk metadata is fetched.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            headers: Extra HTTP headers.
            restart_policy: Override the default :class:`RetryPolicy`.  When
                ``None`` the client's ``default_retry_policy`` is used.

        Returns:
            ``Ok(Ball)`` with all chunk metadata populated, or
            ``Err(MictlanXError)`` on failure.
        """
        try:
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            router     = self.rlb.get_router()
            _input = {"bucket_id": bucket_id, "ball_id": ball_id, "key": ball_id, "timeout": timeout}
            x = await raf(
                func       = lambda: router.get_chunks_metadata(bucket_id=_bucket_id, ball_id=_ball_id, timeout=timeout, headers=headers),
                policy     = self.default_retry_policy if restart_policy is None else restart_policy,
                on_attempt = lambda i: self.__log.debug({
                    "event": "GET.METADATA.BY.BALL.ID.FAILED.ATTEMPT",
                    "message": "metadata by ball_id fetch attempt failed",
                    "bucket_id": bucket_id,
                    "ball_id": ball_id,
                    "key": ball_id,
                    "input": _input,
                    "context": {"attempt": i, "max_attempts": self.default_retry_policy.retries},
                }),
            )
            if x.is_err:
                raise x.unwrap_err()
            bm = x.unwrap()
            b  = InterfaceX.Ball(bucket_id=bucket_id, chunks=bm.chunks, checksum=bm.checksum, ball_id=ball_id)
            b.build()
            return Ok(b)
        except Exception as e:
            # Keep already-typed errors (e.g. NotFoundError) instead of collapsing them into UnknownError.
            _e = e if isinstance(e, EX.MictlanXError) else EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "GET.METADATA.BY.BALL.ID.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": ball_id,
                "function": "get_metadata",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(_e)      
    @_require_auth
    async def get_bucket_metadata(
        self,
        bucket_id:str,
        timeout: int = 120,
        headers: Dict[str, str] = {}
    )-> Result[InterfaceX.Bucket,EX.MictlanXError]:
        """Fetch all ball metadata for a bucket and return a :class:`Bucket`.

        Calls :meth:`get_chunks_by_bucket_id`, then groups the flat chunk
        list into :class:`Ball` objects in parallel via
        :meth:`AsyncClientUtils.group_chunks`.

        Args:
            bucket_id: Bucket whose metadata is fetched.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            headers: Extra HTTP headers.

        Returns:
            ``Ok(Bucket)`` with all balls built, or ``Err(MictlanXError)``.
        """
        try:
            res = await self.get_chunks_by_bucket_id(bucket_id=bucket_id)
            if res.is_err:
                raise res.unwrap_err()
            response = res.unwrap()
            # response.balls
            balls  =  await AsyncClientUtils.group_chunks(balls_list=response.balls,num_threads=4)
            bucket = InterfaceX.Bucket(bucket_id= bucket_id, balls= balls)
            return Ok(bucket)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)
    @_require_auth
    async def get_chunks_by_bucket_id(
            self,
            bucket_id: str,
            timeout: int = 120,
            headers: Dict[str, str] = {}
        ) -> Result[InterfaceX.GetRouterBucketMetadataResponse, EX.MictlanXError]:
            """Fetch raw bucket metadata (flat chunk list) from the router.

            Args:
                bucket_id: Bucket identifier to query.
                timeout: Per-request timeout in seconds. Defaults to ``120``.
                headers: Extra HTTP headers.

            Returns:
                ``Ok(GetRouterBucketMetadataResponse)`` on success or
                ``Err(MictlanXError)`` on failure.
            """
            try:
                router     = self.rlb.get_router()
                t1         = T.monotonic()
                _input     = {"bucket_id": bucket_id, "ball_id": bucket_id, "key": bucket_id, "timeout": timeout}
                x          = await router.get_bucket_metadata(bucket_id=bucket_id, timeout=timeout, headers=headers)
                elapsed_ms = round((T.monotonic() - t1) * 1000, 2)
                if x.is_ok:
                    self.__log.info({
                        "event": "GET.BUCKET.METADATA",
                        "message": "bucket metadata fetched",
                        "bucket_id": bucket_id,
                        "ball_id": bucket_id,
                        "key": bucket_id,
                        "router": router.router_id,
                        "response_time_ms": elapsed_ms,
                        "input": _input,
                        "context": {},
                    })
                    return x
                else:
                    err = x.unwrap_err()
                    self.__log.error({
                        "event": "GET.BUCKET.METADATA.ERROR",
                        "message": str(err),
                        "bucket_id": bucket_id,
                        "ball_id": bucket_id,
                        "key": bucket_id,
                        "function": "get_chunks_by_bucket_id",
                        "error_type": type(err).__name__,
                        "input": _input,
                        "context": {"response_time_ms": elapsed_ms},
                    })
                    return x

            except Exception as e:
                _e = EX.MictlanXError.from_exception(e)
                self.__log.error({
                    "event": "GET.BUCKET.METADATA.ERROR",
                    "message": _e.message,
                    "bucket_id": bucket_id,
                    "ball_id": bucket_id,
                    "key": bucket_id,
                    "function": "get_chunks_by_bucket_id",
                    "error_type": type(_e).__name__,
                    "status_code": _e.status_code,
                    "input": _input,
                    "context": {},
                })
                return Err(_e)


    # --- DELETE METHODS ---
    @_require_auth
    async def delete(self,
        ball_id:str,
        bucket_id:str,
        timeout: int = 120,
        force:bool = True,
        headers: Dict[str, str] = {}
    )->Result[InterfaceX.DeletedByBallIdResponse, EX.MictlanXError]:
        """Delete all chunks of a ball from a bucket.

        Fetches the ball's chunk metadata, then deletes every chunk key
        concurrently via :meth:`delete_by_key`.

        Args:
            ball_id: Ball whose chunks are deleted.
            bucket_id: Bucket containing the ball.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            force: Send a ``Force`` header to force deletion even when the
                chunk is pinned. Defaults to ``True``.
            headers: Extra HTTP headers.

        Returns:
            ``Ok(DeletedByBallIdResponse)`` with the total number of deleted
            records, or ``Err(MictlanXError)`` if any chunk deletion fails.
        """
        try:
            _bucket_id = Utils.sanitize_str(bucket_id)
            _ball_id   = Utils.sanitize_str(ball_id)
            headers["Accept-Encoding"] = headers.get("Accept-Encoding", "identity")
            router     = self.rlb.get_router()
            _input     = {"bucket_id": bucket_id, "ball_id": ball_id, "key": ball_id, "timeout": timeout, "force": force}
            self.cache.remove(AccessStats.key(_bucket_id, _ball_id))

            ball_metadata_result = await router.get_chunks_metadata(bucket_id=_bucket_id, ball_id=_ball_id, timeout=timeout, headers=headers)
            if ball_metadata_result.is_err:
                return Err(ball_metadata_result.unwrap_err())

            ball_metadata = ball_metadata_result.unwrap()
            coros = [
                self.delete_by_key(bucket_id=bucket_id, key=c.key, timeout=timeout, force=force, headers=headers)
                for c in ball_metadata.chunks
            ]
            results: List[Result[InterfaceX.DeletedByKeyResponse]] = await asyncio.gather(*coros)
            _results      = list(map(lambda x: x.unwrap(), filter(lambda x: x.is_ok, results)))
            n_err_results = len(results) - len(_results)

            if n_err_results > 0:
                return Err(EX.UnknownError("Failed to delete a chunk, please try again."))

            res = InterfaceX.DeletedByBallIdResponse(n_deletes=0, ball_id=_ball_id)
            for r in _results:
                res.n_deletes += r.n_deletes
            return Ok(res)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "event": "DELETE.ERROR",
                "message": _e.message,
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": ball_id,
                "function": "delete",
                "error_type": type(_e).__name__,
                "status_code": _e.status_code,
                "input": _input,
                "context": {},
            })
            return Err(_e)
    @_require_auth
    async def delete_by_key(self,
                     key: str,
                     bucket_id: str,
                     timeout: int = 120,
                     force:bool = True,
                     headers: Dict[str, str] = {}) -> Result[InterfaceX.DeletedByKeyResponse, EX.MictlanXError]:
        """Delete a single chunk key from a bucket across all routers.

        Sends a delete request to every router in the pool so that all
        replicas are removed.

        Args:
            key: Chunk key to delete.
            bucket_id: Bucket containing the key.
            timeout: Per-request timeout in seconds. Defaults to ``120``.
            force: Send a ``Force`` header to force deletion. Defaults to
                ``True``.
            headers: Extra HTTP headers.

        Returns:
            ``Ok(DeletedByKeyResponse)`` with the aggregate delete count
            across all routers, or ``Err(MictlanXError)`` on error.
        """
        _key = Utils.sanitize_str(x=key)
        _bucket_id = Utils.sanitize_str(x=bucket_id)
        try:
            failed  = []
            del_res = InterfaceX.DeletedByKeyResponse(n_deletes=0, key=key)
            headers["Force"] = str(int(force))
            _input = {"bucket_id": bucket_id, "ball_id": key, "key": key, "timeout": timeout, "force": force}
            for router in self.__routers:
                t1         = T.monotonic()
                del_result = await router.delete(bucket_id=_bucket_id, key=_key, headers=headers)
                elapsed_ms = round((T.monotonic() - t1) * 1000, 2)
                if del_result.is_err:
                    err = del_result.unwrap_err()
                    self.__log.error({
                        "event": "DELETE.ERROR",
                        "message": str(err),
                        "bucket_id": _bucket_id,
                        "ball_id": key,
                        "key": key,
                        "function": "delete_by_key",
                        "error_type": type(err).__name__,
                        "input": _input,
                        "context": {"router": router.router_id, "response_time_ms": elapsed_ms},
                    })
                    failed.append(router)
                else:
                    x_response = del_result.unwrap()
                    del_res.n_deletes += x_response.n_deletes
                    self.__log.info({
                        "event": "DELETE",
                        "message": "key deleted",
                        "bucket_id": _bucket_id,
                        "ball_id": key,
                        "key": _key,
                        "router": router.router_id,
                        "response_time_ms": elapsed_ms,
                        "input": _input,
                        "context": {"n_deletes": del_res.n_deletes},
                    })
            return Ok(del_res)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)    
    
    @_require_auth
    async def delete_bucket(self, bucket_id: str, headers: Dict[str, str] = {}, timeout: int = 120,force:bool = True) -> Result[InterfaceX.DeleteBucketResponse, Exception]:
        """
        Asynchronously deletes the specified bucket by fetching metadata from each router,
        then concurrently deleting each object (ball) found.

        Args:
            bucket_id (str): The ID of the bucket to delete.
            headers (Dict[str, str]): Optional HTTP headers.
            timeout (int): Request timeout in seconds.

        Returns:
            Result[InterfaceX.DeleteBucketResponse, Exception]: An Ok-wrapped response on success,
            or an Err with an Exception on failure.
        """
        try: 
            prefix = AccessStats.key(Utils.sanitize_str(bucket_id), "")
            for k in [k for k in self.cache.get_keys() if k.startswith(prefix)]:
                self.cache.remove(k)
            start_time = T.time()
            deleted = 0
            failed = 0
            total = 0
            keys = []  # Collect keys if needed

            deletion_tasks = []

            # For each router, schedule an asynchronous get_bucket_metadata call.
            metadata_tasks = [
                router.get_bucket_metadata(bucket_id=bucket_id, headers=headers, timeout=timeout)
                for router in self.__routers
            ]
            
            # Wait for all metadata calls to complete concurrently.
            metadata_results = await asyncio.gather(*metadata_tasks, return_exceptions=True)
            
            # Process each router's metadata result.
            for router, meta_result in zip(self.__routers, metadata_results):
                if meta_result.is_err:
                    self.__log.error({
                        "msg": str(meta_result.unwrap_err()),
                        "bucket_id": bucket_id,
                        "router_id": router.router_id
                    })
                    continue
                if meta_result.is_ok:
                    metadata = meta_result.unwrap()
                    total += len(metadata.balls)
                    # Schedule delete tasks for each ball (object) in the metadata.
                    for ball in metadata.balls:
                        deletion_tasks.append(
                            self.delete_by_key(key=ball.key, bucket_id=bucket_id, headers=headers, timeout=timeout,force=force)
                        )
                else:
                    self.__log.error({
                        "msg": str(meta_result.unwrap_err()),
                        "bucket_id": bucket_id,
                        "router_id": router.router_id
                    })
            
            # Execute all deletion tasks concurrently.
            deletion_results = await asyncio.gather(*deletion_tasks, return_exceptions=True)
            
            for result in deletion_results:
                if isinstance(result, Exception):
                    failed += 1
                else:
                    if result.is_ok:
                        deleted += 1
                    else:
                        failed += 1

            rt = T.time() - start_time
            self.__log.info({
                "event": "DELETE.BUCKET",
                "bucket_id": bucket_id,
                "deleted": deleted,
                "failed": failed,
                "total": total,
                "response_time": rt
            })
            
            return Ok(InterfaceX.DeleteBucketResponse(
                bucket_id=bucket_id,
                deleted=deleted,
                failed=failed,
                total=total,
                keys=keys,
                response_time=rt
            ))
        except Exception as e: 
            _e = EX.MictlanXError.from_exception(e)
            self.__log.error({
                "name":_e.get_name(),
                "message":_e.message,
                "status":_e.status_code, 
            })
            return Err(_e)
        
