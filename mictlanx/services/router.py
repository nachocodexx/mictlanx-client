
from typing import Dict,Any,List,AsyncGenerator
import os 
import mictlanx.errors as EX
# 
from xolo.utils import Utils as XoloUtils
import httpx
from option import Result,Ok,Err
# 
from mictlanx.services import AsyncPeer
import mictlanx.interfaces.responses as ResponseModels
import humanfriendly as HF
from mictlanx.types import VerifyType


class AsyncRouter:
    """Async HTTP client for a single MictlanX router node.

    A router manages peer selection, load balancing, and replication for
    a VSS.  All methods return ``Result[T, Exception]``; callers should
    check ``.is_ok`` / ``.is_err`` and call ``.unwrap()`` / ``.unwrap_err()``.
    """

    def __init__(self, router_id: str, ip_addr: str, port: int, protocol: str = "http",http2:bool=False,api_version:int=4):
        """Initialise a router client.

        Args:
            router_id: Unique identifier of the router.
            ip_addr: Hostname or IP address of the router.
            port: TCP port the router listens on.  Use ``-1`` or ``0`` to
                omit the port from the base URL.
            protocol: ``"http"`` or ``"https"``. Defaults to ``"http"``.
            http2: When ``True`` HTTP/2 is used for chunk uploads.
                Defaults to ``False``.
            api_version: API version to include in URL paths. Defaults to ``4``.
        """
        self.router_id   = router_id
        self.ip_addr     = ip_addr
        self.port        = port
        self.protocol    = protocol
        self.http2       = http2
        self.api_version = api_version

    def get_addr(self) -> str:
        """Return the ``host:port`` address string.

        Returns:
            String in the form ``"<ip_addr>:<port>"``.
        """
        return f"{self.ip_addr}:{self.port}"

    def base_url(self) -> str:
        """Return the base URL for API requests to this router.

        Omits the port for HTTPS or when ``port`` is ``-1``/``0``.

        Returns:
            URL string such as ``"http://192.168.1.1:60666"``.
        """
        if self.port in (-1, 0):
            return f"{self.protocol}://{self.ip_addr}"
        elif self.protocol == "https":
            return f"{self.protocol}://{self.ip_addr}"
        return f"{self.protocol}://{self.ip_addr}:{self.port}"

    def __eq__(self, other: "AsyncRouter") -> bool:
        if not isinstance(other, AsyncRouter):
            return False
        return (self.ip_addr == other.ip_addr and self.port == other.port) or self.router_id == other.peer_id

    def __str__(self):
        return f"Router(id = {self.router_id}, ip_addr={self.ip_addr}, port={self.port})"

    @staticmethod
    def from_str(x: str) -> Result["AsyncRouter", Exception]:
        """Parse a colon-separated string into an ``AsyncRouter``.

        Expected format: ``"<router_id>:<ip_addr>:<port>"``.

        Args:
            x: Colon-separated router string.

        Returns:
            ``Ok(AsyncRouter)`` on success, ``Err(Exception)`` if the string
            has fewer than three colon-separated parts.
        """
        parts = x.split(":")
        if len(parts) < 3:
            return Err(Exception(f"Invalid string: {x} - expected format <router_id>:<ip_addr>:<port>"))
        router_id, ip_addr, port = parts[0], parts[1], parts[2]
        protocol = "https" if int(port) <= 0 else "http"
        return Ok(AsyncRouter(peer_id=router_id, ip_addr=ip_addr, port=int(port), protocol=protocol))

    @staticmethod
    def from_router(x:"AsyncRouter",http2:bool=False)->'AsyncRouter':
        """Clone an ``AsyncRouter``, optionally overriding the http2 flag.

        Args:
            x: Source router to copy.
            http2: When ``True`` enable HTTP/2. Defaults to ``False``.

        Returns:
            A new ``AsyncRouter`` with the same coordinates as ``x``.
        """
        return AsyncRouter(router_id=x.router_id, ip_addr=x.ip_addr, port=x.port, protocol=x.protocol,http2=http2 )

    async def add_peers(self, peers: List['AsyncPeer'], headers: Dict[str, str] = {}, timeout: int = 120) -> Result[bool, Exception]:
        """Register a list of peer nodes with this router.

        Args:
            peers: Peers to register.
            headers: Additional HTTP headers.
            timeout: Request timeout in seconds. Defaults to ``120``.

        Returns:
            ``Ok(True)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v4/xpeers"
            xs = [{
                "protocol": p.protocol,
                "hostname": p.ip_addr,
                "port": p.port,
                "peer_id": p.peer_id
            } for p in peers]
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(url, headers=headers, json=xs)
                response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    async def update_metadata(self, bucket_id: str, key: str, metadata: Any, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[bool, Exception]:
        """Update the metadata record for an existing chunk via the router.

        Args:
            bucket_id: Bucket containing the chunk.
            key: Chunk key whose metadata should be updated.
            metadata: Object whose ``__dict__`` is sent as the JSON body.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.
            timeout: Request timeout in seconds. Defaults to ``120``.

        Returns:
            ``Ok(True)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v4/u/buckets/{bucket_id}/{key}"
            data_json = metadata.__dict__
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, headers=headers, json=data_json)
                response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    

    async def get_streaming(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[httpx.Response, Exception]:
        """Fetch a chunk via the router and return the raw response for streaming.

        Args:
            bucket_id: Bucket containing the chunk.
            key: Chunk key to download.
            timeout: Request timeout in seconds. Defaults to ``300``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(httpx.Response)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                # We return the response. The caller is responsible for streaming the data.
            return Ok(response)
        except Exception as e:
            return Err(e)
    async def get_to_file(self,
                          bucket_id: str,
                          key: str,
                          chunk_size: str = "1MB",
                          sink_folder_path: str = "/mictlanx/data",
                          timeout: int = 300,
                          filename: str = "",
                          headers: Dict[str, str] = {},
                          extension: str = "",
                          verify:VerifyType = False
                          ) -> Result[str, Exception]:
        """Download a chunk via the router and write it directly to disk.

        Args:
            bucket_id: Bucket containing the chunk.
            key: Chunk key to download.
            chunk_size: Streaming read buffer size. Defaults to ``"1MB"``.
            sink_folder_path: Destination directory. Created if absent.
                Defaults to ``"/mictlanx/data"``.
            timeout: Request timeout in seconds. Defaults to ``300``.
            filename: Override destination filename; defaults to the SHA-256
                hash of ``bucket_id@key``.
            headers: Additional HTTP headers.
            extension: File extension appended to the filename. Defaults to
                ``""``.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(full_path)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            # from humanfriendly import parse_size
            _chunk_size = HF.parse_size(chunk_size)
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            combined_key = filename if filename else XoloUtils.sha256(f"{bucket_id}@{key}".encode())
            fullpath = f"{sink_folder_path}/{combined_key}{extension}"

            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                async with client.stream("GET", f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}", headers=headers) as response:
                    response.raise_for_status()
                    with open(fullpath, "wb") as f:
                        async for chunk in response.aiter_bytes(_chunk_size):
                            f.write(chunk)
            return Ok(fullpath)
        except Exception as e:
            return Err(e)    
    async def put_chunked(self, task_id: str, chunks: AsyncGenerator[bytes, Any], timeout: int = 120, headers: Dict[str, str] = {}, verify:VerifyType = False) -> Result[ResponseModels.RouterPutChunkedResponse, Exception]:
        """Upload a chunk to the router using chunked transfer encoding.

        The router distributes the chunk to the selected peer(s).

        Args:
            task_id: Task identifier returned by :meth:`put_metadata`.
            chunks: Async generator that yields the chunk's raw bytes.
            timeout: Request timeout in seconds. Defaults to ``120``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(RouterPutChunkedResponse)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/data/{task_id}/chunked"
            async with httpx.AsyncClient(http2=self.http2,timeout=timeout,verify=verify,limits=httpx.Limits(max_connections=None, max_keepalive_connections=None)) as client:
                # req = client.build_request(method="POST", url=url, headers=headers,timeout=timeout, )
                # put_response = await client.post(url, data=chunks, headers=headers)
                put_response = await client.post(url, content=chunks, headers=headers)
                put_response.raise_for_status()

                # data = ResponseModels.PeerPutChunkedResponse.model_validate(put_response.json())
                data = ResponseModels.RouterPutChunkedResponse.model_validate(put_response.json())
                return Ok(data)
        except Exception as e:
            return Err(e)   
    async def delete_by_ball_id(self, ball_id: str, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[ResponseModels.DeletedByBallIdResponse, Exception]:
        """Delete all chunks belonging to a ball via the router.

        Args:
            ball_id: Ball identifier whose chunks should be deleted.
            bucket_id: Bucket containing the ball.
            timeout: Request timeout in seconds. Defaults to ``120``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(DeletedByBallIdResponse)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/bid/{ball_id}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                content_data = response.json()
                return Ok(ResponseModels.DeletedByBallIdResponse.model_validate(content_data))
        except Exception as e:
            return Err(e)
    async def get_chunks_metadata(self, ball_id: str, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[ResponseModels.BallMetadata, Exception]:
        """Retrieve aggregated chunk metadata for a ball via the router.

        Args:
            ball_id: Ball identifier to query.
            bucket_id: Bucket containing the ball.
            timeout: Request timeout in seconds. Defaults to ``120``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(BallMetadata)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata/{ball_id}/chunks"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                # Convert each JSON object into an instance of ResponseModels.Metadata
                data = ResponseModels.BallMetadata.model_validate(response.json())
                # chunks_metadata = map(lambda x: ResponseModels.Metadata(**x), data)
                return Ok(data)
        except Exception as e:
            return Err(e)
    async def delete(self, bucket_id: str, key: str, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[ResponseModels.DeletedByKeyResponse, Exception]:
        """Delete a single chunk by key via the router.

        Args:
            bucket_id: Bucket containing the chunk.
            key: Chunk key to delete.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.
            timeout: Request timeout in seconds. Defaults to ``120``.

        Returns:
            ``Ok(DeletedByKeyResponse)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}"
            async with httpx.AsyncClient(http2=True,verify=verify,timeout=timeout) as client:
                response = await client.delete(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                return Ok(ResponseModels.DeletedByKeyResponse(**json_data) )
        except Exception as e:
            return Err(e)
    async def disable(self, bucket_id: str, key: str, headers: Dict[str, str] = {},verify:VerifyType = False,timeout:int=120) -> Result[bool, Exception]:
        """Mark a chunk as disabled via the router (soft delete).

        Args:
            bucket_id: Bucket containing the chunk.
            key: Chunk key to disable.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.
            timeout: Request timeout in seconds. Defaults to ``120``.

        Returns:
            ``Ok(True)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/{key}/disable"
            async with httpx.AsyncClient(verify=verify,timeout=timeout) as client:
                response = await client.post(url, headers=headers)
                response.raise_for_status()
                return Ok(True)
        except Exception as e:
            return Err(e)
    async def put_metadata(self,bucket_id:str,ball_id: str, key: str, size: int, checksum: str, producer_id: str, content_type: str,
                            tags: Dict[str, str] = {}, timeout: int = 120,
                           is_disabled: bool = False, replication_factor: int = 1,
                           headers: Dict[str, str] = {},verify:VerifyType =False) -> Result[ResponseModels.PutMetadataResponse, EX.MictlanXError]:
        """Register chunk metadata with the router (first step of the put flow).

        The router selects peers and returns ``task_id`` values that must be
        passed to :meth:`put_data` or :meth:`put_chunked`.

        Args:
            key: Unique chunk key.
            size: Byte size of the chunk data.
            checksum: SHA-256 checksum of the chunk data.
            producer_id: Identifier of the uploader.
            content_type: MIME type of the data.
            ball_id: Parent ball identifier.
            bucket_id: Destination bucket.
            tags: User-defined key/value metadata. Defaults to ``{}``.
            timeout: Request timeout in seconds. Defaults to ``120``.
            is_disabled: When ``True`` the chunk is registered but marked
                disabled. Defaults to ``False``.
            replication_factor: Number of replicas to create. Defaults to ``1``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(PutMetadataResponse)`` on success,
            ``Err(MaxAvailabilityReachedError)`` when no peers are available,
            or ``Err(MictlanXError)`` on other failures.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata"
            data_json = {
                "bucket_id": bucket_id,
                "key": key,
                "ball_id": ball_id,
                "checksum": checksum,
                "size": size,
                "tags": tags,
                "producer_id": producer_id,
                "content_type": content_type,
                "is_disabled": is_disabled,
                "replication_factor": replication_factor
            }
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, json=data_json, headers=headers)
                response.raise_for_status()
                res_json = response.json()
                return Ok(ResponseModels.PutMetadataResponse.model_validate(res_json) )
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                try:
                    error_json    = e.response.json()
                    detail        = error_json.get("detail", {})
                    error_message = detail.get("msg", f"No available peers for {bucket_id}/{key}.") if isinstance(detail, dict) else str(detail)
                    _e = EX.MaxAvailabilityReachedError(message=error_message,status_code=409)
                    return Err(_e)
                except Exception as e:
                    _e = EX.MictlanXError.from_exception(e=e)
                    return Err(_e)
            _e = EX.MictlanXError.from_exception(e=e)
            return Err(_e)
        except httpx.RequestError as e:
            _e = EX.MictlanXError.from_exception(e=e)
            return Err(_e)
        except Exception as e:
            _e = EX.MictlanXError.from_exception(e=e)
            return Err(_e)
    async def put_data(self, task_id: str, ball_id: str, value: bytes, content_type: str, timeout: int = 120,
                       headers: Dict[str, str] = {}, file_id: str = "data",verify:VerifyType = False) -> Result[Any, Exception]:
        """Upload raw chunk data to the router (second step of the put flow).

        Args:
            task_id: Task identifier from :meth:`put_metadata`.
            ball_id: Ball identifier.
            value: Raw bytes to upload.
            content_type: MIME type of the data.
            timeout: Request timeout in seconds. Defaults to ``120``.
            headers: Additional HTTP headers.
            file_id: Multipart form field name. Defaults to ``"data"``.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(())`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/data/{task_id}"
            # For file uploads, using httpx's 'files' parameter:
            files = {file_id: (ball_id, value, content_type)}
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.post(url, files=files, headers=headers)
                response.raise_for_status()
            return Ok(())
        except Exception as e:
            return Err(e)
    async def get_bucket_metadata(self, bucket_id: str, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[ResponseModels.GetRouterBucketMetadataResponse, Exception]:
        """Retrieve aggregated metadata for all balls in a bucket via the router.

        Args:
            bucket_id: Bucket to query.
            timeout: Request timeout in seconds. Defaults to ``120``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(GetRouterBucketMetadataResponse)`` on success,
            ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v4/buckets/{bucket_id}/metadata"
            async with httpx.AsyncClient(timeout=timeout,verify =verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return Ok(ResponseModels.GetRouterBucketMetadataResponse.model_validate(response.json()))
        except Exception as e:
            return Err(e)
    async def get_stats(self, timeout: int = 120, headers: Dict[str, str] = {},verify:VerifyType = False) -> Result[Dict[str, ResponseModels.PeerStatsResponse], Exception]:
        """Retrieve per-peer statistics from all peers managed by this router.

        Args:
            timeout: Request timeout in seconds. Defaults to ``120``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(Dict[peer_id, PeerStatsResponse])`` on success,
            ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v4/peers/stats"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                json_data = response.json()
                validated = dict(list(map(lambda x:(x[0],ResponseModels.PeerStatsResponse(**x[1])) , json_data.items())))
                return Ok(validated)
                # return Ok(ResponseModels.(**response.json()))
        except Exception as e:
            return Err(e)
    async def get_metadata(self, bucket_id: str, key: str, timeout: int = 300, headers: Dict[str, str] = {},verify:VerifyType=False) -> Result[ResponseModels.GetMetadataResponse, Exception]:
        """Retrieve metadata for a specific chunk via the router.

        Args:
            bucket_id: Bucket containing the chunk.
            key: Chunk key to query.
            timeout: Request timeout in seconds. Defaults to ``300``.
            headers: Additional HTTP headers.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(GetMetadataResponse)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            url = f"{self.base_url()}/api/v{self.api_version}/buckets/{bucket_id}/metadata/{key}"
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                metadata_obj = ResponseModels.GetMetadataResponse.model_validate(response.json())
                return Ok(metadata_obj)
        except Exception as e:
            return Err(e)
    
    # @deprecated(reason="Use get_streaming or get_to_file instead for more efficient downloads.")
    async def get_by_checksum_to_file(
        self,
        checksum: str,
        chunk_size: str = "1MB",
        sink_folder_path: str = "/mictlanx/data",
        timeout: int = 300,
        filename: str = "",
        headers: Dict[str, str] = {},
        extension: str = "",
        verify:VerifyType = False
    ) -> Result[str, Exception]:
        """Download a chunk identified by its SHA-256 checksum and write it to disk.

        If the destination file already exists it is returned immediately
        without making a network request.

        Args:
            checksum: SHA-256 checksum that identifies the chunk.
            chunk_size: Streaming read buffer size. Defaults to ``"1MB"``.
            sink_folder_path: Destination directory. Created if absent.
                Defaults to ``"/mictlanx/data"``.
            timeout: Request timeout in seconds. Defaults to ``300``.
            filename: Override destination filename; defaults to ``checksum``.
            headers: Additional HTTP headers.
            extension: File extension appended to the filename. Defaults to
                ``""``.
            verify: SSL verification option. Defaults to ``False``.

        Returns:
            ``Ok(full_path)`` on success, ``Err(Exception)`` on failure.
        """
        try:
            # Convert the chunk size string into bytes
            _chunk_size = HF.parse_size(chunk_size)
            
            # Ensure the sink folder exists
            if not os.path.exists(sink_folder_path):
                os.makedirs(sink_folder_path, exist_ok=True)
            
            # Determine the filename to use (if none provided, use the checksum)
            combined_key = checksum if filename == "" else filename
            fullpath = f"{sink_folder_path}/{combined_key}{extension}"
            
            # If the file already exists, return its path
            if os.path.exists(fullpath):
                return Ok(fullpath)
            
            # Construct the URL for downloading the file by checksum
            url = f"{self.base_url()}/api/v{4}/buckets/checksum/{checksum}"
            
            # Create an asynchronous HTTP client
            async with httpx.AsyncClient(timeout=timeout,verify=verify) as client:
                # Stream the GET response from the URL
                async with client.stream("GET", url, headers=headers) as response:
                    response.raise_for_status()

            
            return Ok(fullpath)
        
        except Exception as e:
            return Err(e)
