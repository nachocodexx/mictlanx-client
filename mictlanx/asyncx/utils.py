import mictlanx.interfaces as InterfaceX
from mictlanx.services import AsyncRouter
import mictlanx.errors as EX
from mictlanx.utils.segmentation import Chunk
import humanfriendly as HF
from typing import Dict,Tuple,List
from option import Err,Ok,Result
import asyncio
from mictlanx.types import VerifyType
import httpx 
import time as T
from mictlanx.logger import Log
log = Log(name=__name__)

class AsyncClientUtils:
    """Static helper methods used by :class:`AsyncClient` for metadata processing.

    All methods are stateless — instantiation is not required; call them
    directly as ``AsyncClientUtils.method(...)``.
    """

    def __init__(self):
        pass

    @staticmethod
    def process_balls_segment(segment: List['InterfaceX.Metadata']) -> Dict[str, InterfaceX. Ball]:
        """Group a flat list of chunk metadata records into a ``ball_id`` → ``Ball`` dict.

        Args:
            segment: Flat list of :class:`Metadata` records for one segment of
                the overall ball list.

        Returns:
            Dict mapping each ``ball_id`` to a :class:`Ball` with its chunks.
        """
        local_balls: Dict[str, InterfaceX.Ball] = {}

        for chunk in segment:
            ball_id = chunk.ball_id
            if ball_id not in local_balls:
                local_balls[ball_id] = InterfaceX.Ball(bucket_id=chunk.bucket_id,ball_id=ball_id,chunks=[chunk])
            else:
                local_balls[ball_id].add_chunk(chunk)


        return local_balls

    @staticmethod
    def merge_balls(partials: List[Dict[str, InterfaceX.Ball]]) -> Dict[str, InterfaceX.Ball]:
        """Merge multiple partial ``ball_id`` → ``Ball`` dicts into one.

        Chunks from balls sharing the same ``ball_id`` across multiple
        partials are merged using :meth:`Ball.merge`.

        Args:
            partials: List of partial dicts returned by
                :meth:`process_balls_segment`.

        Returns:
            Single merged dict mapping each ``ball_id`` to its complete
            :class:`Ball`.
        """
        merged: Dict[str, InterfaceX.Ball] = {}

        for partial in partials:
            for ball_id, ball in partial.items():
                if ball_id in merged:
                    merged[ball_id].merge(other=ball)
                else:
                    merged[ball_id] = ball

        return merged

    @staticmethod
    async def group_chunks(balls_list: List['InterfaceX.Metadata'], num_threads: int = 4) -> Dict[str, InterfaceX.Ball]:
        """Group a large flat list of chunk metadata into balls in parallel.

        The list is split into ``num_threads`` segments; each segment is
        processed by :meth:`process_balls_segment` in a thread pool, and the
        results are merged with :meth:`merge_balls`.  Each ball's derived
        fields are populated via :meth:`Ball.build` before returning.

        Args:
            balls_list: Complete flat list of :class:`Metadata` records.
            num_threads: Number of parallel segments/threads. Defaults to ``4``.

        Returns:
            Dict mapping each ``ball_id`` to a fully-built :class:`Ball`.
        """
        chunk_size = (len(balls_list) + num_threads - 1) // num_threads
        segments = [balls_list[i * chunk_size:(i + 1) * chunk_size] for i in range(num_threads)]

        tasks = [asyncio.to_thread(AsyncClientUtils.process_balls_segment, segment) for segment in segments]
        partials = await asyncio.gather(*tasks)
        result = AsyncClientUtils.merge_balls(partials)
        for bid ,b in result.items():
            b.build()
        return result 

    @staticmethod
    async def merge_chunks(chunks:List[Tuple[InterfaceX.Metadata, memoryview]])->memoryview:
        """Merges memoryview chunks in correct order using metadata index."""
        try:
            num_chunks = len(chunks)
            ordered_chunks = [None] * num_chunks  # ✅ Pre-allocate N holes

            
            async def place_chunk(metadata, chunk):
                """Places chunk in the correct position."""
                index = int(metadata.tags["index"])  # Convert string index to int
                if 0 <= index < num_chunks:
                    ordered_chunks[index] = chunk  # ✅ Insert into correct position
                else: 
                    raise EX.ValidationError(message=f"Index out of range:{index} > {num_chunks}")

            # ✅ Process all chunks concurrently
            await asyncio.gather(*(place_chunk(meta, chunk) for meta, chunk in chunks))

            # ✅ Ensure all chunks are present (Optional: Handle missing chunks)
            if None in ordered_chunks:
                raise ValueError("Some chunks are missing")

            # ✅ Merge all chunks into one contiguous memoryview (join reads the buffers directly: a single copy)
            merged = b"".join(ordered_chunks)

            return memoryview(merged)
        except Exception as e:
            raise e


    @staticmethod
    async def get_chunk(
        client:httpx.Client,
        router: AsyncRouter,
        bucket_id: str,
        key: str,
        timeout: int = 120,
        headers: Dict[str, str] = {},
        chunk_size: str = "256kb",  # Faster large chunk transfers
        verify:VerifyType = False,
    ) -> Result[Tuple[InterfaceX.Metadata, memoryview], EX.MictlanXError]:
        """Ultra-fast download function similar to `curl`."""
        try:
            _chunk_size = HF.parse_size(chunk_size)  # Convert "4MB" to bytes

            metadata_result = await router.get_metadata(bucket_id, key, timeout, headers)
            if metadata_result.is_err:
                return Err(EX.MictlanXError.from_exception(metadata_result.unwrap_err()))
            
            metadata = metadata_result.unwrap().metadata
            expected_size = int(metadata.size)  # ✅ Expected total size

            # async with httpx.AsyncClient(http2=True, timeout=timeout,verify=verify) as client:
            url = f"{router.base_url()}/api/v4/buckets/{bucket_id}/{key}"
            async with client.stream("GET", url, headers=headers) as response:
                if response.status_code != 200:
                    return Err(EX.MictlanXError(f"HTTP {response.status_code}: Failed to fetch data"))

                chunk_data = bytearray(expected_size)
                view = memoryview(chunk_data)
                offset = 0

                async for chunk in response.aiter_bytes(_chunk_size):
                    size = len(chunk)
                    # pbar.update(size)

                    mv_chunk = memoryview(chunk)  # Convert bytes to memoryview

                    if offset + size > expected_size:
                        size = expected_size - offset  # Avoid overflow

                    view[offset:offset + size] = mv_chunk[:size]  # ✅ Corrected assignment
                    offset += size


                if offset != expected_size:
                    return Err(EX.MictlanXError(f"Mismatch: Received {offset} bytes, expected {expected_size}"))

                return Ok((metadata, memoryview(chunk_data)))  # ✅ Zero-copy memory handling

        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))


    @staticmethod
    async def _get_chunk(router:AsyncRouter,bucket_id, key:str,timeout:int = 120, headers:Dict[str,str]={},chunk_size:str="128kkb")->Result[Tuple[InterfaceX.Metadata, memoryview],EX.MictlanXError]:
        try:
            metadata_result = router.get_metadata(bucket_id=bucket_id, key=key, timeout=timeout,headers=headers)
            if metadata_result.is_ok:
                data_result = router.get_streaming(bucket_id=bucket_id, key=key,timeout=timeout, headers=headers)
                metadata = metadata_result.unwrap().metadata
                if data_result.is_ok:
                    return Ok((metadata, memoryview(data_result.unwrap().content )) )
                return Err(EX.MictlanXError.from_exception(data_result.unwrap_err()))
            else:
                return Err(EX.MictlanXError.from_exception(metadata_result.unwrap_err()))
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))


    @staticmethod
    async def put_chunk(router:AsyncRouter,client_id:str,ball_id:str,bucket_id:str, key:str, chunk:Chunk,metadata:Dict[str,str]={},rf:int=1,timeout:int = 120,chunk_size:str= "256kb")->Result[InterfaceX.PeerPutChunkedResponse, EX.MictlanXError]:
        """Upload a single chunk via a router using the two-step put flow.

        Calls :meth:`AsyncRouter.put_metadata` to register the chunk, then
        :meth:`AsyncRouter.put_chunked` to stream the bytes for each returned
        ``task_id``.

        Args:
            router: Router to upload through.
            client_id: Identifier of the uploading client (used as
                ``producer_id``).
            ball_id: Parent ball identifier.
            bucket_id: Destination bucket.
            key: Chunk key.
            chunk: :class:`Chunk` object containing data and checksum.
            metadata: Additional tags merged with the chunk's metadata.
                Defaults to ``{}``.
            rf: Replication factor. Defaults to ``1``.
            timeout: Request timeout in seconds. Defaults to ``120``.
            chunk_size: Streaming generator buffer size. Defaults to
                ``"256kb"``.

        Returns:
            ``Ok(PeerPutChunkedResponse)`` from the last successful upload,
            or ``Err(MictlanXError)`` on failure.
        """
        try:
            size    = chunk.size
            t1      = T.monotonic()
            _input  = {
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": chunk.chunk_id,
                "rf": rf,
                "timeout": timeout,
                "chunk_size": chunk_size,
            }
            put_metadata_result = await router.put_metadata(
                key                = chunk.chunk_id,
                bucket_id          = bucket_id,
                size               = size,
                checksum           = chunk.checksum,
                ball_id            = ball_id,
                content_type       = "application/octet-stream",
                is_disabled        = False,
                replication_factor = rf,
                tags               = {**chunk.metadata, **metadata},
                timeout            = timeout,
                headers            = {},
                producer_id        = client_id,
            )

            log.debug({
                "event": "PUT.METADATA",
                "message": "metadata registered",
                "bucket_id": bucket_id,
                "ball_id": ball_id,
                "key": chunk.chunk_id,
                "ok": put_metadata_result.is_ok,
                "response_time_ms": round((T.monotonic() - t1) * 1000, 2),
                "input": _input,
                "context": {"size": size, "checksum": chunk.checksum},
            })

            if put_metadata_result.is_ok:
                put_metadata_response = put_metadata_result.unwrap()
                for task_id in put_metadata_response.tasks_ids:
                    chunks = chunk.to_async_generator(chunk_size=chunk_size)
                    
                    put_result = await router.put_chunked(
                        task_id = task_id,
                        chunks  = chunks,
                        timeout = timeout,
                        headers = {"Content-Type": "application/octet-stream"}
                    )
                    return put_result
            else:
                e = put_metadata_result.unwrap_err()
                
                return Err(e)
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e=e))