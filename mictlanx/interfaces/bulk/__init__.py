from typing import List, Dict, AsyncGenerator, TypedDict, Union, Any,Optional
from mictlanx import errors as EX


class BallK(TypedDict, total=False):
    """
    Defines the structure for a single item in a bulk put operation.
    'total=False' means all keys are optional except the required ones.

    Required:
        bucket_id (str): The target bucket.
        ball_id (str): The object identifier.
        source (Union[bytes, str]): The data to upload, either as in-memory
                                   bytes or a string file path.
    Optional:
        tags (Dict[str, str]): A dictionary of tags for the object.
        rf (int): Replication factor.
        chunk_size (str): e.g., "1MB".
        timeout (int): Per-file timeout.
        max_tries (int): Retries for this specific file.
        max_concurrency (int): Concurrency *for this file's chunks*.
        max_backoff (int): Backoff for this specific file.
    """
    bucket_id: str
    ball_id: str
    source: Union[bytes, str]
    tags: Dict[str, str]
    rf: int
    chunk_size: str
    timeout: int
    max_tries: int
    max_concurrency: int
    max_backoff: int
class BallKDTO(TypedDict):
    """Data Transfer Object for BallK without optional fields."""
    bucket_id: str
    ball_id: str
    source:str
    tags: Dict[str, str]
    rf: int
    chunk_size: str
    timeout: int
    max_tries: int
    max_concurrency: int
    max_backoff: int
    in_memory: Optional[bool] = False
    @staticmethod
    def from_ballk(ball: BallK) -> 'BallKDTO':
        """Converts a BallK to a BallKDTO, setting 'in_memory' as needed."""
        dto: BallKDTO = {
            "bucket_id": ball["bucket_id"],
            "ball_id": ball["ball_id"],
            "source": ball["source"] if isinstance(ball["source"], str) else "",
            "tags": ball.get("tags", {}),
            "rf": ball.get("rf", 1),
            "chunk_size": ball.get("chunk_size", "1MB"),
            "timeout": ball.get("timeout", 30),
            "max_tries": ball.get("max_tries", 3),
            "max_concurrency": ball.get("max_concurrency", 5),
            "max_backoff": ball.get("max_backoff", 60),
        }
        ball_source = ball.get("source","")
        if isinstance(ball_source, bytes) or ball_source == "":
            dto["in_memory"] = True
        return dto



class BulkPutSuccess(TypedDict):
    """Reports a successful bulk upload item."""
    item: BallKDTO
    # result: Any # The 'Ok' result from the underlying put call (e.g., True)

class BulkPutFailure(TypedDict):
    """Reports a failed bulk upload item."""
    item: BallKDTO
    error: EX.MictlanXError

class BulkPutResponse(TypedDict):
    """Final response object for a bulk put operation."""
    successes: List[BulkPutSuccess]
    failures: List[BulkPutFailure]
    uploaded_size: Optional[int]=0
    total_balls: Optional[int]=0
    successed: Optional[int]=0
    failed: Optional[int]=0
    response_time: Optional[float]=0.0

