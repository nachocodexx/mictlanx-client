
from typing import Optional
import socket
# 
import httpx
# 
from mictlanx.utils import Utils
class MictlanXError(Exception):
    """Base class for all custom exceptions."""
    
    default_message = "An error occurred."
    default_status_code     = 500                   # Default to Internal Server Error
    default_error_code      = 0

    def __init__(self, message:Optional[int]=None, status_code:Optional[int]=None,error_code:Optional[int]= None):
        """Initialise the error with a message, HTTP-like status code, and app error code.

        Args:
            message: Human-readable description. Defaults to
                ``default_message`` if ``None``.
            status_code: HTTP-like status code. Defaults to ``500``.
            error_code: Application-specific error code. Defaults to ``0``.
        """
        self.message     = message or self.default_message
        self.status_code = status_code or self.default_status_code
        self.error_code  = error_code or self.default_error_code
        super().__init__(self.message)

    def __str__(self):
        return f"{self.__class__.__name__} (status={self.status_code},code={self.error_code}): {self.message}"
    def get_name(self):
        """Return the snake_case name of this error class.

        Returns:
            The class name converted to snake_case (e.g.
            ``"not_found_error"``).
        """
        return Utils.camel_to_snake(self.__class__.__name__)
    
    @staticmethod
    def _root_cause(exc: BaseException) -> BaseException:
        cur = exc
        # Walk __cause__/__context__ to the deepest cause.
        while True:
            nxt = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
            if nxt is None: return cur
            cur = nxt

    @staticmethod
    def _format_endpoint_from_request(req: Optional[httpx.Request]) -> str:
        if not req:
            return ""
        try:
            url = req.url
            # Explicit host:port if available
            host = url.host or ""
            port = url.port or (443 if url.scheme == "https" else 80)
            return f"{url.scheme}://{host}:{port}"
        except Exception:
            return ""
        
    @staticmethod
    def from_exception(e: Exception) -> 'MictlanXError': 
        """Maps an exception to a defined error class based on its status code."""

        status_code = 500  # default
        message = str(e)   # default message
        error_code = 0    # default error code

        # If it's from httpx and a non-2xx HTTP response
        if isinstance(e, httpx.RequestError):
            endpoint = MictlanXError._format_endpoint_from_request(getattr(e, "request", None))
            root = MictlanXError._root_cause(e)
            root_name = root.__class__.__name__
            root_msg = str(root) or repr(root)

            # Classify special cases
            if isinstance(e, httpx.ConnectTimeout):
                return RequestTimeoutError(f"Timeout connecting to {endpoint} (connect): {root_name}: {root_msg}")
            if isinstance(e, (httpx.ReadTimeout, httpx.WriteTimeout)):
                return RequestTimeoutError(f"Timeout during request to {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.ConnectError):
                # Common low-level causes: socket.gaierror (DNS), ConnectionRefusedError, etc.
                if isinstance(root, socket.gaierror):
                    return DNSResolutionError(f"DNS resolution failed for {endpoint}: {root}")
                if isinstance(root, ConnectionRefusedError):
                    return ConnectFailedError(f"Connection refused to {endpoint}: {root}")
                return ConnectFailedError(f"Failed to connect to {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.ProxyError):
                return NetworkError(f"Proxy error while contacting {endpoint}: {root_name}: {root_msg}")
            if isinstance(e, httpx.RemoteProtocolError):
                return UpstreamProtocolError(f"Remote protocol error from {endpoint}: {root_name}: {root_msg}")

        if isinstance(e, httpx.HTTPStatusError):
            resp        = e.response
            status_code = resp.status_code
            # FastAPI puts detail in JSON body
            try:
                detail     = resp.json().get("detail","Unknown Error")
                message    = detail.get("msg", "Unknown Error") if isinstance(detail, dict) else str(detail)
                error_code = detail.get("code",0)
                # message = 
                # detail if isinstance(detail, str) else str(detail)
            except Exception:
                message = resp.text

        # Else if the exception has a status_code attribute (like FastAPI HTTPException locally)
        elif hasattr(e, "status_code"):
            status_code = getattr(e, "status_code", 500)
            if hasattr(e, "detail"):
                message = e.detail if isinstance(e.detail, str) else str(e.detail)
                if hasattr(e, "code"):
                    error_code = getattr(e, "code",0)

        # Define mapping of status codes to custom exceptions
        ERROR_MAP = {
            0: UnknownError, 
            400: ValidationError,
            401: AuthenticationError,
            403: PermissionError,
            404: NotFoundError, 
            405: FileAlreadyExists,
            500: UnknownError,
            501: IntegrityError,
            502: PutChunksError,
            503: GetChunkError,
            1000: NetworkError,
            1001: ConnectFailedError,
            1002: DNSResolutionError,
            1004: RequestTimeoutError,     # mirrors 504
            1005: UpstreamProtocolError,
            666: MaxAvailabilityReachedError,
        }

        error_class = ERROR_MAP.get(error_code, UnknownError)
        return error_class(message)

class MaxAvailabilityReachedError(MictlanXError):
    """Exception raised when maximum replication factor is reached."""
    def __init__(self, message = "Maximum availability reached for this resource",status_code:int = 409,error_code:int = 666):
        super().__init__(message, status_code, error_code)

class ValidationError(MictlanXError):
    """Exception raised when request parameters fail validation (HTTP 400)."""
    def __init__(self, message = "Validation failed",status_code:int = 400,error_code:int = 400):
        super().__init__(message, status_code, error_code)

class GetChunkError(MictlanXError):
    """Exception raised when a chunk download fails after all retries (HTTP 503)."""
    def __init__(self, message = "Get chunk failed",status_code:int = 503,error_code:int = 503):
        super().__init__(message, status_code, error_code)
class PutChunksError(MictlanXError):
    """Exception raised when a chunk upload fails after all retries (HTTP 502)."""
    def __init__(self, message = "Put chunks failed",status_code:int = 502,error_code:int = 502):
        super().__init__(message, status_code, error_code)

class IntegrityError(MictlanXError):
    """Exception raised when the SHA-256 checksum of reassembled data does not match the stored value (HTTP 501)."""
    def __init__(self, message = "Integrity check failed",status_code:int = 501,error_code:int = 501):
        super().__init__(message, status_code, error_code)

class FilterError(MictlanXError):
    """Exception raised for the IO-filter pipeline (put()/get() ``filters=``) error family (HTTP 500)."""
    def __init__(self, message = "Filter pipeline error",status_code:int = 500,error_code:int = 506):
        super().__init__(message, status_code, error_code)

class FilterMismatchError(FilterError):
    """Exception raised when get()'s filters don't match what was recorded at put() time (HTTP 422)."""
    def __init__(self, message = "Filter mismatch",status_code:int = 422,error_code:int = 504):
        super().__init__(message, status_code, error_code)

class FilterExecutionError(FilterError):
    """Exception raised when a filter's filter() call itself fails (bad key, tampered ciphertext, decompression error, missing optional dependency) (HTTP 500)."""
    def __init__(self, message = "Filter execution failed",status_code:int = 500,error_code:int = 505):
        super().__init__(message, status_code, error_code)

class UnknownError(MictlanXError):
    """Catch-all exception for unmapped error codes (HTTP 500)."""
    def __init__(self, message = "An unknown error occurred",status_code:int = 500,error_code:int = 500):
        super().__init__(message, status_code, error_code)


class NotFoundError(MictlanXError):
    """Exception raised when a resource is not found."""
    def __init__(self, message = "Resource not found",status_code:int = 404,error_code:int = 404):
        super().__init__(message, status_code, error_code)
    # default_message = "Resource not found."
    # error_code = 404


class AuthenticationError(MictlanXError):
    """Exception raised for authentication failures."""
    def __init__(self, message = "Authentication failed",status_code:int = 401,error_code:int = 401):
        super().__init__(message, status_code, error_code)

class PermissionError(MictlanXError):
    """Exception raised when a user lacks permissions."""
    def __init__(self, message = "Permission denied",status_code:int = 403,error_code:int = 403):
        super().__init__(message, status_code, error_code)

class FileAlreadyExists(MictlanXError):
    """Exception raised when a local file already exists at the target download path (HTTP 405)."""
    def __init__(self, message = "File already exists",status_code:int = 405,error_code:int = 405):
        super().__init__(message, status_code, error_code)

class NetworkError(MictlanXError):
    """Exception raised for generic network-level failures (error code 1000)."""

    def __init__(self, message = "Network error",status_code=1000,error_code=1000):
        super().__init__(message, status_code, error_code)

class ConnectFailedError(NetworkError):
    """Exception raised when a TCP connection to a peer or router cannot be established."""

    def __init__(self, message = "Connection failed",status_code:int = 1001,error_code:int = 1001):
        super().__init__(message, status_code, error_code)

class DNSResolutionError(NetworkError):
    """Exception raised when a hostname cannot be resolved via DNS."""

    def __init__(self, message = "DNS resolution failed",status_code:int = 1002,error_code:int = 1002):
        super().__init__(message, status_code, error_code)

class RequestTimeoutError(NetworkError):
    """Exception raised when a request to a peer or router exceeds its deadline."""

    def __init__(self, message = "Request timed out",status_code:int = 1004,error_code:int = 1004):
        super().__init__(message, status_code, error_code)

class UpstreamProtocolError(MictlanXError):
    """Exception raised when an upstream peer or router violates the HTTP protocol."""

    def __init__(self, message = "Upstream protocol error",status_code:int = 1005,error_code:int = 1005):
        super().__init__(message, status_code, error_code)

class BadParametersError(MictlanXError):
    """Exception raised when a function is called with invalid parameters."""
    def __init__(self, message = "Bad parameters",status_code:int = 400,error_code:int = 400):
        super().__init__(message, status_code, error_code)

class DockerNotAvailableError(MictlanXError):
    """Exception raised when the `docker` package (docker-py) is not installed."""
    def __init__(self, message = "docker SDK not installed. Install with: pip install mictlanx[vss]",status_code:int = 500,error_code:int = 700):
        super().__init__(message, status_code, error_code)

class VSSNotDeployedError(MictlanXError):
    """Exception raised when an operation requires a deployed VirtualStorageSpace but up() hasn't run yet."""
    def __init__(self, message = "VirtualStorageSpace is not deployed",status_code:int = 409,error_code:int = 702):
        super().__init__(message, status_code, error_code)
class BallConflictError(MictlanXError):
    """Exception raised when a put targets an existing ball with different data (balls are immutable)."""
    def __init__(self, message = "Ball already exists with a different checksum",status_code:int = 409,error_code:int = 710):
        super().__init__(message, status_code, error_code)

class NoActiveClientError(MictlanXError):
    """Exception raised when a Bucket handle is used without a client outside ``async with AsyncClient(...)``."""
    def __init__(self, message = "No active AsyncClient: pass client= or use `async with AsyncClient(...)`",status_code:int = 500,error_code:int = 711):
        super().__init__(message, status_code, error_code)
