from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional, TypeVar
from option import Result
import mictlanx.errors as EX

if TYPE_CHECKING:
    from mictlanx.asyncx import AsyncClient

T = TypeVar("T")

# Set by ``async with AsyncClient(...)``; lets ``Bucket("bk1")`` find its client.
_current_client: ContextVar[Optional["AsyncClient"]] = ContextVar("mictlanx_current_client", default=None)


def current_client() -> "AsyncClient":
    """Return the client bound by the innermost ``async with AsyncClient(...)``.

    Raises:
        NoActiveClientError: when called outside a client context.
    """
    client = _current_client.get()
    if client is None:
        raise EX.NoActiveClientError()
    return client


def _unwrap(result: Result[T, Exception]) -> T:
    """Return the ``Ok`` value or raise the ``Err`` as a :class:`MictlanXError`."""
    if result.is_ok:
        return result.unwrap()
    err = result.unwrap_err()
    if isinstance(err, EX.MictlanXError):
        raise err
    raise EX.MictlanXError.from_exception(err)


def _is_not_found(err: Exception) -> bool:
    """Best-effort check that an error means "ball/bucket does not exist"."""
    if isinstance(err, EX.NotFoundError):
        return True
    if getattr(err, "status_code", None) == 404 or getattr(err, "error_code", None) == 404:
        return True
    message = str(getattr(err, "message", err)).lower()
    return "not found" in message or "404" in message
