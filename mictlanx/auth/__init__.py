from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from option import Result

import mictlanx.errors as EX


class AuthorizationService(ABC):
    """Pluggable authorization strategy for :class:`~mictlanx.asyncx.AsyncClient`.

    Subclass this and implement :meth:`authenticate` and :meth:`verify` to
    integrate MictlanX with your own auth backend (JWT, API keys, OAuth,
    ...). ``AsyncClient`` never talks to an auth backend directly — it only
    calls these two methods, lazily, the first time one of its public
    methods is invoked.
    """

    def __init__(
        self,
        credentials: Optional[Any] = None,
        username_or_email: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        """Store the raw material a subclass needs to authenticate.

        None of these are validated here — a subclass decides which
        combination it actually requires (e.g. ``token`` alone, or
        ``username_or_email`` + ``password``).

        Args:
            credentials: Free-form credential payload (e.g. an API key, a
                dict of client_id/client_secret, ...).
            username_or_email: Login identifier for username/password flows.
            password: Password for username/password flows.
            token: A pre-obtained token. When set, :meth:`verify` may accept
                it directly and skip calling :meth:`authenticate` entirely.
            headers: Extra headers merged into :meth:`auth_headers`.
        """
        self.credentials = credentials
        self.username_or_email = username_or_email
        self.password = password
        self.token = token
        self.headers: Dict[str, str] = dict(headers) if headers else {}

    @abstractmethod
    async def authenticate(self) -> Result[str, EX.MictlanXError]:
        """Obtain a new token and store it on ``self.token``.

        Called at most once per client, the first time it's needed — and
        again later only if a subsequent :meth:`verify` call fails (e.g. the
        token expired).

        Returns:
            ``Ok(token)`` on success, ``Err(MictlanXError)`` on failure
            (e.g. ``EX.AuthenticationError`` for bad credentials).
        """
        raise NotImplementedError

    @abstractmethod
    async def verify(self) -> Result[bool, EX.MictlanXError]:
        """Check whether the current ``self.token`` is still valid.

        Called before every :class:`AsyncClient` public method call, so
        this should be cheap (e.g. a local expiry check) rather than a
        network round-trip on every call. Returning ``Err`` — including
        when no token has been obtained yet — is expected, normal control
        flow, not an error condition to raise from.

        Returns:
            ``Ok(True)`` when the current token is valid, ``Err(...)``
            otherwise (including "not authenticated yet").
        """
        raise NotImplementedError

    def auth_headers(self) -> Dict[str, str]:
        """Build the headers to merge into an outgoing request.

        Returns:
            ``self.headers`` plus a ``Authorization: Bearer <token>`` entry
            when a token is present.
        """
        if self.token:
            return {**self.headers, "Authorization": f"Bearer {self.token}"}
        return dict(self.headers)
