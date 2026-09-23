import asyncio

import pytest
from option import Ok, Err

import mictlanx.errors as EX
from mictlanx.auth import AuthorizationService
from mictlanx.asyncx import AsyncClient
from mictlanx.retry import RetryPolicy

# A syntactically valid, unreachable URI. Construction never touches the
# network, and port 1 lets any *unexpected* real connection attempt fail
# fast instead of hanging — so these tests need no live VSS.
FAKE_URI = "mictlanx://fake@localhost:1/?protocol=http&api_version=4&http2=0"

# A handful of the tests below deliberately let a call past the auth gate
# to prove it reaches real (unreachable) network code. The client's default
# retry policy is 5 attempts with backoff up to 10s each — far too slow for
# a unit test — so those calls pass this single-attempt, no-delay policy.
FAST_FAIL_RETRY_POLICY = RetryPolicy(retries=1, initial_delay=0.01, jitter=False)


class FakeAuthorizationService(AuthorizationService):
    """Controllable AuthorizationService for exercising AsyncClient's auth hook."""

    def __init__(self, *args, fail_authenticate: bool = False, authenticate_delay: float = 0.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.authenticate_calls = 0
        self.verify_calls = 0
        self.fail_authenticate = fail_authenticate
        self.authenticate_delay = authenticate_delay

    async def authenticate(self):
        self.authenticate_calls += 1
        if self.authenticate_delay:
            await asyncio.sleep(self.authenticate_delay)
        if self.fail_authenticate:
            return Err(EX.AuthenticationError("bad credentials"))
        self.token = "tok-123"
        return Ok(self.token)

    async def verify(self):
        self.verify_calls += 1
        if self.token:
            return Ok(True)
        return Err(EX.AuthenticationError("not authenticated"))


# ── AuthorizationService (ABC) ──────────────────────────────────────────────

def test_cannot_instantiate_authorization_service_directly():
    with pytest.raises(TypeError):
        AuthorizationService()


def test_cannot_instantiate_partial_subclass():
    class OnlyAuthenticate(AuthorizationService):
        async def authenticate(self):
            return Ok("t")
        # verify() left unimplemented

    with pytest.raises(TypeError):
        OnlyAuthenticate()


def test_auth_headers_without_token():
    svc = FakeAuthorizationService(headers={"X-Extra": "1"})
    assert svc.auth_headers() == {"X-Extra": "1"}


def test_auth_headers_with_token():
    svc = FakeAuthorizationService(headers={"X-Extra": "1"})
    svc.token = "abc"
    headers = svc.auth_headers()
    assert headers["Authorization"] == "Bearer abc"
    assert headers["X-Extra"] == "1"


# ── AsyncClient wiring ───────────────────────────────────────────────────────

def test_default_authz_is_none():
    client = AsyncClient(uri=FAKE_URI, debug=False)
    assert client.authz is None


@pytest.mark.asyncio
async def test_ensure_authenticated_is_noop_when_authz_none():
    client = AsyncClient(uri=FAKE_URI, debug=False)
    # Must not raise, and must not touch the network.
    await client._ensure_authenticated()


@pytest.mark.asyncio
async def test_ensure_authenticated_authenticates_once_then_reuses_token():
    authz = FakeAuthorizationService()
    client = AsyncClient(uri=FAKE_URI, debug=False, authz=authz)

    await client._ensure_authenticated()
    assert authz.authenticate_calls == 1
    assert authz.token == "tok-123"

    await client._ensure_authenticated()
    assert authz.authenticate_calls == 1  # verify() succeeded — no re-auth


@pytest.mark.asyncio
async def test_ensure_authenticated_skips_authenticate_when_token_preset():
    # Simulates a shared AuthorizationService (or a pre-obtained token)
    # already holding a valid token before the client ever touches it.
    authz = FakeAuthorizationService(token="preexisting")
    client = AsyncClient(uri=FAKE_URI, debug=False, authz=authz)

    await client._ensure_authenticated()
    assert authz.authenticate_calls == 0
    assert authz.verify_calls == 1


@pytest.mark.asyncio
async def test_ensure_authenticated_raises_on_failed_authenticate():
    authz = FakeAuthorizationService(fail_authenticate=True)
    client = AsyncClient(uri=FAKE_URI, debug=False, authz=authz)

    with pytest.raises(EX.AuthenticationError):
        await client._ensure_authenticated()
    assert authz.authenticate_calls == 1


@pytest.mark.asyncio
async def test_concurrent_first_calls_authenticate_only_once():
    authz = FakeAuthorizationService(authenticate_delay=0.05)
    client = AsyncClient(uri=FAKE_URI, debug=False, authz=authz)

    await asyncio.gather(*[client._ensure_authenticated() for _ in range(20)])

    assert authz.authenticate_calls == 1
    assert authz.token == "tok-123"


# ── @_require_auth on real public methods ───────────────────────────────────

@pytest.mark.asyncio
async def test_decorated_method_short_circuits_without_network_on_auth_failure():
    authz = FakeAuthorizationService(fail_authenticate=True)
    client = AsyncClient(uri=FAKE_URI, debug=False, authz=authz)

    result = await client.get_metadata_by_key(bucket_id="b1", key="k1")

    assert result.is_err
    # If the decorator hadn't short-circuited before the real network call,
    # this would surface a ConnectFailedError (port 1 refuses) instead.
    assert isinstance(result.unwrap_err(), EX.AuthenticationError)
    assert authz.authenticate_calls == 1


@pytest.mark.asyncio
async def test_decorated_method_runs_once_authenticated_but_then_hits_real_network():
    # Once auth succeeds, the real method body runs — which does need the
    # network. Against our unreachable fake URI that surfaces as a
    # connection error, proving the auth gate let the call through.
    authz = FakeAuthorizationService()
    client = AsyncClient(uri=FAKE_URI, debug=False, authz=authz)

    result = await client.get_metadata_by_key(
        bucket_id="b1", key="k1", timeout=2, retry_policy=FAST_FAIL_RETRY_POLICY
    )

    assert authz.authenticate_calls == 1
    assert result.is_err
    assert not isinstance(result.unwrap_err(), EX.AuthenticationError)


@pytest.mark.asyncio
async def test_authz_none_preserves_existing_behavior():
    # No authz at all: the decorated method should behave exactly as before
    # this feature existed — i.e. go straight to the network attempt.
    client = AsyncClient(uri=FAKE_URI, debug=False)

    result = await client.get_metadata_by_key(
        bucket_id="b1", key="k1", timeout=2, retry_policy=FAST_FAIL_RETRY_POLICY
    )

    assert result.is_err
    assert not isinstance(result.unwrap_err(), EX.AuthenticationError)
