"""Unit tests for mictlanx.vss.VirtualStorageSpace.

These are mocked: docker-py's client is a MagicMock (no real Docker daemon
needed), and the HTTP calls VirtualStorageSpace makes through AsyncSummoner /
AsyncRouter are intercepted with respx. This is the suite `run_tests.sh`
picks up by default -- fast and deterministic.

A small live-integration tier (marked `@pytest.mark.integration`, requiring a
real Docker daemon) is intentionally NOT included here yet; see the plan at
/home/nacho/.claude/plans/ethereal-pondering-stroustrup.md for that follow-up.
"""
import json as J
import re
import uuid
from unittest.mock import MagicMock

import docker as docker_sdk
import httpx
import pytest
import pytest_asyncio
import respx

import mictlanx.errors as EX
from mictlanx.utils.uri import MictlanXURI
from mictlanx.vss import VirtualStorageSpace

ROUTER_PORT = 64666
SUMMONER_PORT = 15100
RM_PORT = 5655


def _mock_docker_client() -> MagicMock:
    """A stateful docker-py client mock: containers created via `.run()`
    become visible to subsequent `.get()` calls by name (simulating a real
    daemon), and are absent (raise NotFound) until then."""
    client = MagicMock()
    created = {}

    def get_side_effect(name):
        if name in created:
            return created[name]
        raise docker_sdk.errors.NotFound(f"container {name} not found")

    def run_side_effect(image, name=None, **kwargs):
        container = MagicMock()
        container.status = "running"
        container.name = name
        created[name] = container
        return container

    client.networks.list.return_value = []
    client.networks.create.return_value = MagicMock()
    client.containers.get.side_effect = get_side_effect
    client.containers.run.side_effect = run_side_effect
    client._created = created
    return client


@pytest_asyncio.fixture
async def vss(monkeypatch):
    mock_client = _mock_docker_client()
    monkeypatch.setattr("mictlanx.vss.docker.from_env", lambda: mock_client)
    instance = VirtualStorageSpace(
        peers=2,
        vss_id=f"test-vss-{uuid.uuid4().hex[:6]}",
        router_port=ROUTER_PORT,
        summoner_port=SUMMONER_PORT,
        rm_port=RM_PORT,
    )
    instance._mock_docker_client = mock_client  # stashed for assertions
    yield instance


def _mock_infra(router: respx.MockRouter, summon_bodies: list = None):
    """Register routes on the given (test-local) respx router for the
    summoner + router HTTP calls a deployed VirtualStorageSpace makes.

    NOTE: `@respx.mock(...)` (with parentheses) creates a *local* router,
    separate from the `respx.get/post/route` module-level shortcuts (which
    target respx's *global* router) -- routes must be registered on this
    same local `router` instance or they silently never match.
    """
    summoner_base = f"http://localhost:{SUMMONER_PORT}/api/v3"
    router_base = f"http://localhost:{ROUTER_PORT}"

    router.get(f"{summoner_base}/health").mock(return_value=httpx.Response(200, json={"status": "ok"}))
    router.get(f"{summoner_base}/stats").mock(return_value=httpx.Response(200, json={"summons": {}}))
    router.get(f"{router_base}/api/v4/peers/stats").mock(return_value=httpx.Response(200, json={}))
    router.post(f"{router_base}/api/v4/xpeers").mock(return_value=httpx.Response(200, json={}))

    def summon_side_effect(request):
        body = J.loads(request.content)
        if summon_bodies is not None:
            summon_bodies.append(body)
        return httpx.Response(200, json={
            "container_id": body["container_id"],
            "service_time": 1,
            "ip_addr": body["container_id"],
            "port": body["exposed_ports"][0]["host_port"],
        })

    router.post(f"{summoner_base}/containers").mock(side_effect=summon_side_effect)
    router.route(method="DELETE", url__regex=re.escape(summoner_base) + r"/containers/.*").mock(
        return_value=httpx.Response(200, json={})
    )


# ── Construction (no I/O) ───────────────────────────────────────────────────

def test_construction_requires_no_docker():
    """__init__ must never touch docker-py or do any I/O."""
    vs = VirtualStorageSpace(peers=3)
    assert vs.size == 0
    assert vs.peer_ids() == []
    assert vs._deployed is False
    assert vs._docker_client is None


def test_router_id_defaults_from_vss_id():
    vs = VirtualStorageSpace(peers=1, vss_id="alpha")
    assert vs.router_id == "alpha-router-0"
    assert vs.summoner_id == "alpha-summoner-0"
    assert vs.rm_id == "alpha-rm-0"


def test_router_id_override_respected():
    vs = VirtualStorageSpace(peers=1, vss_id="alpha", router_id="my-custom-router")
    assert vs.router_id == "my-custom-router"


def test_uri_points_at_router():
    vs = VirtualStorageSpace(peers=1, vss_id="alpha", router_port=61000)
    assert vs.uri == "mictlanx://alpha-router-0@localhost:61000/?protocol=http&api_version=4&http2=0"
    routers = MictlanXURI.parse(vs.uri)
    assert [(r.router_id, r.port) for r in routers] == [("alpha-router-0", 61000)]


# ── up() ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_up_creates_infra_and_initial_peers(vss, respx_mock):
    _mock_infra(respx_mock)
    res = await vss.up()
    assert res.is_ok, res.unwrap_err()
    assert vss._deployed is True
    assert vss.size == 2
    # router, summoner, rm containers -- peers go through Summoner's HTTP API, not docker-py
    assert vss._mock_docker_client.containers.run.call_count == 3
    created_names = set(vss._mock_docker_client._created.keys())
    assert created_names == {vss.router_id, vss.summoner_id, vss.rm_id}


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_up_is_idempotent(vss, respx_mock):
    _mock_infra(respx_mock)
    first = await vss.up()
    assert first.is_ok
    second = await vss.up()
    assert second.is_ok
    # No extra containers/peers on the second call.
    assert vss._mock_docker_client.containers.run.call_count == 3
    assert vss.size == 2


# ── expand() / elastic() ────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_expand_auto_deploys_without_double_creating(vss, respx_mock):
    """expand(n=peers) on a fresh instance must not create peers twice
    (once via up()'s internal expand, once via this call)."""
    _mock_infra(respx_mock)
    res = await vss.expand(n=2)
    assert res.is_ok, res.unwrap_err()
    assert vss.size == 2


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_expand_adds_additional_peers(vss, respx_mock):
    _mock_infra(respx_mock)
    up_res = await vss.up()
    assert up_res.is_ok
    res = await vss.expand(n=1)
    assert res.is_ok, res.unwrap_err()
    assert len(res.unwrap()) == 1
    assert vss.size == 3


@pytest.mark.asyncio
async def test_elastic_is_expand(vss):
    assert vss.elastic == vss.expand


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_expand_meshes_new_peer_with_existing_ones(vss, respx_mock):
    bodies = []
    _mock_infra(respx_mock, summon_bodies=bodies)
    up_res = await vss.up()
    assert up_res.is_ok

    await vss.expand(n=1)

    # The 3rd peer's PEERS env must reference the first two.
    third_peer_body = bodies[-1]
    peers_env = third_peer_body["envs"]["PEERS"]
    first_two_ids = vss.peer_ids()[:2]
    for pid in first_two_ids:
        assert pid in peers_env


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_expand_registers_new_peers_with_router(vss, respx_mock):
    _mock_infra(respx_mock)
    await vss.up()
    xpeers_calls_before = len([c for c in respx_mock.calls if c.request.url.path == "/api/v4/xpeers"])
    await vss.expand(n=1)
    xpeers_calls_after = len([c for c in respx_mock.calls if c.request.url.path == "/api/v4/xpeers"])
    assert xpeers_calls_after == xpeers_calls_before + 1


# ── retract() ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_retract_removes_most_recent_peer(vss, respx_mock):
    _mock_infra(respx_mock)
    await vss.up()
    await vss.expand(n=1)  # size == 3 now
    last_peer_id = vss.peer_ids()[-1]

    res = await vss.retract(n=1)
    assert res.is_ok, res.unwrap_err()
    assert res.unwrap() == [last_peer_id]
    assert vss.size == 2
    assert last_peer_id not in vss.peer_ids()


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_retract_more_than_size_fails_without_mutating(vss, respx_mock):
    _mock_infra(respx_mock)
    await vss.up()
    res = await vss.retract(n=99)
    assert res.is_err
    assert isinstance(res.unwrap_err(), EX.BadParametersError)
    assert vss.size == 2  # unchanged


@pytest.mark.asyncio
async def test_retract_before_deploy_fails():
    vs = VirtualStorageSpace(peers=1)
    res = await vs.retract(n=1)
    assert res.is_err
    assert isinstance(res.unwrap_err(), EX.VSSNotDeployedError)


# ── down() ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_down_is_a_no_op_when_never_deployed():
    vs = VirtualStorageSpace(peers=1)
    res = await vs.down()
    assert res.is_ok
    assert res.unwrap() is True


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_down_removes_all_tracked_containers(vss, respx_mock):
    _mock_infra(respx_mock)
    await vss.up()
    created = dict(vss._mock_docker_client._created)

    res = await vss.down()
    assert res.is_ok, res.unwrap_err()
    assert vss._deployed is False
    assert vss.size == 0
    for name, container in created.items():
        container.remove.assert_called_once()


# ── size / peer_ids / stats ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stats_before_deploy_fails():
    vs = VirtualStorageSpace(peers=1)
    res = await vs.stats()
    assert res.is_err
    assert isinstance(res.unwrap_err(), EX.VSSNotDeployedError)


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_stats_delegates_to_router(vss, respx_mock):
    _mock_infra(respx_mock)
    await vss.up()
    res = await vss.stats()
    assert res.is_ok, res.unwrap_err()
    assert isinstance(res.unwrap(), dict)


# ── async context manager ───────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_context_manager_deploys_and_tears_down(vss, respx_mock):
    _mock_infra(respx_mock)
    async with vss as v:
        assert v.size == 2
        assert v._deployed is True
    assert vss._deployed is False


@pytest.mark.asyncio
@respx.mock(assert_all_called=False)
async def test_context_manager_tears_down_on_exception(vss, respx_mock):
    _mock_infra(respx_mock)
    with pytest.raises(RuntimeError):
        async with vss:
            raise RuntimeError("boom")
    assert vss._deployed is False


# ── optional dependency guard ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_up_without_docker_installed_returns_clear_error(monkeypatch):
    monkeypatch.setattr("mictlanx.vss.DOCKER_AVAILABLE", False)
    vs = VirtualStorageSpace(peers=1)
    res = await vs.up()
    assert res.is_err
    assert isinstance(res.unwrap_err(), EX.DockerNotAvailableError)
