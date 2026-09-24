"""``VirtualStorageSpace``: deploy and elastically scale a local MictlanX
cluster (router + summoner + rm + a pool of storage peers) entirely from
Python, using the Docker Engine API.

Requires the optional ``docker`` (docker-py) dependency:
``pip install mictlanx[vss]``. Constructing a :class:`VirtualStorageSpace`
never requires ``docker`` to be installed — only :meth:`VirtualStorageSpace.up`
and the methods that depend on it do.
"""
import asyncio
from typing import Any, Dict, List, Optional, Set

from option import Err, Ok, Result

import mictlanx.errors as EX
from mictlanx.interfaces.responses import PeerStatsResponse
from mictlanx.services import AsyncPeer, AsyncRouter, AsyncSummoner

try:
    import docker
    from docker.errors import NotFound as DockerNotFound
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False


class VirtualStorageSpace:
    """A locally-deployed VSS: one router + summoner + rm + a pool of peers,
    all managed via the Docker Engine API (docker-py).

    ``__init__`` only stores desired state — no containers are created until
    :meth:`up` (or any of :meth:`expand`/:meth:`elastic`, which auto-deploy)
    is awaited.
    """

    def __init__(
        self,
        peers: int = 1,
        vss_id: str = "mictlanx-vss-0",
        network_id: str = "mictlanx",
        router_id: Optional[str] = None,
        router_port: int = 63666,
        router_image: str = "nachocode/mictlanx:router-0.1.0a6",
        summoner_port: int = 15000,
        summoner_image: str = "nachocode/mictlanx:summoner-0.1.0a4",
        rm_port: int = 5555,
        rm_image: str = "nachocode/mictlanx:rm-0.1.0a3",
        peer_image: str = "nachocode/mictlanx:peer-0.1.0a2",
        peer_base_port: int = 25000,
        peer_memory: str = "4GB",
        peer_disk: str = "100GB",
        peer_workers: int = 2,
        protocol: str = "http",
        api_version: int = 4,
    ):
        self.peers = peers
        self.vss_id = vss_id
        self.network_id = network_id
        # router_id defaults from vss_id so two VSS instances on one host
        # never collide on a container name.
        self.router_id = router_id or f"{vss_id}-router-0"
        self.router_port = router_port
        self.router_image = router_image
        self.summoner_id = f"{vss_id}-summoner-0"
        self.summoner_port = summoner_port
        self.summoner_image = summoner_image
        self.rm_id = f"{vss_id}-rm-0"
        self.rm_port = rm_port
        self.rm_image = rm_image
        self.peer_image = peer_image
        self.peer_base_port = peer_base_port
        self.peer_memory = peer_memory
        self.peer_disk = peer_disk
        self.peer_workers = peer_workers
        self.protocol = protocol
        self.api_version = api_version

        # Populated by up()/expand(), not by __init__.
        self._deployed: bool = False
        self._docker_client: Optional[Any] = None
        self._router: Optional[AsyncRouter] = None
        self._summoner: Optional[AsyncSummoner] = None
        self._peer_registry: List[Dict[str, Any]] = []
        self._next_peer_index: int = 0
        self._used_ports: Set[int] = set()

    def __str__(self) -> str:
        return f"VirtualStorageSpace(vss_id={self.vss_id}, size={self.size}, deployed={self._deployed})"

    # -------- lifecycle --------

    async def _run_sync(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _ensure_network(self) -> None:
        existing = self._docker_client.networks.list(names=[self.network_id])
        if not existing:
            self._docker_client.networks.create(self.network_id, driver="bridge")

    def _ensure_container(
        self,
        name: str,
        image: str,
        environment: Dict[str, str],
        ports: Dict[str, int],
        volumes: Dict[str, Dict[str, str]],
        command: Optional[Any] = None,
        privileged: bool = False,
    ) -> Any:
        try:
            container = self._docker_client.containers.get(name)
            if container.status != "running":
                container.start()
            return container
        except DockerNotFound:
            return self._docker_client.containers.run(
                image,
                name=name,
                hostname=name,
                environment=environment,
                ports=ports,
                volumes=volumes,
                command=command,
                network=self.network_id,
                detach=True,
                privileged=privileged,
                restart_policy={"Name": "unless-stopped"},
            )

    def _remove_container(self, name: str, remove_volumes: bool) -> None:
        try:
            container = self._docker_client.containers.get(name)
            container.remove(force=True, v=remove_volumes)
        except DockerNotFound:
            pass

    def _ensure_router_container(self) -> None:
        environment = {
            "MICTLANX_DEBUG": "0",
            "MICTLANX_ROUTER_HOST": "0.0.0.0",
            "MICTLANX_INTERNAL_ROUTER_PORT": "60666",
            "MICTLANX_MAX_WORKERS": "1",
            "MICTLANX_ROUTER_NETWORK_ID": self.network_id,
            "MICTLANX_PROCOTOL": self.protocol,
            "MICTLANX_API_VERSION": str(self.api_version),
            "MICTLANX_ROUTER_MAX_PEERS": "10",
            "MICTLANX_SUMMONER_API_VERSION": "3",
            "MICTLANX_SUMMONER_IP_ADDR": self.summoner_id,
            "MICTLANX_SUMMONER_PORT": str(self.summoner_port),
            "MICTLANX_SUMMONER_MODE": "docker",
            "MICTLANX_DAEMON_HOSTNAME": self.rm_id,
            "MICTLANX_DAEMON_PORT": str(self.rm_port),
            "MICTLANX_DAEMON_PROTOCOL": "tcp",
        }
        command = (
            "sh -c 'gunicorn --worker-class uvicorn.workers.UvicornWorker "
            "mictlanxrouter.server:app --bind 0.0.0.0:60666 --workers 1'"
        )
        self._ensure_container(
            name=self.router_id,
            image=self.router_image,
            environment=environment,
            ports={"60666/tcp": self.router_port},
            volumes={f"{self.router_id}-logs": {"bind": "/log", "mode": "rw"}},
            command=command,
        )

    def _ensure_summoner_container(self) -> None:
        environment = {
            "USER_ID": "1001",
            "GROUP_ID": "1002",
            "DOCKER_GID": "1001",
            "BASE_PATH": "/app/mictlanx",
            "BIN_NAME": "summoner",
            "NODE_ID": self.summoner_id,
            "NODE_PORT": str(self.summoner_port),
            "IP_ADDRESS": self.summoner_id,
            "SERVER_IP_ADDR": "0.0.0.0",
            "LOG_PATH": "/app/mictlanx/log",
            "LOCAL_PATH": "/app/mictlanx/local",
            "DATA_PATH": "/app/mictlanx/data",
        }
        self._ensure_container(
            name=self.summoner_id,
            image=self.summoner_image,
            environment=environment,
            ports={f"{self.summoner_port}/tcp": self.summoner_port},
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                f"{self.summoner_id}-data": {"bind": "/mictlanx", "mode": "rw"},
            },
            privileged=True,
        )

    def _ensure_rm_container(self) -> None:
        # `rm`'s peers.json (a bind-mounted host file in mictlanx-router.yml)
        # has no host-file equivalent here since VSS has no filesystem
        # artifact to mount. Left unmounted on the assumption that
        # `mictlanxrm.main` creates PEERS_CONFIG_PATH if it's missing;
        # verify this against a real image before relying on it in
        # production, and mount a pre-seeded file/volume here if it does not.
        environment = {
            "DAEMON_TICK_TIMEOUT": "10s",
            "MAX_TIMEOUT_TO_GET_STATS": "5s",
            "TICK_PEERS_HANDSHAKE": "5s",
            "RECOVER_TICK_TIMEOUT": "10s",
            "QUEUE_TICK_TIMEOUT": "5s",
            "MAX_TIMEOUT_TO_RECOVER": "10s",
            "MAX_TIMEOUT_PEERS_HANDSHAKE": "30s",
            "MAX_RECOVER_TIME_UNTIL_RESTART": "5sec",
            "MICTLANX_PROTOCOL": "tcp",
            "MICTLANX_IP_ADDR": "0.0.0.0",
            "MICTLANX_PORT": str(self.rm_port),
            "MICTLANX_DEBUG": "0",
            "MICTLANX_GC_TIMEOUT": "60s",
            "LOCAL_STORE_PATH": "/mictlanx/db/x.json",
            "SUMMONER_IP_ADDR": self.summoner_id,
            "SUMMONER_PROTOCOL": self.protocol,
            "SUMMONER_PORT": str(self.summoner_port),
            "SUMMONER_API_VERSION": "3",
            "MICTLANX_PEERS_PROTOCOL": self.protocol,
            "MICTLANX_PEERS_API_VERSION": str(self.api_version),
            "MICTLANX_PEERS": "mictlanx://examples-vss-01-peer-0@examples-vss-01-peer-0:25000,examples-vss-01-peer-1@examples-vss-01-peer-1:25001/?protocol=http&api_version=4",
            "DEFAULT_PEER_MEMORY": self.peer_memory,
            "DEFAULT_PEER_DISK": self.peer_disk,
            "DEFAULT_PEER_CPU": "1",
            "MAX_RETRIES": "2",
            "MAX_IDLE_TIME": "10s",
            "SUMMONER_BASE_PORT": str(self.peer_base_port),
            "SUMMONER_BASE_PROTOCOL": self.protocol,
            "SUMMONER_PHYSICAL_NODES_INDEXES": "0,2,3,4,5,6,7,8,9",
            "DEBUG": "True",
            "SUMMONER_MODE": "docker",
            "PEERS_CONFIG_PATH": "/app/peers.json",
            "MAX_TIMEOUT_TO_SAVE_PEER_CONFIG": "30min",
            "PEER_ELASTIC": "True",
            "SUMMONER_PEER_DOCKER_IMAGE": self.peer_image,
            "PEER_MIN_INTERVAL_TIME": "5",
            "PEER_MAX_INTERVAL_TIME": "20",
            "SUMMONER_NETWORK_ID": self.network_id,
        }
        self._ensure_container(
            name=self.rm_id,
            image=self.rm_image,
            environment=environment,
            ports={f"{self.rm_port}/tcp": self.rm_port},
            volumes={f"{self.rm_id}-db": {"bind": "/mictlanx/db", "mode": "rw"}},
            command=["python", "-m", "mictlanxrm.main"],
        )

    async def _wait_healthy(self, timeout_s: int = 120, interval_s: float = 2.0) -> Result[bool, EX.MictlanXError]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        while True:
            summoner_res = await self._summoner.health_check()
            router_res = await self._router.get_stats()
            if summoner_res.is_ok and router_res.is_ok:
                return Ok(True)
            if loop.time() >= deadline:
                return Err(EX.RequestTimeoutError(
                    f"VirtualStorageSpace '{self.vss_id}' did not become healthy within {timeout_s}s"
                ))
            await asyncio.sleep(interval_s)

    async def up(self) -> Result[bool, EX.MictlanXError]:
        """Idempotent bootstrap: creates the docker network, router, summoner
        and rm containers (reusing/starting them if they already exist),
        then brings the peer pool up to ``self.peers`` via :meth:`expand`.
        """
        if self._deployed:
            return Ok(True)
        if not DOCKER_AVAILABLE:
            return Err(EX.DockerNotAvailableError())
        try:
            self._docker_client = await self._run_sync(docker.from_env)
            self._used_ports = {self.router_port, self.summoner_port, self.rm_port}

            await self._run_sync(self._ensure_network)
            await self._run_sync(self._ensure_router_container)
            await self._run_sync(self._ensure_summoner_container)
            await self._run_sync(self._ensure_rm_container)

            self._router = AsyncRouter(
                router_id=self.router_id,
                ip_addr="localhost",
                port=self.router_port,
                protocol=self.protocol,
                api_version=self.api_version,
            )
            self._summoner = AsyncSummoner(
                summoner_id=self.summoner_id,
                ip_addr="localhost",
                port=self.summoner_port,
                protocol=self.protocol,
                storage_peer_image=self.peer_image,
            )

            health_res = await self._wait_healthy()
            if health_res.is_err:
                return Err(health_res.unwrap_err())

            # Set _deployed before expand() so its auto-up() check is a no-op.
            self._deployed = True

            if self.peers > 0:
                expand_res = await self.expand(n=self.peers)
                if expand_res.is_err:
                    return Err(expand_res.unwrap_err())

            return Ok(True)
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))

    async def down(self, remove_volumes: bool = False) -> Result[bool, EX.MictlanXError]:
        """Tear down every container this instance deployed (peers, rm,
        summoner, router). Does not remove the docker network by default.
        Safe to call even if :meth:`up` was never awaited (no-op)."""
        if not self._deployed:
            return Ok(True)
        if not DOCKER_AVAILABLE:
            return Err(EX.DockerNotAvailableError())
        try:
            for peer in reversed(self._peer_registry):
                await self._summoner.delete_container(container_id=peer["container_id"])
            self._peer_registry.clear()

            await self._run_sync(self._remove_container, self.rm_id, remove_volumes)
            await self._run_sync(self._remove_container, self.summoner_id, remove_volumes)
            await self._run_sync(self._remove_container, self.router_id, remove_volumes)

            self._deployed = False
            self._router = None
            self._summoner = None
            return Ok(True)
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))

    # -------- scaling --------

    def _allocate_port(self) -> int:
        port = self.peer_base_port + self._next_peer_index
        while port in self._used_ports:
            port += 1
        self._used_ports.add(port)
        return port

    async def expand(
        self,
        n: int = 1,
        memory: Optional[str] = None,
        disk: Optional[str] = None,
        workers: Optional[int] = None,
    ) -> Result[List[str], EX.MictlanXError]:
        """Summon ``n`` new peer containers, mesh them with peers already in
        this VSS (via the ``PEERS`` env var), and register them with the
        router. Auto-deploys via :meth:`up` first if not yet deployed.
        Returns the new peer ids."""
        if not self._deployed:
            up_res = await self.up()
            if up_res.is_err:
                return Err(up_res.unwrap_err())
            if self._deployed and n == self.peers:
                # up() already brought the pool to `self.peers` as part of
                # its own bootstrap — nothing left to do for this call.
                return Ok(self.peer_ids())

        if not DOCKER_AVAILABLE:
            return Err(EX.DockerNotAvailableError())

        existing_addrs = [
            f"{self.protocol}://{p['peer_id']}:{p['port']}" for p in self._peer_registry
        ]

        new_peer_ids: List[str] = []
        try:
            for _ in range(n):
                index = self._next_peer_index
                peer_id = f"{self.vss_id}-peer-{index}"
                port = self._allocate_port()

                summon_res = await self._summoner.summon_peer(
                    container_id=peer_id,
                    port=port,
                    memory=memory or self.peer_memory,
                    disk=disk or self.peer_disk,
                    workers=workers or self.peer_workers,
                    network_id=self.network_id,
                    peers=list(existing_addrs),
                )
                if summon_res.is_err:
                    return Err(EX.MictlanXError.from_exception(summon_res.unwrap_err()))

                entry = {"peer_id": peer_id, "container_id": peer_id, "port": port, "index": index}
                self._peer_registry.append(entry)
                existing_addrs.append(f"{self.protocol}://{peer_id}:{port}")
                self._next_peer_index += 1
                new_peer_ids.append(peer_id)

            new_entries = self._peer_registry[-n:] if n > 0 else []
            if new_entries:
                async_peers = [
                    AsyncPeer(
                        peer_id=p["peer_id"],
                        ip_addr=p["peer_id"],
                        port=p["port"],
                        protocol=self.protocol,
                        api_version=self.api_version,
                    )
                    for p in new_entries
                ]
                add_res = await self._router.add_peers(async_peers)
                if add_res.is_err:
                    return Err(EX.MictlanXError.from_exception(add_res.unwrap_err()))

            return Ok(new_peer_ids)
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))

    # `vs.elastic(n=1)` is exactly `vs.expand(n=1)`.
    elastic = expand

    async def retract(self, n: int = 1) -> Result[List[str], EX.MictlanXError]:
        """Remove the ``n`` most-recently-added peer containers (LIFO) via
        ``AsyncSummoner.delete_container``.

        Known gap: there is no router endpoint to deregister a peer, so a
        retracted peer's container is gone but the router may keep
        referencing it until its own health/liveness logic (if any) notices.
        """
        if not self._deployed:
            return Err(EX.VSSNotDeployedError())
        if n > len(self._peer_registry):
            return Err(EX.BadParametersError(
                f"Cannot retract {n} peers; only {len(self._peer_registry)} deployed"
            ))
        removed_ids: List[str] = []
        try:
            for _ in range(n):
                peer = self._peer_registry.pop()
                del_res = await self._summoner.delete_container(container_id=peer["container_id"])
                if del_res.is_err:
                    self._peer_registry.append(peer)
                    return Err(EX.MictlanXError.from_exception(del_res.unwrap_err()))
                removed_ids.append(peer["peer_id"])
            return Ok(removed_ids)
        except Exception as e:
            return Err(EX.MictlanXError.from_exception(e))

    # -------- introspection --------

    @property
    def size(self) -> int:
        """Current number of live peers managed by this VSS."""
        return len(self._peer_registry)

    @property
    def uri(self) -> str:
        """``mictlanx://`` URI of this VSS's router, ready for ``AsyncClient(uri=...)``.

        Derived from the constructor arguments, so it is available before
        :meth:`up`; the router only answers once the VSS is deployed."""
        return f"mictlanx://{self.router_id}@localhost:{self.router_port}/?protocol={self.protocol}&api_version={self.api_version}&http2=0"

    def peer_ids(self) -> List[str]:
        """container_ids of all peers currently deployed by this instance."""
        return [p["peer_id"] for p in self._peer_registry]

    async def stats(self) -> Result[Dict[str, PeerStatsResponse], EX.MictlanXError]:
        """Aggregated router-reported stats for the current peer pool."""
        if not self._deployed or self._router is None:
            return Err(EX.VSSNotDeployedError())
        res = await self._router.get_stats()
        if res.is_err:
            return Err(EX.MictlanXError.from_exception(res.unwrap_err()))
        return Ok(res.unwrap())

    # -------- async context manager --------

    async def __aenter__(self) -> "VirtualStorageSpace":
        res = await self.up()
        if res.is_err:
            raise res.unwrap_err()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.down()
