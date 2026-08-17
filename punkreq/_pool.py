from __future__ import annotations

import contextlib
import threading
import typing

from httpunk.h2.client import H2Connection

from ._config import DEFAULT_LIMITS, Limits
from ._connect import Origin
from ._exceptions import ConnectTimeout, PoolTimeout


__all__ = ["ConnectionPool"]


def _is_multiplexed(conn: typing.Any) -> bool:
    flag = getattr(conn, "multiplexed", None)
    if flag is not None:
        return bool(flag)
    return isinstance(conn, H2Connection)


class _HostState:
    __slots__ = ("mode", "shared", "idle", "dialing", "leased")

    def __init__(self) -> None:
        self.mode: str | None = None  # None (unknown) | "h1" | "h2"
        self.shared: typing.Any = None  # the h2 connection
        self.idle: list[tuple[typing.Any, float]] = []  # (h1 conn, idle_since); LIFO
        self.dialing: typing.Any = None  # event while a coalesced dial is in flight
        self.leased = 0  # h1 conns checked out exclusively


class ConnectionPool:
    """Pools httpunk connections per origin. `acquire()` returns
    `(connection, exclusive, reused)`; when `exclusive` is True (h1) the caller
    must hand the connection back via `release()` once the exchange is over.
    `reused` is True when the connection served earlier exchanges (an h1
    keep-alive checkout or the existing shared h2 connection) — the signal for
    retrying requests that die in the idle-reuse race."""

    def __init__(
        self,
        connector: typing.Callable[[Origin], typing.Awaitable[typing.Any]],
        *,
        backend: typing.Any,
        limits: Limits = DEFAULT_LIMITS,
    ) -> None:
        self._connector = connector
        self._backend = backend
        self._limits = limits
        self._lock = threading.Lock()  # guards state; never held across an await
        self._hosts: dict[Origin, _HostState] = {}
        self._count = 0  # live connections (in use + idle + being dialed)
        self._waiters: list[typing.Any] = []  # events of acquirers waiting for capacity
        self._closed = False

    @property
    def connection_count(self) -> int:
        with self._lock:
            return self._count

    @property
    def idle_count(self) -> int:
        with self._lock:
            return sum(len(host.idle) for host in self._hosts.values())

    async def acquire(
        self,
        origin: Origin,
        *,
        connect_timeout: float | None = None,
        pool_timeout: float | None = None,
    ) -> tuple[typing.Any, bool, bool]:
        deadline = self._backend.monotonic() + pool_timeout if pool_timeout is not None else None
        while True:
            action, value, stale = self._attempt(origin)
            await self._close_all(stale)

            if action == "conn":
                conn, exclusive = value
                if exclusive:
                    return conn, True, True
                if await self._h2_ready(origin, conn, deadline):
                    return conn, False, True
                continue  # the shared connection died; re-attempt (re-dial)

            if action == "dial":
                conn = await self._dial(origin, connect_timeout)
                return self._install(origin, conn)

            # action == "wait": either a coalesced dial or pool capacity
            await self._wait(value, deadline)

    async def release(self, origin: Origin, conn: typing.Any) -> None:
        """Return an exclusively-held (h1) connection to the idle set, or close
        it if it is no longer reusable."""
        to_close = []
        with self._lock:
            host = self._hosts.get(origin)
            if host is not None:
                host.leased -= 1
            if conn.closed or host is None or self._closed:
                self._count -= 1
                to_close.append(conn)
                self._prune_locked(origin)
            else:
                host.idle.append((conn, self._backend.monotonic()))
                to_close.extend(self._evict_over_keepalive_locked())
        await self._close_all(to_close)
        if not to_close:
            self._wake_waiters()  # an idle connection is now available for reuse

    async def close(self) -> None:
        with self._lock:
            self._closed = True
            conns = []
            for host in self._hosts.values():
                if host.shared is not None:
                    conns.append(host.shared)
                conns.extend(conn for conn, _ in host.idle)
            self._hosts.clear()
            self._count = 0
        for conn in conns:
            await self._close_conn(conn)
        self._wake_waiters()

    async def __aenter__(self) -> ConnectionPool:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        await self.close()
        return False

    # -- internals -----------------------------------------------------------

    def _attempt(self, origin: Origin) -> tuple[str, typing.Any, list[typing.Any]]:
        """One synchronous scheduling decision, under the lock. Returns
        (action, value, stale-connections-to-close)."""
        stale: list[typing.Any] = []
        with self._lock:
            if self._closed:
                raise RuntimeError("The connection pool is closed.")
            host = self._hosts.setdefault(origin, _HostState())

            if host.shared is not None:
                if host.shared.closed:
                    stale.append(host.shared)
                    self._count -= 1
                    host.shared = None
                else:
                    return "conn", (host.shared, False), stale

            now = self._backend.monotonic()
            expiry = self._limits.keepalive_expiry
            while host.idle:
                conn, since = host.idle.pop()
                if conn.closed or (expiry is not None and now - since >= expiry):
                    stale.append(conn)
                    self._count -= 1
                else:
                    host.leased += 1
                    return "conn", (conn, True), stale

            if host.dialing is not None and host.mode != "h1":
                return "wait", host.dialing, stale

            max_connections = self._limits.max_connections
            if max_connections is None or self._count < max_connections:
                self._count += 1
                if host.mode != "h1":
                    host.dialing = self._backend.event()
                return "dial", None, stale

            event = self._backend.event()
            self._waiters.append(event)
            self._prune_locked(origin)
            return "wait", event, stale

    async def _dial(self, origin: Origin, connect_timeout: float | None) -> typing.Any:
        try:
            if connect_timeout is None:
                conn = await self._connector(origin)
                await conn.__aenter__()
            else:
                result, completed = await self._backend.timeout(self._dial_inner(origin), connect_timeout)
                if not completed:
                    raise ConnectTimeout(f"Timed out connecting to {origin}")
                conn = result
        except BaseException:
            self._abandon_dial(origin)
            raise
        return conn

    async def _dial_inner(self, origin: Origin) -> typing.Any:
        conn = await self._connector(origin)
        await conn.__aenter__()
        return conn

    def _abandon_dial(self, origin: Origin) -> None:
        with self._lock:
            self._count -= 1
            host = self._hosts.get(origin)
            event = None
            if host is not None and host.dialing is not None:
                event, host.dialing = host.dialing, None
            self._prune_locked(origin)
        if event is not None:
            event.set()
        self._wake_waiters()

    def _install(self, origin: Origin, conn: typing.Any) -> tuple[typing.Any, bool, bool]:
        """Record a freshly-dialed connection and settle the origin's mode."""
        with self._lock:
            host = self._hosts.setdefault(origin, _HostState())
            event, host.dialing = host.dialing, None
            if _is_multiplexed(conn):
                host.mode = "h2"
                host.shared = conn
                exclusive = False
            else:
                host.mode = "h1"
                host.leased += 1
                exclusive = True
        if event is not None:
            event.set()
        return conn, exclusive, False

    async def _h2_ready(self, origin: Origin, conn: typing.Any, deadline: float | None) -> bool:
        """Wait for a stream slot on the shared connection. False → the
        connection is dead (evicted here); the caller should re-attempt."""
        try:
            if deadline is None:
                await conn.ready()
            else:
                remaining = deadline - self._backend.monotonic()
                if remaining <= 0:
                    raise PoolTimeout("Timed out waiting for a connection from the pool")
                _, completed = await self._backend.timeout(conn.ready(), remaining)
                if not completed:
                    raise PoolTimeout("Timed out waiting for a connection from the pool")
        except (PoolTimeout, ConnectTimeout):
            raise
        except Exception:
            # ready() raises when the connection failed or the peer sent GOAWAY
            with self._lock:
                host = self._hosts.get(origin)
                if host is not None and host.shared is conn:
                    host.shared = None
                    self._count -= 1
                    self._prune_locked(origin)
            await self._close_conn(conn)
            self._wake_waiters()
            return False
        return True

    async def _wait(self, event: typing.Any, deadline: float | None) -> None:
        try:
            if deadline is None:
                await event.wait()
                return
            remaining = deadline - self._backend.monotonic()
            if remaining <= 0:
                raise PoolTimeout("Timed out waiting for a connection from the pool")
            _, completed = await self._backend.timeout(event.wait(), remaining)
            if not completed:
                raise PoolTimeout("Timed out waiting for a connection from the pool")
        finally:
            with self._lock:
                if event in self._waiters:
                    self._waiters.remove(event)

    def _evict_over_keepalive_locked(self) -> list[typing.Any]:
        """Under the lock: pop the oldest idle connections beyond the keepalive cap."""
        cap = self._limits.max_keepalive_connections
        if cap is None:
            return []
        evicted = []
        while sum(len(host.idle) for host in self._hosts.values()) > cap:
            origin, oldest_host = min(
                ((origin, host) for origin, host in self._hosts.items() if host.idle),
                key=lambda item: item[1].idle[0][1],
            )
            conn, _ = oldest_host.idle.pop(0)
            evicted.append(conn)
            self._count -= 1
            self._prune_locked(origin)
        return evicted

    def _prune_locked(self, origin: Origin) -> None:
        """Under the lock: drop the origin's entry once nothing references it.
        Only the cached `mode` hint is lost; `setdefault` recreates on demand."""
        host = self._hosts.get(origin)
        if host is not None and host.shared is None and not host.idle and host.dialing is None and host.leased == 0:
            del self._hosts[origin]

    async def _close_all(self, conns: list[typing.Any]) -> None:
        for conn in conns:
            await self._close_conn(conn)
        if conns:
            self._wake_waiters()

    async def _close_conn(self, conn: typing.Any) -> None:
        with contextlib.suppress(Exception):
            await conn.__aexit__(None, None, None)

    def _wake_waiters(self) -> None:
        with self._lock:
            waiters, self._waiters = self._waiters, []
        for event in waiters:
            event.set()
