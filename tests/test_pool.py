import asyncio

import pytest
from httpunk import Backend

import punkreq
from punkreq import Limits
from punkreq._connect import Origin
from punkreq._pool import ConnectionPool


ORIGIN = Origin("https", "example.com", 443)
OTHER_ORIGIN = Origin("https", "other.org", 443)


def run(coro):
    return asyncio.run(coro)


class FakeConnection:
    def __init__(self, multiplexed=False):
        self.multiplexed = multiplexed
        self.closed = False
        self.busy = False  # httpunk >= 0.1.4: exchange holds the in-flight slot
        self.entered = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self.closed = True
        return False

    async def ready(self):
        if self.closed:
            raise RuntimeError("connection is closed")


class FakeConnector:
    def __init__(self, multiplexed=False, delay=0.0, fail=0):
        self.multiplexed = multiplexed
        self.delay = delay
        self.fail = fail  # number of dials to fail before succeeding
        self.dials = 0
        self.connections = []

    async def __call__(self, origin):
        self.dials += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail > 0:
            self.fail -= 1
            raise punkreq.ConnectError(f"boom dialing {origin}")
        conn = FakeConnection(multiplexed=self.multiplexed)
        self.connections.append(conn)
        return conn


def make_pool(connector, **limits):
    backend = Backend.asyncio.create()
    return ConnectionPool(connector, backend=backend, limits=Limits(**limits))


class SteppingBackend:
    """Delegates to a real backend, but monotonic() strictly increases on every
    call: on coarse OS clocks (Windows, ~16ms) consecutive releases can get
    equal idle timestamps, making eviction order tie-dependent."""

    def __init__(self, backend):
        self._backend = backend
        self._now = 0.0

    def monotonic(self):
        self._now += 1.0
        return self._now

    def __getattr__(self, name):
        return getattr(self._backend, name)


class TestH1:
    def test_acquire_is_exclusive_and_reused_after_release(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, exclusive, _ = await pool.acquire(ORIGIN)
            assert exclusive
            assert conn.entered
            await pool.release(ORIGIN, conn)
            assert pool.idle_count == 1
            conn2, _, _ = await pool.acquire(ORIGIN)
            assert conn2 is conn
            await pool.release(ORIGIN, conn2)
            await pool.close()
            assert conn.closed

        run(main())
        assert connector.dials == 1

    def test_parallel_acquires_on_known_h1_dial_in_parallel(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            # settle the mode first
            conn, _, _ = await pool.acquire(ORIGIN)
            await pool.release(ORIGIN, conn)
            conns = await asyncio.gather(pool.acquire(ORIGIN), pool.acquire(ORIGIN), pool.acquire(ORIGIN))
            assert len({id(c) for c, _, _ in conns}) == 3
            assert pool.connection_count == 3
            for c, _, _ in conns:
                await pool.release(ORIGIN, c)
            await pool.close()

        run(main())
        assert connector.dials == 3

    def test_closed_connection_not_reused(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            await pool.release(ORIGIN, conn)
            conn.closed = True
            conn2, _, _ = await pool.acquire(ORIGIN)
            assert conn2 is not conn
            assert pool.connection_count == 1
            await pool.close()

        run(main())
        assert connector.dials == 2

    def test_release_of_closed_connection_drops_it(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            conn.closed = True
            await pool.release(ORIGIN, conn)
            assert pool.idle_count == 0
            assert pool.connection_count == 0
            await pool.close()

        run(main())

    def test_keepalive_expiry(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector, keepalive_expiry=0.01)
            conn, _, _ = await pool.acquire(ORIGIN)
            await pool.release(ORIGIN, conn)
            await asyncio.sleep(0.03)
            conn2, _, _ = await pool.acquire(ORIGIN)
            assert conn2 is not conn
            assert conn.closed
            await pool.close()

        run(main())
        assert connector.dials == 2

    def test_max_keepalive_evicts_oldest(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector, max_keepalive_connections=1)
            first, _, _ = await pool.acquire(ORIGIN)
            await pool.release(ORIGIN, first)
            second, _, _ = await pool.acquire(OTHER_ORIGIN)
            await pool.release(OTHER_ORIGIN, second)
            assert pool.idle_count == 1
            assert first.closed  # oldest evicted
            assert not second.closed
            await pool.close()

        run(main())


class TestH2:
    def test_shared_connection(self):
        connector = FakeConnector(multiplexed=True)

        async def main():
            pool = make_pool(connector)
            conn1, exclusive1, _ = await pool.acquire(ORIGIN)
            conn2, exclusive2, _ = await pool.acquire(ORIGIN)
            assert conn1 is conn2
            assert not exclusive1 and not exclusive2
            assert pool.connection_count == 1
            await pool.close()
            assert conn1.closed

        run(main())
        assert connector.dials == 1

    def test_concurrent_dials_coalesce(self):
        connector = FakeConnector(multiplexed=True, delay=0.02)

        async def main():
            pool = make_pool(connector)
            conns = await asyncio.gather(*[pool.acquire(ORIGIN) for _ in range(5)])
            assert len({id(c) for c, _, _ in conns}) == 1
            await pool.close()

        run(main())
        assert connector.dials == 1

    def test_dead_shared_connection_redialed(self):
        connector = FakeConnector(multiplexed=True)

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            conn.closed = True
            conn2, _, _ = await pool.acquire(ORIGIN)
            assert conn2 is not conn
            assert pool.connection_count == 1
            await pool.close()

        run(main())
        assert connector.dials == 2


class TestHostPruning:
    def test_keepalive_eviction_prunes_dead_hosts(self):
        connector = FakeConnector()
        origins = [Origin("http", f"origin-{i}.example", 80) for i in range(20)]

        async def main():
            pool = make_pool(connector, max_keepalive_connections=1)
            for origin in origins:
                conn, _, _ = await pool.acquire(origin)
                await pool.release(origin, conn)
            assert pool.idle_count == 1
            assert len(pool._hosts) == 1  # only the origin holding the idle conn
            await pool.close()
            assert len(pool._hosts) == 0

        run(main())

    def test_dial_failure_prunes(self):
        origins = [Origin("http", f"origin-{i}.example", 80) for i in range(10)]
        connector = FakeConnector(fail=len(origins))

        async def main():
            pool = make_pool(connector)
            for origin in origins:
                with pytest.raises(punkreq.ConnectError):
                    await pool.acquire(origin)
            assert len(pool._hosts) == 0
            assert pool.connection_count == 0
            await pool.close()

        run(main())

    def test_connect_timeout_prunes(self):
        connector = FakeConnector(delay=1.0)

        async def main():
            pool = make_pool(connector)
            with pytest.raises(punkreq.ConnectTimeout):
                await pool.acquire(ORIGIN, connect_timeout=0.02)
            assert len(pool._hosts) == 0
            await pool.close()

        run(main())

    def test_release_of_closed_connection_prunes(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            conn.closed = True
            await pool.release(ORIGIN, conn)
            assert len(pool._hosts) == 0
            await pool.close()

        run(main())

    def test_leased_host_survives_eviction(self):
        connector = FakeConnector()

        async def main():
            backend = SteppingBackend(Backend.asyncio.create())
            pool = ConnectionPool(connector, backend=backend, limits=Limits(max_keepalive_connections=1))
            first, _, _ = await pool.acquire(ORIGIN)
            second, _, _ = await pool.acquire(ORIGIN)
            await pool.release(ORIGIN, first)
            # OTHER_ORIGIN's release evicts ORIGIN's idle conn; the entry must
            # survive because `second` is still checked out.
            other, _, _ = await pool.acquire(OTHER_ORIGIN)
            await pool.release(OTHER_ORIGIN, other)
            assert first.closed
            assert ORIGIN in pool._hosts
            await pool.release(ORIGIN, second)
            assert not second.closed  # parked, not dropped
            again, _, reused = await pool.acquire(ORIGIN)
            assert again is second
            assert reused
            await pool.release(ORIGIN, again)
            await pool.close()

        run(main())

    def test_dead_shared_h2_and_failed_redial_prune(self):
        connector = FakeConnector(multiplexed=True)

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            conn.closed = True
            connector.fail = 1
            with pytest.raises(punkreq.ConnectError):
                await pool.acquire(ORIGIN)
            assert len(pool._hosts) == 0
            assert pool.connection_count == 0
            await pool.close()

        run(main())


class TestDiscard:
    def test_release_discard_drops_open_connection(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            assert not conn.closed
            # discard: the exchange didn't complete cleanly — the conn must be
            # closed and dropped even though its own state still claims open
            await pool.release(ORIGIN, conn, discard=True)
            assert conn.closed
            assert pool.idle_count == 0
            assert pool.connection_count == 0
            assert len(pool._hosts) == 0
            conn2, _, reused = await pool.acquire(ORIGIN)
            assert conn2 is not conn
            assert not reused
            await pool.release(ORIGIN, conn2)
            await pool.close()

        run(main())
        assert connector.dials == 2


class TestBusy:
    def test_busy_connection_not_parked(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            conn.busy = True  # release was interrupted; the exchange still holds the slot
            await pool.release(ORIGIN, conn)
            assert conn.closed
            assert pool.idle_count == 0
            await pool.close()

        run(main())

    def test_busy_idle_connection_dropped_at_acquire(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            conn, _, _ = await pool.acquire(ORIGIN)
            await pool.release(ORIGIN, conn)
            assert pool.idle_count == 1
            conn.busy = True
            conn2, _, _ = await pool.acquire(ORIGIN)
            assert conn2 is not conn
            assert conn.closed
            assert pool.connection_count == 1
            await pool.release(ORIGIN, conn2)
            await pool.close()

        run(main())
        assert connector.dials == 2


class TestLimitsAndTimeouts:
    def test_max_connections_blocks_then_reuses_released(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector, max_connections=1)
            conn, _, _ = await pool.acquire(ORIGIN)

            async def second():
                other, _, _ = await pool.acquire(ORIGIN)
                return other

            task = asyncio.ensure_future(second())
            await asyncio.sleep(0.02)
            assert not task.done()  # blocked on capacity
            await pool.release(ORIGIN, conn)
            other = await asyncio.wait_for(task, 1.0)
            assert other is conn  # reused the released connection
            await pool.close()

        run(main())
        assert connector.dials == 1

    def test_pool_timeout(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector, max_connections=1)
            conn, _, _ = await pool.acquire(ORIGIN)
            with pytest.raises(punkreq.PoolTimeout):
                await pool.acquire(ORIGIN, pool_timeout=0.02)
            await pool.release(ORIGIN, conn)
            await pool.close()

        run(main())

    def test_connect_timeout(self):
        connector = FakeConnector(delay=1.0)

        async def main():
            pool = make_pool(connector)
            with pytest.raises(punkreq.ConnectTimeout):
                await pool.acquire(ORIGIN, connect_timeout=0.02)
            assert pool.connection_count == 0
            await pool.close()

        run(main())

    def test_dial_failure_propagates_and_pool_recovers(self):
        connector = FakeConnector(fail=1)

        async def main():
            pool = make_pool(connector)
            with pytest.raises(punkreq.ConnectError):
                await pool.acquire(ORIGIN)
            assert pool.connection_count == 0
            conn, _, _ = await pool.acquire(ORIGIN)
            assert pool.connection_count == 1
            await pool.release(ORIGIN, conn)
            await pool.close()

        run(main())
        assert connector.dials == 2

    def test_acquire_after_close_raises(self):
        connector = FakeConnector()

        async def main():
            pool = make_pool(connector)
            await pool.close()
            with pytest.raises(RuntimeError):
                await pool.acquire(ORIGIN)

        run(main())
