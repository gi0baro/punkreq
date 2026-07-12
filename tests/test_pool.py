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
