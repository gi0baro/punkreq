import asyncio

import pytest
from httpunk import Backend

import punkreq
from punkreq import Limits
from punkreq._connect import Origin
from punkreq._pool import ConnectionPool
from tests.fakes import FakeConnection, FakeConnector, SteppingBackend


pytestmark = pytest.mark.asyncio

ORIGIN = Origin("https", "example.com", 443)
OTHER_ORIGIN = Origin("https", "other.org", 443)
THIRD_ORIGIN = Origin("https", "third.dev", 443)


def stepping_pool(connector, **limits):
    """A pool whose clock strictly increases per call, for eviction-order tests."""
    return ConnectionPool(connector, backend=SteppingBackend(Backend.asyncio.create()), limits=Limits(**limits))


# ----- h1: one exchange per connection -----


async def test_h1_acquire_is_exclusive_and_reused_after_release(make_pool):
    connector = FakeConnector()
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
    assert connector.dials == 1


async def test_h1_known_origin_dials_in_parallel(make_pool):
    connector = FakeConnector()
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
    assert connector.dials == 3


async def test_h1_closed_connection_not_reused(make_pool):
    connector = FakeConnector()
    pool = make_pool(connector)
    conn, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, conn)
    conn.closed = True
    conn2, _, _ = await pool.acquire(ORIGIN)
    assert conn2 is not conn
    assert pool.connection_count == 1
    await pool.close()
    assert connector.dials == 2


async def test_h1_release_of_closed_connection_drops_it(make_pool):
    pool = make_pool(FakeConnector())
    conn, _, _ = await pool.acquire(ORIGIN)
    conn.closed = True
    await pool.release(ORIGIN, conn)
    assert pool.idle_count == 0
    assert pool.connection_count == 0
    assert len(pool._hosts) == 0  # the origin's entry is pruned with it
    await pool.close()


async def test_h1_keepalive_expiry(make_pool):
    connector = FakeConnector()
    pool = make_pool(connector, keepalive_expiry=0.01)
    conn, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, conn)
    await asyncio.sleep(0.03)
    conn2, _, _ = await pool.acquire(ORIGIN)
    assert conn2 is not conn
    assert conn.closed
    await pool.close()
    assert connector.dials == 2


async def test_h1_max_keepalive_evicts_oldest(make_pool):
    pool = make_pool(FakeConnector(), max_keepalive_connections=1)
    first, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, first)
    second, _, _ = await pool.acquire(OTHER_ORIGIN)
    await pool.release(OTHER_ORIGIN, second)
    assert pool.idle_count == 1
    assert first.closed  # oldest evicted
    assert not second.closed
    await pool.close()


# ----- h2: one shared connection per origin -----


async def test_h2_connection_is_shared(make_pool):
    connector = FakeConnector(multiplexed=True)
    pool = make_pool(connector)
    conn1, exclusive1, _ = await pool.acquire(ORIGIN)
    conn2, exclusive2, _ = await pool.acquire(ORIGIN)
    assert conn1 is conn2
    assert not exclusive1 and not exclusive2
    assert pool.connection_count == 1
    await pool.close()
    assert conn1.closed
    assert connector.dials == 1


async def test_h2_concurrent_dials_coalesce(make_pool):
    connector = FakeConnector(multiplexed=True, delay=0.02)
    pool = make_pool(connector)
    conns = await asyncio.gather(*[pool.acquire(ORIGIN) for _ in range(5)])
    assert len({id(c) for c, _, _ in conns}) == 1
    await pool.close()
    assert connector.dials == 1


async def test_h2_dead_shared_connection_redialed(make_pool):
    connector = FakeConnector(multiplexed=True)
    pool = make_pool(connector)
    conn, _, _ = await pool.acquire(ORIGIN)
    conn.closed = True
    conn2, _, _ = await pool.acquire(ORIGIN)
    assert conn2 is not conn
    assert pool.connection_count == 1
    await pool.close()
    assert connector.dials == 2


async def test_h2_shared_lease_bookkeeping(make_pool):
    pool = make_pool(FakeConnector(multiplexed=True))
    conn, _, _ = await pool.acquire(ORIGIN)  # install: first lease
    again, _, _ = await pool.acquire(ORIGIN)  # checkout: second
    assert again is conn
    host = pool._hosts[ORIGIN]
    assert host.shared_leases == 2
    await pool.release(ORIGIN, conn)
    assert host.shared_leases == 1
    await pool.release(ORIGIN, conn)
    assert host.shared_leases == 0
    assert host.shared_idle_since > 0.0
    await pool.close()


async def test_h2_ready_failure_gives_back_lease_and_redials(make_pool):
    class NotReady(FakeConnection):
        def __init__(self):
            super().__init__(multiplexed=True)
            self.fail_ready = False

        async def ready(self):
            if self.fail_ready:
                raise RuntimeError("GOAWAY")
            await super().ready()

    async def connector(origin):
        return NotReady()

    pool = make_pool(connector)
    conn, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, conn)
    conn.fail_ready = True
    conn2, _, _ = await pool.acquire(ORIGIN)  # ready() fails: evict + redial
    assert conn2 is not conn
    assert conn.closed
    assert pool.connection_count == 1
    assert pool._hosts[ORIGIN].shared_leases == 1  # the fresh install's lease
    await pool.release(ORIGIN, conn2)
    await pool.close()


async def test_h2_late_release_of_replaced_shared_is_noop(make_pool):
    pool = make_pool(FakeConnector(multiplexed=True))
    old, _, _ = await pool.acquire(ORIGIN)
    old.closed = True  # dies with its lease still out
    new, _, _ = await pool.acquire(ORIGIN)  # checkout evicts + redials
    assert new is not old
    count = pool.connection_count
    leases = pool._hosts[ORIGIN].shared_leases
    await pool.release(ORIGIN, old)  # late release for the evicted conn
    assert pool.connection_count == count  # identity guard: no double decrement
    assert pool._hosts[ORIGIN].shared_leases == leases
    await pool.release(ORIGIN, new)
    await pool.close()


# ----- per-origin host entries -----


async def test_host_entries_pruned_on_keepalive_eviction(make_pool):
    origins = [Origin("http", f"origin-{i}.example", 80) for i in range(20)]
    pool = make_pool(FakeConnector(), max_keepalive_connections=1)
    for origin in origins:
        conn, _, _ = await pool.acquire(origin)
        await pool.release(origin, conn)
    assert pool.idle_count == 1
    assert len(pool._hosts) == 1  # only the origin holding the idle conn
    await pool.close()
    assert len(pool._hosts) == 0


async def test_host_entry_survives_eviction_while_leased():
    pool = stepping_pool(FakeConnector(), max_keepalive_connections=1)
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


async def test_host_entry_pruned_when_h2_redial_fails(make_pool):
    connector = FakeConnector(multiplexed=True)
    pool = make_pool(connector)
    conn, _, _ = await pool.acquire(ORIGIN)
    conn.closed = True
    connector.fail = 1
    with pytest.raises(punkreq.ConnectError):
        await pool.acquire(ORIGIN)
    assert len(pool._hosts) == 0
    assert pool.connection_count == 0
    await pool.close()


# ----- release verdicts -----


async def test_release_discard_drops_open_connection(make_pool):
    connector = FakeConnector()
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
    assert connector.dials == 2


async def test_release_of_busy_connection_does_not_park_it(make_pool):
    pool = make_pool(FakeConnector())
    conn, _, _ = await pool.acquire(ORIGIN)
    conn.busy = True  # release was interrupted; the exchange still holds the slot
    await pool.release(ORIGIN, conn)
    assert conn.closed
    assert pool.idle_count == 0
    await pool.close()


async def test_acquire_drops_idle_connection_turned_busy(make_pool):
    connector = FakeConnector()
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
    assert connector.dials == 2


# ----- limits and timeouts -----


async def test_max_connections_blocks_then_reuses_released(make_pool):
    connector = FakeConnector()
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
    assert connector.dials == 1


async def test_pool_timeout(make_pool):
    pool = make_pool(FakeConnector(), max_connections=1)
    conn, _, _ = await pool.acquire(ORIGIN)
    with pytest.raises(punkreq.PoolTimeout):
        await pool.acquire(ORIGIN, pool_timeout=0.02)
    await pool.release(ORIGIN, conn)
    await pool.close()


async def test_connect_timeout(make_pool):
    pool = make_pool(FakeConnector(delay=1.0))
    with pytest.raises(punkreq.ConnectTimeout):
        await pool.acquire(ORIGIN, connect_timeout=0.02)
    assert pool.connection_count == 0
    assert len(pool._hosts) == 0  # the failed origin's entry is pruned
    await pool.close()


async def test_dial_failure_propagates_and_pool_recovers(make_pool):
    connector = FakeConnector(fail=1)
    pool = make_pool(connector)
    with pytest.raises(punkreq.ConnectError):
        await pool.acquire(ORIGIN)
    assert pool.connection_count == 0
    assert len(pool._hosts) == 0  # the failed origin's entry is pruned
    conn, _, _ = await pool.acquire(ORIGIN)
    assert pool.connection_count == 1
    await pool.release(ORIGIN, conn)
    await pool.close()
    assert connector.dials == 2


async def test_acquire_after_close_raises(make_pool):
    pool = make_pool(FakeConnector())
    await pool.close()
    with pytest.raises(RuntimeError):
        await pool.acquire(ORIGIN)


# ----- capacity reclaim at max_connections: stale purge first, then LRU across
# idle h1 conns and zero-lease h2 shared conns; a request waits only while every
# counted connection is genuinely in flight -----


async def test_capacity_new_origin_reclaims_lru_idle():
    connector = FakeConnector()
    pool = stepping_pool(connector, max_connections=2)
    first, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, first)
    second, _, _ = await pool.acquire(OTHER_ORIGIN)
    await pool.release(OTHER_ORIGIN, second)
    # at cap, all idle: a third origin must not starve
    third, _, reused = await asyncio.wait_for(pool.acquire(THIRD_ORIGIN), 1.0)
    assert not reused
    assert first.closed  # LRU victim: parked earliest
    assert not second.closed
    assert pool.connection_count == 2
    await pool.release(THIRD_ORIGIN, third)
    await pool.close()
    assert connector.dials == 3


async def test_capacity_stale_reclaimed_before_live():
    pool = stepping_pool(FakeConnector(), max_connections=2)
    first, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, first)
    second, _, _ = await pool.acquire(OTHER_ORIGIN)
    await pool.release(OTHER_ORIGIN, second)
    second.closed = True  # dies while parked — NEWER than first
    await asyncio.wait_for(pool.acquire(THIRD_ORIGIN), 1.0)
    # the stale purge freed the slot; the live (older) conn survives
    assert not first.closed
    assert pool.connection_count == 2
    await pool.close()


async def test_capacity_expired_idle_of_other_origin_reclaimed(make_pool):
    pool = make_pool(FakeConnector(), max_connections=1, keepalive_expiry=0.01)
    first, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, first)
    await asyncio.sleep(0.03)
    second, _, _ = await asyncio.wait_for(pool.acquire(OTHER_ORIGIN), 1.0)
    assert first.closed
    assert pool.connection_count == 1
    await pool.release(OTHER_ORIGIN, second)
    await pool.close()


async def test_capacity_all_in_flight_still_waits(make_pool):
    pool = make_pool(FakeConnector(), max_connections=1)
    conn, _, _ = await pool.acquire(ORIGIN)  # leased, in flight
    with pytest.raises(punkreq.PoolTimeout):
        await pool.acquire(OTHER_ORIGIN, pool_timeout=0.02)
    await pool.release(ORIGIN, conn)
    await pool.close()


async def test_capacity_release_unblocks_waiting_origin(make_pool):
    connector = FakeConnector()
    pool = make_pool(connector, max_connections=1)
    conn, _, _ = await pool.acquire(ORIGIN)

    task = asyncio.ensure_future(pool.acquire(OTHER_ORIGIN))
    await asyncio.sleep(0.02)
    assert not task.done()  # blocked: the only conn is in flight
    await pool.release(ORIGIN, conn)  # parked idle -> reclaimable
    other, _, _ = await asyncio.wait_for(task, 1.0)
    assert conn.closed  # reclaimed for the new origin
    assert pool.connection_count == 1
    await pool.release(OTHER_ORIGIN, other)
    await pool.close()
    assert connector.dials == 2


async def test_capacity_idle_shared_h2_reclaimed(make_pool):
    async def connector(origin):
        return FakeConnection(multiplexed=(origin == ORIGIN))

    pool = make_pool(connector, max_connections=1)
    shared, exclusive, _ = await pool.acquire(ORIGIN)
    assert not exclusive
    await pool.release(ORIGIN, shared)  # zero leases -> reclaimable
    other, _, _ = await asyncio.wait_for(pool.acquire(OTHER_ORIGIN), 1.0)
    assert shared.closed
    assert pool.connection_count == 1
    await pool.release(OTHER_ORIGIN, other)
    await pool.close()


async def test_capacity_leased_shared_h2_not_reclaimed(make_pool):
    async def connector(origin):
        return FakeConnection(multiplexed=(origin == ORIGIN))

    pool = make_pool(connector, max_connections=1)
    shared, _, _ = await pool.acquire(ORIGIN)  # lease held, no release
    with pytest.raises(punkreq.PoolTimeout):
        await pool.acquire(OTHER_ORIGIN, pool_timeout=0.02)
    await pool.release(ORIGIN, shared)
    await pool.close()


async def test_capacity_lru_is_protocol_blind():
    # the h2 conn went idle BEFORE the h1 conn was parked: the h2 conn is
    # the victim — eviction never depends on the negotiated protocol
    async def connector(origin):
        return FakeConnection(multiplexed=(origin == ORIGIN))

    pool = stepping_pool(connector, max_connections=2)
    shared, _, _ = await pool.acquire(ORIGIN)
    await pool.release(ORIGIN, shared)  # h2 idle-since: t1
    h1conn, _, _ = await pool.acquire(OTHER_ORIGIN)
    await pool.release(OTHER_ORIGIN, h1conn)  # h1 parked-since: t2 > t1
    await asyncio.wait_for(pool.acquire(THIRD_ORIGIN), 1.0)
    assert shared.closed
    assert not h1conn.closed
    await pool.close()
