import pytest
import pytest_asyncio
import trustme
from httpunk import Backend

from punkreq import Limits
from punkreq._pool import ConnectionPool
from punkreq.asyncio import Client
from tests.fakes import FakeConnector
from tests.servers import RawProxy, start_server


@pytest.fixture(scope="session")
def ca():
    """A throwaway certificate authority for TLS peers."""
    return trustme.CA()


@pytest.fixture
def make_client():
    """`(client, connector)`: a Client over a fake connector whose connections
    answer every request from `handler(request, conn)`."""

    def factory(handler, *, multiplexed=False, **kwargs):
        connector = FakeConnector(handler, multiplexed=multiplexed)
        return Client(connector=connector, **kwargs), connector

    return factory


@pytest.fixture
def make_pool():
    """A ConnectionPool over `connector` on the asyncio backend (or `backend`),
    with `Limits(**limits)`."""

    def factory(connector, *, backend=None, **limits):
        return ConnectionPool(connector, backend=backend or Backend.asyncio.create(), limits=Limits(**limits))

    return factory


# ----- real peers on the asyncio test's own loop (`@pytest.mark.asyncio` tests only) -----


@pytest_asyncio.fixture
async def echo_server():
    """`await echo_server(ssl_context=None) -> port`: a JSON echo server on this
    test's loop; every server started is closed at teardown."""
    servers = []

    async def start(ssl_context=None):
        server, port = await start_server(ssl_context=ssl_context)
        servers.append(server)
        return port

    yield start
    for server in servers:
        server.close()
        if hasattr(server, "close_clients"):  # 3.13+: `wait_closed` waits for clients too
            server.close_clients()
        await server.wait_closed()


@pytest_asyncio.fixture
async def proxy():
    """`await proxy(connect_status=200) -> RawProxy`: a CONNECT proxy on this
    test's loop; every proxy started is stopped at teardown."""
    proxies = []

    async def start(connect_status=200):
        started = await RawProxy(connect_status).start()
        proxies.append(started)
        return started

    yield start
    for started in proxies:
        await started.stop()
