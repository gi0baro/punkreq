"""The end-to-end scenarios on the tonio backend, against tonio peers from
`tests.servers_tonio`, one set per test, run on the plugin's runtime. This is
where the backend-specific surface gets exercised: `connect_tls`, the CONNECT
tunnel over `wrap_tls`, and tonio's own failure exceptions mapping to punkreq's."""

import sys

import pytest

from tests import e2e
from tests.servers import server_tls_context


pytest.importorskip("tonio")

from punkreq.tonio import Client
from tests.servers_tonio import TonioEchoServer, TonioRawProxy


pytestmark = pytest.mark.tonio


@pytest.fixture(scope="session", autouse=True)
def _needs_free_threading():
    # tonio refuses to start a runtime once the GIL is on, which a GIL-only
    # extension imported earlier in the session can force even on a free-threaded
    # build. Session-scoped so it runs before the plugin's session-scoped
    # `tonio_runtime` fixture, which is where the runtime starts.
    if getattr(sys, "_is_gil_enabled", lambda: True)():
        pytest.skip("tonio needs the GIL disabled")


@pytest.fixture
async def tonio_echo_server():
    """`await tonio_echo_server(ssl_context=None) -> port`: a JSON echo server on
    the tonio runtime; every server started is stopped at teardown."""
    servers = []

    async def start(ssl_context=None):
        server = await TonioEchoServer(ssl_context).start()
        servers.append(server)
        return server.port

    yield start
    for server in servers:
        await server.stop()


@pytest.fixture
async def tonio_proxy():
    """`await tonio_proxy(connect_status=200) -> TonioRawProxy`: a CONNECT proxy
    on the tonio runtime; every proxy started is stopped at teardown."""
    proxies = []

    async def start(connect_status=200):
        started = await TonioRawProxy(connect_status).start()
        proxies.append(started)
        return started

    yield start
    for started in proxies:
        await started.stop()


async def test_client_h1_get_and_post_with_keepalive(tonio_echo_server):
    await e2e.client_h1_get_and_post_with_keepalive(Client, await tonio_echo_server())


async def test_client_h2_prior_knowledge(tonio_echo_server):
    await e2e.client_h2_prior_knowledge(Client, await tonio_echo_server())


async def test_tunnel_https_h1(ca, tonio_echo_server, tonio_proxy):
    upstream_port = await tonio_echo_server(server_tls_context(ca, ["http/1.1"]))
    await e2e.tunnel_https_h1(Client, ca, upstream_port, await tonio_proxy())


async def test_tunnel_https_h2(ca, tonio_echo_server, tonio_proxy):
    upstream_port = await tonio_echo_server(server_tls_context(ca, ["h2"]))
    await e2e.tunnel_https_h2(Client, ca, upstream_port, await tonio_proxy())


async def test_tunnel_handshake_failure_raises_connect_error(ca, tonio_echo_server, tonio_proxy):
    upstream_port = await tonio_echo_server(server_tls_context(ca, ["http/1.1"]))
    await e2e.tunnel_handshake_failure_raises_connect_error(Client, upstream_port, await tonio_proxy())


async def test_direct_handshake_failure_raises_connect_error(ca, tonio_echo_server):
    upstream_port = await tonio_echo_server(server_tls_context(ca, ["http/1.1"]))
    await e2e.direct_handshake_failure_raises_connect_error(Client, upstream_port)


async def test_tunnel_refused_raises_proxy_error(tonio_proxy):
    await e2e.tunnel_refused_raises_proxy_error(Client, await tonio_proxy(connect_status=403))
