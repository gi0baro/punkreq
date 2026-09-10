"""The end-to-end scenarios on the asyncio backend, against the asyncio peers
from `conftest` (`echo_server`, `proxy`)."""

import pytest

from punkreq.asyncio import Client
from tests import e2e
from tests.servers import server_tls_context


pytestmark = pytest.mark.asyncio


async def test_client_h1_get_and_post_with_keepalive(echo_server):
    await e2e.client_h1_get_and_post_with_keepalive(Client, await echo_server())


async def test_client_h2_prior_knowledge(echo_server):
    await e2e.client_h2_prior_knowledge(Client, await echo_server())


async def test_tunnel_https_h1(ca, echo_server, proxy):
    upstream_port = await echo_server(server_tls_context(ca, ["http/1.1"]))
    await e2e.tunnel_https_h1(Client, ca, upstream_port, await proxy())


async def test_tunnel_https_h2(ca, echo_server, proxy):
    upstream_port = await echo_server(server_tls_context(ca, ["h2"]))
    await e2e.tunnel_https_h2(Client, ca, upstream_port, await proxy())


async def test_tunnel_handshake_failure_raises_connect_error(ca, echo_server, proxy):
    upstream_port = await echo_server(server_tls_context(ca, ["http/1.1"]))
    await e2e.tunnel_handshake_failure_raises_connect_error(Client, upstream_port, await proxy())


async def test_direct_handshake_failure_raises_connect_error(ca, echo_server):
    upstream_port = await echo_server(server_tls_context(ca, ["http/1.1"]))
    await e2e.direct_handshake_failure_raises_connect_error(Client, upstream_port)


async def test_tunnel_refused_raises_proxy_error(proxy):
    await e2e.tunnel_refused_raises_proxy_error(Client, await proxy(connect_status=403))
