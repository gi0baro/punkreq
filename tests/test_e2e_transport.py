import asyncio
import ssl

import httpunk.asyncio
import pytest
import trustme
from httpunk import Backend
from httpunk.h1.client import H1Connection
from httpunk.h2.client import H2Connection

from punkreq._connect import Connector, Origin, create_ssl_context
from punkreq._pool import ConnectionPool


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


class _Echo(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        await request.read()
        await request.respond(200, headers={"content-type": "text/plain"}, body=b"ok:" + request.path.encode())


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=15))


async def _start_server(ssl_context=None):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(_Echo, "127.0.0.1", 0, ssl=ssl_context)
    port = server.sockets[0].getsockname()[1]
    return server, port


def _server_ssl(ca, alpn):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost", "127.0.0.1").configure_cert(context)
    context.set_alpn_protocols(alpn)
    return context


def _client_ssl(ca):
    # route through create_ssl_context(verify=<path>) so the CA-bundle branch is exercised
    with ca.cert_pem.tempfile() as ca_path:
        return create_ssl_context(verify=ca_path)


class _RecordingBackend:
    """The asyncio backend with `connect_tls` recorded and answered with an inert
    stream (the connection is never entered)."""

    def __init__(self, selected):
        self._inner = Backend.asyncio.create()
        self.selected = selected
        self.calls = []

    async def connect_tls(self, host, port, *, alpn=None, ssl_context=None):
        self.calls.append((alpn, ssl_context))
        return object(), self.selected

    def __getattr__(self, name):
        return getattr(self._inner, name)


class TestConnectorALPN:
    """The ALPN offer lives on the SSL context, set once at construction: a
    caller-supplied context is never mutated by a dial (two concurrent dials
    configuring it would negotiate each other's offer); a backend-created default
    context (no context given) is configured per dial."""

    @pytest.mark.parametrize(
        ("http1", "http2", "offer"),
        [(True, True, ("h2", "http/1.1")), (False, True, ("h2",)), (True, False, ("http/1.1",))],
    )
    def test_offer_from_flags(self, http1, http2, offer):
        backend = _RecordingBackend("h2" if http2 else "http/1.1")
        context = ssl.create_default_context()
        connector = Connector(backend=backend, ssl_context=context, http1=http1, http2=http2)
        assert connector.alpn == offer
        run(connector(Origin("https", "example.com", 443)))
        assert backend.calls == [(None, context)]  # already on the context: no per-dial offer

    def test_default_context_configured_per_dial(self):
        backend = _RecordingBackend("h2")
        connector = Connector(backend=backend)
        run(connector(Origin("https", "example.com", 443)))
        assert backend.calls == [(("h2", "http/1.1"), None)]


class TestPlainTCP:
    def test_h1_roundtrip_and_keepalive_reuse(self):
        async def main():
            server, port = await _start_server()
            backend = Backend.asyncio.create()
            connector = Connector(backend=backend)
            origin = Origin("http", "127.0.0.1", port)
            async with ConnectionPool(connector, backend=backend) as pool:
                conn, exclusive, _ = await pool.acquire(origin)
                assert isinstance(conn, H1Connection)
                assert exclusive
                response = await conn.request("GET", "/first", headers={"host": origin.authority})
                assert response.status == 200
                assert await response.read() == b"ok:/first"
                await pool.release(origin, conn)

                conn2, _, _ = await pool.acquire(origin)
                assert conn2 is conn  # keep-alive reuse over a real socket
                response = await conn2.request("GET", "/second", headers={"host": origin.authority})
                assert await response.read() == b"ok:/second"
                await pool.release(origin, conn2)
                assert pool.connection_count == 1
            server.close()
            await server.wait_closed()

        run(main())

    def test_h2_prior_knowledge(self):
        async def main():
            server, port = await _start_server()
            backend = Backend.asyncio.create()
            connector = Connector(backend=backend, http1=False)
            origin = Origin("http", "127.0.0.1", port)
            async with ConnectionPool(connector, backend=backend) as pool:
                conn, exclusive, _ = await pool.acquire(origin)
                assert isinstance(conn, H2Connection)
                assert not exclusive
                response = await conn.request("GET", "/h2")
                assert response.status == 200
                assert await response.read() == b"ok:/h2"
                # concurrent requests multiplex on the same shared connection
                conn2, _, _ = await pool.acquire(origin)
                assert conn2 is conn
            server.close()
            await server.wait_closed()

        run(main())


class TestTLSALPN:
    def test_alpn_negotiates_h2(self, ca):
        async def main():
            server, port = await _start_server(_server_ssl(ca, ["h2", "http/1.1"]))
            backend = Backend.asyncio.create()
            connector = Connector(backend=backend, ssl_context=_client_ssl(ca))
            origin = Origin("https", "127.0.0.1", port)
            async with ConnectionPool(connector, backend=backend) as pool:
                conn, exclusive, _ = await pool.acquire(origin)
                assert isinstance(conn, H2Connection)
                assert not exclusive
                response = await conn.request("GET", "/tls")
                assert response.status == 200
                assert await response.read() == b"ok:/tls"
            server.close()
            await server.wait_closed()

        run(main())

    def test_alpn_falls_back_to_h1(self, ca):
        async def main():
            server, port = await _start_server(_server_ssl(ca, ["http/1.1"]))
            backend = Backend.asyncio.create()
            connector = Connector(backend=backend, ssl_context=_client_ssl(ca))
            origin = Origin("https", "127.0.0.1", port)
            async with ConnectionPool(connector, backend=backend) as pool:
                conn, exclusive, _ = await pool.acquire(origin)
                assert isinstance(conn, H1Connection)
                assert exclusive
                response = await conn.request("GET", "/tls1", headers={"host": origin.authority})
                assert response.status == 200
                assert await response.read() == b"ok:/tls1"
                await pool.release(origin, conn)
            server.close()
            await server.wait_closed()

        run(main())

    def test_http2_disabled_pins_h1(self, ca):
        async def main():
            server, port = await _start_server(_server_ssl(ca, ["h2", "http/1.1"]))
            backend = Backend.asyncio.create()
            connector = Connector(backend=backend, ssl_context=_client_ssl(ca), http2=False)
            origin = Origin("https", "127.0.0.1", port)
            async with ConnectionPool(connector, backend=backend) as pool:
                conn, _, _ = await pool.acquire(origin)
                assert isinstance(conn, H1Connection)
                await pool.release(origin, conn)
            server.close()
            await server.wait_closed()

        run(main())


class TestConnectErrors:
    def test_refused_connection_maps_to_connect_error(self):
        import punkreq

        async def main():
            backend = Backend.asyncio.create()
            connector = Connector(backend=backend)
            # dial a port nothing is listening on
            origin = Origin("http", "127.0.0.1", 1)
            async with ConnectionPool(connector, backend=backend) as pool:
                try:
                    await pool.acquire(origin)
                except punkreq.ConnectError:
                    return
                raise AssertionError("expected ConnectError")

        run(main())
