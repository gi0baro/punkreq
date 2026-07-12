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
