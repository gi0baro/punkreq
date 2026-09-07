import asyncio
import contextlib
import json
import ssl

import httpunk.asyncio
import pytest
import trustme
from httpunk import Backend

import punkreq
from punkreq import Proxy
from punkreq._connect import create_ssl_context
from punkreq._proxies import ProxyConfig, TunnelTLSStream
from punkreq.asyncio import Client


@pytest.fixture(scope="module")
def ca():
    return trustme.CA()


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=15))


class _EchoJson(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        body = await request.read()
        payload = {"method": request.method, "path": request.path, "body": body.decode("utf-8", "replace")}
        await request.respond(200, headers={"content-type": "application/json"}, body=json.dumps(payload).encode())


async def _start_upstream(ssl_context=None):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(_EchoJson, "127.0.0.1", 0, ssl=ssl_context)
    return server, server.sockets[0].getsockname()[1]


def _upstream_tls(ca, alpn):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost", "127.0.0.1").configure_cert(context)
    context.set_alpn_protocols(alpn)
    return context


class RawProxy:
    """A minimal HTTP proxy on asyncio streams: answers absolute-form requests
    itself and relays CONNECT tunnels to the requested destination."""

    def __init__(self, connect_status=200):
        self.connect_status = connect_status
        self.heads: list[bytes] = []
        self.connect_targets: list[str] = []
        self.server = None
        self.port = None
        self._writers: list = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        for writer in self._writers:
            with contextlib.suppress(Exception):
                writer.close()
        self.server.close()
        await self.server.wait_closed()

    async def _handle(self, reader, writer):
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = await reader.read(65536)
            if not chunk:
                writer.close()
                return
            head += chunk
        head, _, leftover = head.partition(b"\r\n\r\n")
        self.heads.append(head)
        method, target, _ = head.split(b"\r\n")[0].decode().split(" ")

        if method == "CONNECT":
            self.connect_targets.append(target)
            if self.connect_status != 200:
                writer.write(f"HTTP/1.1 {self.connect_status} Denied\r\ncontent-length: 0\r\n\r\n".encode())
                await writer.drain()
                writer.close()
                return
            host, _, port = target.rpartition(":")
            upstream_reader, upstream_writer = await asyncio.open_connection(host.strip("[]"), int(port))
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            if leftover:
                upstream_writer.write(leftover)
                await upstream_writer.drain()

            async def pipe(src, dst):
                try:
                    while data := await src.read(65536):
                        dst.write(data)
                        await dst.drain()
                except Exception:
                    pass
                finally:
                    with contextlib.suppress(Exception):
                        dst.close()

            await asyncio.gather(pipe(reader, upstream_writer), pipe(upstream_reader, writer))
        else:
            body = b"proxied:" + target.encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: %d\r\n\r\n%s" % (len(body), body)
            )
            await writer.drain()
            self._writers.append(writer)  # kept open for keep-alive; closed by stop()


class TestProxyConfig:
    def test_disabled(self, monkeypatch):
        monkeypatch.setenv("HTTP_PROXY", "http://should-not-matter:1")
        assert ProxyConfig.resolve(None, trust_env=False) is None

    def test_explicit_proxy_matches_everything(self):
        config = ProxyConfig.resolve("http://proxy.example:3128", trust_env=False)
        intercept = config.matcher.intercept("https://anything.example/")
        assert intercept is not None
        assert intercept.uri == "http://proxy.example:3128/"

    def test_explicit_proxy_auth_embedded(self):
        config = ProxyConfig.resolve(Proxy("http://proxy.example", auth=("user", "pass")), trust_env=False)
        intercept = config.matcher.intercept("http://anything.example/")
        assert intercept.basic_auth() == "Basic dXNlcjpwYXNz"

    def test_env_matcher(self, monkeypatch):
        for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"):
            monkeypatch.delenv(name, raising=False)
            monkeypatch.delenv(name.lower(), raising=False)
        config = ProxyConfig.resolve(None, trust_env=True)
        assert config.matcher.intercept("http://anything.example/") is None

        monkeypatch.setenv("HTTP_PROXY", "http://envproxy:8080")
        monkeypatch.setenv("NO_PROXY", "internal.example")
        config = ProxyConfig.resolve(None, trust_env=True)
        assert config.matcher.intercept("http://external.example/").uri == "http://envproxy:8080/"
        assert config.matcher.intercept("http://internal.example/") is None
        assert config.matcher.intercept("https://external.example/") is None  # HTTP_PROXY only covers http


class TestPlainHTTPProxy:
    def test_absolute_form_with_auth_and_headers(self):
        async def main():
            proxy = await RawProxy().start()
            async with Client(
                proxy=Proxy(f"http://127.0.0.1:{proxy.port}", auth=("u", "p"), headers={"x-proxy-extra": "1"})
            ) as client:
                response = await client.get("http://upstream.invalid/some/path?q=1")
                assert response.status_code == 200
                assert await response.text() == "proxied:http://upstream.invalid/some/path?q=1"
            await proxy.stop()
            head = proxy.heads[0]
            assert head.startswith(b"GET http://upstream.invalid/some/path?q=1 HTTP/1.1")
            assert b"proxy-authorization: Basic dTpw" in head
            assert b"x-proxy-extra: 1" in head
            assert b"host: upstream.invalid" in head

        run(main())


class TestTunnelTLSStream:
    """The tunnel stream as each httpunk backend sees a TLS transport: the
    send-time peek (`receive_nowait`, httpunk >= 0.3.0: `None` = nothing ready,
    `b""` = EOF, else bytes) and the sync `close_transport`."""

    @staticmethod
    async def _open(ca, upstream_port):
        backend = Backend.asyncio.create()
        raw = await backend.connect_tcp("127.0.0.1", upstream_port)
        closes = []
        with ca.cert_pem.tempfile() as ca_path:
            context = create_ssl_context(verify=ca_path)
        context.set_alpn_protocols(["http/1.1"])
        tls = TunnelTLSStream(raw, context, server_hostname="127.0.0.1", closer=lambda: closes.append(1))
        await tls.handshake()
        return backend, raw, tls, closes

    def test_peek_contract_and_close(self, ca):
        async def main():
            upstream, upstream_port = await _start_upstream(_upstream_tls(ca, ["http/1.1"]))
            backend, raw, tls, closes = await self._open(ca, upstream_port)
            assert tls.selected_alpn_protocol() == "http/1.1"
            # nothing decrypted yet: "not ready", NOT the EOF that `b""` now means
            assert tls.read_nowait() is None
            assert backend.receive_nowait(tls) is None

            await tls.send_all(b"GET /peek HTTP/1.1\r\nhost: x\r\n\r\n")
            first = await tls.receive_some(1)  # decrypts the record: the rest is pending
            assert first == b"H"
            rest = tls.read_nowait()
            assert rest and rest.startswith(b"TTP/1.1 200")

            backend.close_transport(tls)
            backend.close_transport(tls)  # idempotent
            assert closes == [1]
            raw.close()
            upstream.close()
            await upstream.wait_closed()

        run(main())

    def test_tonio_backend_shape(self, ca):
        """The tonio backend finds a TLS transport by `_ssl` and reaches into it as
        into its own `_SSLProxy` (`_lock`, `_inner`); it closes through `.transport`."""
        pytest.importorskip("tonio")
        tonio_backend = Backend.tonio.create()

        async def main():
            upstream, upstream_port = await _start_upstream(_upstream_tls(ca, ["http/1.1"]))
            _, raw, tls, closes = await self._open(ca, upstream_port)
            assert tonio_backend.receive_nowait(tls) is None
            await tls.send_all(b"GET /peek HTTP/1.1\r\nhost: x\r\n\r\n")
            assert await tls.receive_some(1) == b"H"
            pending = tonio_backend.receive_nowait(tls)
            assert pending and pending.startswith(b"TTP/1.1 200")
            tonio_backend.close_transport(tls)
            assert closes == [1]
            raw.close()
            upstream.close()
            await upstream.wait_closed()

        run(main())


class TestConnectTunnel:
    def test_https_via_connect_h1(self, ca):
        async def main():
            upstream, upstream_port = await _start_upstream(_upstream_tls(ca, ["http/1.1"]))
            proxy = await RawProxy().start()
            with ca.cert_pem.tempfile() as ca_path:
                client = Client(proxy=f"http://127.0.0.1:{proxy.port}", verify=ca_path)
            async with client:
                response = await client.get(f"https://127.0.0.1:{upstream_port}/tunneled")
                assert response.status_code == 200
                assert (await response.json())["path"] == "/tunneled"
                assert response.http_version == "HTTP/1.1"
            assert proxy.connect_targets == [f"127.0.0.1:{upstream_port}"]
            await proxy.stop()
            upstream.close()
            await upstream.wait_closed()

        run(main())

    def test_https_via_connect_h2(self, ca):
        async def main():
            upstream, upstream_port = await _start_upstream(_upstream_tls(ca, ["h2"]))
            proxy = await RawProxy().start()
            with ca.cert_pem.tempfile() as ca_path:
                client = Client(proxy=f"http://127.0.0.1:{proxy.port}", verify=ca_path)
            async with client:
                first, second = await asyncio.gather(
                    client.get(f"https://127.0.0.1:{upstream_port}/a"),
                    client.get(f"https://127.0.0.1:{upstream_port}/b"),
                )
                assert first.http_version == "HTTP/2"
                paths = {(await first.json())["path"], (await second.json())["path"]}
                assert paths == {"/a", "/b"}
                assert client._pool.connection_count == 1  # one multiplexed tunnel
            await proxy.stop()
            upstream.close()
            await upstream.wait_closed()

        run(main())

    def test_connect_post_roundtrip(self, ca):
        async def main():
            upstream, upstream_port = await _start_upstream(_upstream_tls(ca, ["http/1.1"]))
            proxy = await RawProxy().start()
            with ca.cert_pem.tempfile() as ca_path:
                client = Client(proxy=f"http://127.0.0.1:{proxy.port}", verify=ca_path)
            async with client:
                response = await client.post(f"https://127.0.0.1:{upstream_port}/items", json={"n": 1})
                assert (await response.json())["body"] == '{"n":1}'
            await proxy.stop()
            upstream.close()
            await upstream.wait_closed()

        run(main())

    def test_connect_refused_raises_proxy_error(self):
        async def main():
            proxy = await RawProxy(connect_status=403).start()
            async with Client(proxy=f"http://127.0.0.1:{proxy.port}") as client:
                with pytest.raises(punkreq.ProxyError):
                    await client.get("https://denied.example/")
            await proxy.stop()

        run(main())


class TestNoProxyBypass:
    def test_env_no_proxy_goes_direct(self, monkeypatch):
        async def main():
            upstream, upstream_port = await _start_upstream()
            proxy = await RawProxy().start()
            monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy.port}")
            monkeypatch.setenv("NO_PROXY", "127.0.0.1")
            async with Client() as client:
                response = await client.get(f"http://127.0.0.1:{upstream_port}/direct")
                assert (await response.json())["path"] == "/direct"  # served by upstream, not the proxy
            assert proxy.heads == []
            await proxy.stop()
            upstream.close()
            await upstream.wait_closed()

        run(main())

    def test_env_proxy_used_without_no_proxy(self, monkeypatch):
        async def main():
            proxy = await RawProxy().start()
            monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy.port}")
            monkeypatch.delenv("NO_PROXY", raising=False)
            async with Client() as client:
                response = await client.get("http://upstream.invalid/via-env")
                assert await response.text() == "proxied:http://upstream.invalid/via-env"
            assert len(proxy.heads) == 1
            await proxy.stop()

        run(main())
