"""End-to-end scenarios shared by the asyncio and tonio suites: each takes the
backend's `Client` class and the peers the suite started, so both backends run
the same body against their own runtime."""

import pytest

import punkreq


async def client_h1_get_and_post_with_keepalive(client_cls, upstream_port):
    async with client_cls(base_url=f"http://127.0.0.1:{upstream_port}") as client:
        response = await client.get("/hello", params={"a": "1"})
        assert response.status_code == 200
        echo = await response.json()
        assert echo["method"] == "GET"
        assert echo["path"] == "/hello?a=1"
        assert echo["headers"]["host"] == f"127.0.0.1:{upstream_port}"
        assert echo["headers"]["user-agent"].startswith("python-punkreq/")

        response = await client.post("/items", json={"name": "x"})
        assert (await response.json())["body"] == '{"name":"x"}'
        assert client._pool.connection_count == 1  # keep-alive reuse


async def client_h2_prior_knowledge(client_cls, upstream_port):
    async with client_cls(base_url=f"http://127.0.0.1:{upstream_port}", http1=False) as client:
        first = await client.get("/a")
        second = await client.get("/b")
        assert first.http_version == "HTTP/2"
        assert {(await first.json())["path"], (await second.json())["path"]} == {"/a", "/b"}
        assert client._pool.connection_count == 1  # one multiplexed connection


async def tunnel_https_h1(client_cls, ca, upstream_port, proxy):
    with ca.cert_pem.tempfile() as ca_path:
        async with client_cls(proxy=f"http://127.0.0.1:{proxy.port}", verify=ca_path) as client:
            response = await client.get(f"https://127.0.0.1:{upstream_port}/tunneled")
            assert response.status_code == 200
            assert (await response.json())["path"] == "/tunneled"
            assert response.http_version == "HTTP/1.1"
            # a second request reuses the tunnel: the h1 send-time peek runs on the wrapped stream
            response = await client.get(f"https://127.0.0.1:{upstream_port}/again")
            assert (await response.json())["path"] == "/again"
            assert client._pool.connection_count == 1
    assert proxy.connect_targets == [f"127.0.0.1:{upstream_port}"]


async def tunnel_https_h2(client_cls, ca, upstream_port, proxy):
    with ca.cert_pem.tempfile() as ca_path:
        async with client_cls(proxy=f"http://127.0.0.1:{proxy.port}", verify=ca_path) as client:
            first = await client.get(f"https://127.0.0.1:{upstream_port}/a")
            second = await client.get(f"https://127.0.0.1:{upstream_port}/b")
            assert first.http_version == "HTTP/2"
            assert {(await first.json())["path"], (await second.json())["path"]} == {"/a", "/b"}
            assert client._pool.connection_count == 1  # one multiplexed tunnel


async def tunnel_handshake_failure_raises_connect_error(client_cls, upstream_port, proxy):
    """The origin's certificate is not trusted (system store): the backend's TLS
    layer fails the handshake inside the tunnel and closes it; punkreq reports it
    as a connect failure, like a failed direct dial."""
    async with client_cls(proxy=f"http://127.0.0.1:{proxy.port}") as client:
        with pytest.raises(punkreq.ConnectError, match="via proxy"):
            await client.get(f"https://127.0.0.1:{upstream_port}/untrusted")
    assert proxy.connect_targets == [f"127.0.0.1:{upstream_port}"]


async def direct_handshake_failure_raises_connect_error(client_cls, upstream_port):
    """The same on a direct dial: whatever the backend raises for a failed
    handshake surfaces as punkreq's `ConnectError`."""
    async with client_cls() as client:
        with pytest.raises(punkreq.ConnectError):
            await client.get(f"https://127.0.0.1:{upstream_port}/untrusted")


async def tunnel_refused_raises_proxy_error(client_cls, proxy):
    async with client_cls(proxy=f"http://127.0.0.1:{proxy.port}") as client:
        with pytest.raises(punkreq.ProxyError):
            await client.get("https://denied.example/")
