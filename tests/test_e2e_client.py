import asyncio
import json

import httpunk.asyncio

from punkreq.asyncio import Client


class _EchoJson(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        body = await request.read()
        payload = {
            "method": request.method,
            "path": request.path,
            "headers": {key: value.decode("latin-1") for key, value in request.headers.items()},
            "body": body.decode("utf-8", errors="replace"),
        }
        await request.respond(200, headers={"content-type": "application/json"}, body=json.dumps(payload).encode())


class _Redirector(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        await request.read()
        if request.path == "/start":
            await request.respond(302, headers={"location": "/finish", "set-cookie": "hop=yes; Path=/"})
        else:
            cookie = request.headers.get("cookie", b"").decode()
            await request.respond(200, headers={"content-type": "text/plain"}, body=f"done:{cookie}".encode())


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=15))


async def _start(protocol):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(protocol, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


class TestClientE2E:
    def test_h1_get_and_post_with_keepalive(self):
        async def main():
            server, port = await _start(_EchoJson)
            async with Client(base_url=f"http://127.0.0.1:{port}") as client:
                response = await client.get("/hello", params={"a": "1"})
                assert response.status_code == 200
                echo = await response.json()
                assert echo["method"] == "GET"
                assert echo["path"] == "/hello?a=1"
                assert echo["headers"]["host"] == f"127.0.0.1:{port}"
                assert echo["headers"]["user-agent"].startswith("python-punkreq/")

                response = await client.post("/items", json={"name": "x"})
                assert (await response.json())["body"] == '{"name":"x"}'
                assert client._pool.connection_count == 1  # keep-alive reuse

            server.close()
            await server.wait_closed()

        run(main())

    def test_h2_prior_knowledge_client(self):
        async def main():
            server, port = await _start(_EchoJson)
            async with Client(base_url=f"http://127.0.0.1:{port}", http1=False) as client:
                first, second = await asyncio.gather(client.get("/a"), client.get("/b"))
                assert first.http_version == "HTTP/2"
                paths = {(await first.json())["path"], (await second.json())["path"]}
                assert paths == {"/a", "/b"}
                assert client._pool.connection_count == 1  # multiplexed

            server.close()
            await server.wait_closed()

        run(main())

    def test_redirect_with_cookie_jar_e2e(self):
        async def main():
            server, port = await _start(_Redirector)
            async with Client(base_url=f"http://127.0.0.1:{port}", cookies={}) as client:
                response = await client.get("/start")
                assert response.status_code == 200
                assert await response.text() == "done:hop=yes"
                assert [r.status_code for r in response.history] == [302]

            server.close()
            await server.wait_closed()

        run(main())

    def test_streaming_download(self):
        async def main():
            server, port = await _start(_EchoJson)
            async with Client() as client:
                async with client.get(f"http://127.0.0.1:{port}/stream") as response:
                    assert response.status_code == 200
                    body = b"".join([chunk async for chunk in response.iter_bytes()])
                    assert json.loads(body)["path"] == "/stream"
                # connection released back after the block
                assert client._pool.idle_count == 1

            server.close()
            await server.wait_closed()

        run(main())

    def test_module_level_api(self):
        from punkreq import asyncio as punkreq_asyncio

        async def main():
            server, port = await _start(_EchoJson)
            response = await punkreq_asyncio.get(f"http://127.0.0.1:{port}/top-level")
            assert response.status_code == 200
            assert (await response.json())["path"] == "/top-level"
            # the throwaway client closed itself once the body was read
            async with punkreq_asyncio.get(f"http://127.0.0.1:{port}/as-cm") as response:
                assert (await response.json())["path"] == "/as-cm"
            server.close()
            await server.wait_closed()

        run(main())
