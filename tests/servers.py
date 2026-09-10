"""Real peers for the end-to-end tests: an httpunk echo server on asyncio, its
TLS context, and a minimal CONNECT proxy."""

import asyncio
import contextlib
import json
import ssl

import httpunk.asyncio


class EchoJsonProtocol(httpunk.asyncio.AutoServerProtocol):
    """Answers every request with a JSON echo of its method, path, headers and body."""

    async def handle(self, request):
        body = await request.read()
        payload = {
            "method": request.method,
            "path": request.path,
            "headers": {key: value.decode("latin-1") for key, value in request.headers.items()},
            "body": body.decode("utf-8", errors="replace"),
        }
        await request.respond(200, headers={"content-type": "application/json"}, body=json.dumps(payload).encode())


async def start_server(protocol=EchoJsonProtocol, ssl_context=None):
    """`(server, port)` for `protocol` listening on a free loopback port."""
    loop = asyncio.get_running_loop()
    server = await loop.create_server(protocol, "127.0.0.1", 0, ssl=ssl_context)
    return server, server.sockets[0].getsockname()[1]


def server_tls_context(ca, alpn):
    """A server-side TLS context with a `ca`-issued loopback certificate offering `alpn`."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert("localhost", "127.0.0.1").configure_cert(context)
    context.set_alpn_protocols(alpn)
    return context


class RawProxy:
    """A minimal HTTP proxy on asyncio streams that relays CONNECT tunnels to the
    requested destination (or refuses them with `connect_status`)."""

    def __init__(self, connect_status=200):
        self.connect_status = connect_status
        self.connect_targets: list[str] = []
        self.server = None
        self.port = None

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
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
        method, target, _ = head.split(b"\r\n")[0].decode().split(" ")
        if method != "CONNECT":
            writer.write(b"HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return

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
