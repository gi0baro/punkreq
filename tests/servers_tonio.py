"""Real peers for the tonio end-to-end tests: an httpunk echo server and a
CONNECT proxy on tonio's own streams. Each runs its accept loop in its own
scope and its connections as untracked tasks, so they stay alive across the
pytest plugin's separate `run_until_complete` calls; `stop()` closes what they
listen and read on, then cancels and exits the scope."""

import json

import tonio.colored as tonio
from httpunk import Backend
from httpunk.util import auto
from tonio.colored.net import open_tcp_listeners, open_tcp_stream
from tonio.colored.net.tls import open_tls_over_tcp_listeners


_backend = Backend.tonio.create()  # for its sync, abortive `close_transport`


async def read_head(stream):
    """`(head, leftover)` up to the first blank line, or `(b"", b"")` at EOF."""
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = await stream.receive_some(65536)
        if not chunk:
            return b"", b""
        buf += chunk
    head, _, leftover = buf.partition(b"\r\n\r\n")
    return head, leftover


class TonioEchoServer:
    """The JSON echo upstream on tonio: one accept-loop task, one task per connection."""

    def __init__(self, ssl_context=None):
        self._ssl_context = ssl_context
        self._listener = None
        self._scope = tonio.scope()
        self._streams = []  # every accepted transport, for the abortive close at stop()
        self.port = None

    async def start(self):
        if self._ssl_context is None:
            self._listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
            sock = self._listener.socket
        else:
            self._listener = (await open_tls_over_tcp_listeners(0, self._ssl_context, host="127.0.0.1"))[0]
            sock = self._listener.transport.socket
        self.port = sock.getsockname()[1]
        await self._scope.__aenter__()
        self._accept_task = self._scope.spawn(self._accept_loop())
        return self

    async def _accept_loop(self):
        while True:
            try:
                stream = await self._listener.accept()
            except Exception:  # the listener was closed by stop()
                return
            self._streams.append(stream)
            tonio.spawn.without_tracking(self._serve(stream))

    async def _serve(self, stream):
        try:
            server = await auto.serve(stream, backend=Backend.tonio)
            async with server:
                async for request in server:
                    body = await request.read()
                    payload = {
                        "method": request.method,
                        "path": request.path,
                        "headers": {key: value.decode("latin-1") for key, value in request.headers.items()},
                        "body": body.decode("utf-8", errors="replace"),
                    }
                    await request.respond(
                        200, headers={"content-type": "application/json"}, body=json.dumps(payload).encode()
                    )
        except Exception:
            pass  # the peer went away (failed handshake, abortive close): not a test failure

    async def stop(self):
        self._listener.close()  # ends the parked accept: the loop returns
        for stream in self._streams:
            _backend.close_transport(stream)  # ends any parked read: the connection tasks end
        self._scope.cancel()
        await self._scope.__aexit__(None, None, None)


class TonioRawProxy:
    """A CONNECT-only proxy on tonio: relays the tunnel to the requested target,
    or refuses it with `connect_status`."""

    def __init__(self, connect_status=200):
        self.connect_status = connect_status
        self.connect_targets = []
        self._listener = None
        self._scope = tonio.scope()
        self._streams = []
        self.port = None

    async def start(self):
        self._listener = (await open_tcp_listeners(0, host="127.0.0.1"))[0]
        self.port = self._listener.socket.getsockname()[1]
        await self._scope.__aenter__()
        self._accept_task = self._scope.spawn(self._accept_loop())
        return self

    async def _accept_loop(self):
        while True:
            try:
                stream = await self._listener.accept()
            except Exception:  # the listener was closed by stop()
                return
            self._streams.append(stream)
            tonio.spawn.without_tracking(self._handle(stream))

    async def _handle(self, client):
        try:
            head, leftover = await read_head(client)
            if not head:
                client.close()
                return
            method, target, _ = head.split(b"\r\n")[0].decode().split(" ")
            if method != "CONNECT":
                await client.send_all(b"HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\n\r\n")
                client.close()
                return
            self.connect_targets.append(target)
            if self.connect_status != 200:
                await client.send_all(f"HTTP/1.1 {self.connect_status} Denied\r\ncontent-length: 0\r\n\r\n".encode())
                client.close()
                return
            host, _, port = target.rpartition(":")
            upstream = await open_tcp_stream(host.strip("[]"), int(port))
            self._streams.append(upstream)
            await client.send_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            if leftover:
                await upstream.send_all(leftover)
        except Exception:
            client.close()
            return
        # two pumps; whichever ends first closes both sockets, which ends the other's parked read
        tonio.spawn.without_tracking(self._pump(client, upstream))
        tonio.spawn.without_tracking(self._pump(upstream, client))

    @staticmethod
    async def _pump(src, dst):
        try:
            while data := await src.receive_some(65536):
                await dst.send_all(data)
        except Exception:
            pass
        finally:
            src.close()
            dst.close()

    async def stop(self):
        self._listener.close()  # ends the parked accept: the loop returns
        for stream in self._streams:
            stream.close()  # ends any parked read: the tunnel tasks end
        self._scope.cancel()
        await self._scope.__aexit__(None, None, None)
