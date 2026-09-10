"""Stand-ins for the httpunk objects punkreq drives: a response, a pool-facing
connection and a connector, plus a backend wrapper with a deterministic clock."""

import asyncio

from httpunk import HeaderMap, Version

import punkreq


class FakeHttpunkResponse:
    """The slice of an httpunk response punkreq reads: status, headers, version
    and a body iterated once through `aiter_bytes`, then `aclose`d. Records how
    it was driven (`aclose_calls`, `iter_finalized`); `aclose_error` makes the
    close itself fail."""

    def __init__(self, status=200, headers=None, body=b"", *, chunk_delay=0.0, aclose_error=None):
        self.status = status
        self.headers = HeaderMap(headers or {})
        self.version = Version.HTTP_11  # stamped per protocol by FakeConnection.send_request
        self._chunks = [body] if isinstance(body, bytes) else list(body)
        self._chunk_delay = chunk_delay
        self._aclose_error = aclose_error
        self._conn = None  # set by FakeConnection.send_request
        self._consumed = False
        self.closed = False
        self.aclose_calls = 0
        self.iter_finalized = False

    async def aiter_bytes(self):
        try:
            for chunk in self._chunks:
                if self._chunk_delay:
                    await asyncio.sleep(self._chunk_delay)
                yield chunk
            self._consumed = True
        finally:
            self.iter_finalized = True

    async def aclose(self):
        self.aclose_calls += 1
        self.closed = True
        # mirror httpunk h1: aborting a partially-read body closes the connection
        if not self._consumed and self._conn is not None and not self._conn.multiplexed:
            self._conn.closed = True
        if self._aclose_error is not None:
            raise self._aclose_error


class FakeConnection:
    """A pool-facing connection. `handler(request, conn)` answers each
    `send_request` (a FakeHttpunkResponse, an awaitable of one, or by raising); with no
    handler a streamed request body is drained and a 200 "ok" returned.
    `fail_next` makes the next send raise that exception instead."""

    def __init__(self, handler=None, *, multiplexed=False):
        self.handler = handler
        self.multiplexed = multiplexed
        self.closed = False
        self.busy = False  # httpunk: an exchange still holds the in-flight slot
        self.entered = False
        self.fail_next = None
        self.requests = []
        self.bodies_touched = 0

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self.closed = True
        return False

    async def ready(self):
        if self.closed:
            raise RuntimeError("connection is closed")

    async def send_request(self, request):
        self.requests.append(request)
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        if self.handler is not None:
            result = self.handler(request, self)
            if asyncio.iscoroutine(result):
                result = await result
        else:
            if request.body is not None and not isinstance(request.body, bytes):
                self.bodies_touched += 1
                async for _ in request.body:
                    pass
            result = FakeHttpunkResponse(200, body=b"ok")
        if isinstance(result, FakeHttpunkResponse):
            result._conn = self
            result.version = Version.HTTP_2 if self.multiplexed else Version.HTTP_11
        return result


class FakeConnector:
    """Dials a fresh FakeConnection per call and records them. `delay` sleeps
    before each dial; `fail` makes that many dials raise ConnectError first."""

    def __init__(self, handler=None, *, multiplexed=False, delay=0.0, fail=0):
        self.handler = handler
        self.multiplexed = multiplexed
        self.delay = delay
        self.fail = fail
        self.dials = 0
        self.connections = []

    async def __call__(self, origin):
        self.dials += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail > 0:
            self.fail -= 1
            raise punkreq.ConnectError(f"boom dialing {origin}")
        conn = FakeConnection(self.handler, multiplexed=self.multiplexed)
        self.connections.append(conn)
        return conn

    @property
    def requests(self):
        """Every request sent, across all connections, in order."""
        return [request for conn in self.connections for request in conn.requests]


def ok_handler(body=b"hello", headers=None):
    """A connection handler answering every request with one 200 response."""

    def handler(request, conn):
        return FakeHttpunkResponse(200, headers=headers, body=body)

    return handler


class SteppingBackend:
    """Delegates to a real backend, but `monotonic()` strictly increases on every
    call: on coarse OS clocks (Windows, ~16ms) consecutive releases can get equal
    idle timestamps, making eviction order tie-dependent."""

    def __init__(self, backend):
        self._backend = backend
        self._now = 0.0

    def monotonic(self):
        self._now += 1.0
        return self._now

    def __getattr__(self, name):
        return getattr(self._backend, name)
