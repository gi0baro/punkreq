import asyncio

import httpunk.asyncio
import pytest
from httpunk import Backend

from punkreq import Request, Response
from punkreq._connect import Connector
from punkreq._pool import ConnectionPool
from punkreq._transport import PoolTransport, _PooledStream


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=15))


class FakeHttpunkResponse:
    """The `_PooledStream`-facing slice of an httpunk response."""

    def __init__(self, chunks, aclose_error=None):
        self._chunks = list(chunks)
        self.aclose_calls = 0
        self.iter_finalized = False
        self._aclose_error = aclose_error

    async def aiter_bytes(self):
        try:
            for chunk in self._chunks:
                yield chunk
        finally:
            self.iter_finalized = True

    async def aclose(self):
        self.aclose_calls += 1
        if self._aclose_error is not None:
            raise self._aclose_error


class ReleaseRecorder:
    def __init__(self):
        self.calls = []

    async def __call__(self, *, discard=False):
        self.calls.append(discard)


def make_stream(fake, recorder):
    return _PooledStream(
        fake,
        Request("GET", "http://example.com/"),
        backend=Backend.asyncio.create(),
        read_timeout=None,
        deadline=None,
        release=recorder,
    )


def make_response(fake, recorder):
    return Response(200, stream=make_stream(fake, recorder), request=Request("GET", "http://example.com/"))


class TestPooledStreamRelease:
    def test_full_read_releases_reusable(self):
        fake = FakeHttpunkResponse([b"aa", b"bb"])
        recorder = ReleaseRecorder()

        async def main():
            response = make_response(fake, recorder)
            assert await response.read() == b"aabb"

        run(main())
        assert recorder.calls == [False]
        assert fake.aclose_calls == 1
        assert fake.iter_finalized

    def test_aborted_read_releases_discard(self):
        fake = FakeHttpunkResponse([b"aa", b"bb", b"cc"])
        recorder = ReleaseRecorder()

        async def main():
            response = make_response(fake, recorder)
            body = response.iter_bytes()
            assert await body.__anext__() == b"aa"
            await body.aclose()
            # the inner httpunk iterator was closed deterministically by the
            # aclose chain, not left suspended for GC to finalize
            assert fake.iter_finalized
            assert response.is_closed

        run(main())
        assert recorder.calls == [True]
        assert fake.aclose_calls == 1

    def test_interrupted_abort_still_discards(self):
        # the abort itself dies mid-teardown (stand-in for a cancellation or a
        # GC-driven unwind cutting the close path short): the connection must
        # still be released exactly once, as a discard
        fake = FakeHttpunkResponse([b"aa", b"bb"], aclose_error=RuntimeError("boom mid-teardown"))
        recorder = ReleaseRecorder()

        async def main():
            response = make_response(fake, recorder)
            body = response.iter_bytes()
            assert await body.__anext__() == b"aa"
            with pytest.raises(RuntimeError):
                await body.aclose()

        run(main())
        assert recorder.calls == [True]

    def test_close_before_reading_discards(self):
        fake = FakeHttpunkResponse([b"aa"])
        recorder = ReleaseRecorder()

        async def main():
            response = make_response(fake, recorder)
            await response.close()

        run(main())
        assert recorder.calls == [True]
        assert fake.aclose_calls == 1

    def test_close_is_idempotent(self):
        fake = FakeHttpunkResponse([b"aa"])
        recorder = ReleaseRecorder()

        async def main():
            stream = make_stream(fake, recorder)
            await stream.close()
            await stream.close()

        run(main())
        assert recorder.calls == [True]
        assert fake.aclose_calls == 1


_BIG_BODY = b"x" * 262_144


class _BigBodyServer(httpunk.asyncio.AutoServerProtocol):
    async def handle(self, request):
        await request.read()
        await request.respond(200, headers={"content-type": "application/octet-stream"}, body=_BIG_BODY)


class TestAbortReuse:
    """Regression for the aborted-mid-stream/pool-poisoning race: a connection
    whose response was not fully read must never be parked as keepalive."""

    async def _setup(self):
        loop = asyncio.get_running_loop()
        server = await loop.create_server(_BigBodyServer, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        backend = Backend.asyncio.create()
        inner = Connector(backend=backend)
        dials = [0]

        async def connector(origin):
            dials[0] += 1
            return await inner(origin)

        return server, port, backend, connector, dials

    def test_aborted_body_never_parked_next_request_redials(self):
        async def main():
            server, port, backend, connector, dials = await self._setup()
            async with ConnectionPool(connector, backend=backend) as pool:
                transport = PoolTransport(pool, backend=backend)
                response = await transport.send(Request("GET", f"http://127.0.0.1:{port}/big"))
                body = response.iter_bytes()
                await body.__anext__()  # partial read (first chunk < full body)
                await body.aclose()  # abort mid-body
                assert pool.idle_count == 0  # never parked as keepalive

                response2 = await transport.send(Request("GET", f"http://127.0.0.1:{port}/again"))
                assert await response2.read() == _BIG_BODY
                assert dials[0] == 2  # fresh dial, no poisoned reuse
                assert pool.idle_count == 1  # the healthy conn is parked
            server.close()
            await server.wait_closed()

        run(main())

    def test_fully_read_body_keeps_keepalive(self):
        async def main():
            server, port, backend, connector, dials = await self._setup()
            async with ConnectionPool(connector, backend=backend) as pool:
                transport = PoolTransport(pool, backend=backend)
                response = await transport.send(Request("GET", f"http://127.0.0.1:{port}/one"))
                assert await response.read() == _BIG_BODY
                assert pool.idle_count == 1

                response2 = await transport.send(Request("GET", f"http://127.0.0.1:{port}/two"))
                assert await response2.read() == _BIG_BODY
                assert dials[0] == 1  # keep-alive reuse survived the gating
            server.close()
            await server.wait_closed()

        run(main())
