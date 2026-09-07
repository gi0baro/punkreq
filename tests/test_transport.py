import asyncio

import httpunk.asyncio
import pytest
from httpunk import Backend, GoAwayError, H2Reason, HeaderMap, StreamResetError, Version
from httpunk.exceptions import (
    ConnectionClosedError,
    H1IncompleteMessageError,
    H1ParseError,
    H1UnexpectedMessageError,
    H1UserError,
    H2UserError,
)

from punkreq import LocalProtocolError, RemoteProtocolError, Request, Response
from punkreq._connect import Connector
from punkreq._pool import ConnectionPool
from punkreq._transport import PoolTransport, _is_retryable_nack, _PooledStream, map_httpunk_exception


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


def _unsent(exc):
    exc.request_unsent = True
    return exc


class TestNackPolicy:
    """`_is_retryable_nack`: the failure classes where the server demonstrably
    never processed the request, gated on the body still being resendable."""

    def test_request_unsent_retries_any_body_on_reused(self):
        exc = _unsent(ConnectionClosedError("connection closed"))
        assert _is_retryable_nack(exc, reused=True, replayable=False)
        assert _is_retryable_nack(exc, reused=True, replayable=True)

    def test_request_unsent_fresh_connection_never_retries(self):
        exc = _unsent(ConnectionClosedError("connection closed"))
        assert not _is_retryable_nack(exc, reused=False, replayable=True)

    def test_request_unsent_marker_works_on_any_exception_type(self):
        # the idle-bytes poison error carries the marker on a ValueError
        exc = _unsent(ValueError("received 7 unexpected bytes on an idle HTTP/1 connection"))
        assert _is_retryable_nack(exc, reused=True, replayable=False)

    @pytest.mark.parametrize(
        "exc",
        [
            # httpunk >= 0.3.0 h1: EOF before the response head (hyper `IncompleteMessage`)
            H1IncompleteMessageError("connection closed before message completed: no response head"),
            # a transport failure with the exchange in flight (hyper `Io`, h1 + h2)
            ConnectionClosedError("connection closed"),
        ],
    )
    def test_disconnected_without_marker_needs_replayable(self, exc):
        assert _is_retryable_nack(exc, reused=True, replayable=True)
        assert not _is_retryable_nack(exc, reused=True, replayable=False)
        assert not _is_retryable_nack(exc, reused=False, replayable=True)

    def test_other_h1_errors_never_retry(self):
        # unexpected bytes past a response poison the connection: a genuine
        # protocol violation, not a nack (only its `request_unsent` re-raise on
        # the next send is retried, and that goes through the marker branch)
        assert not _is_retryable_nack(H1UnexpectedMessageError("7 bytes"), reused=True, replayable=True)
        assert not _is_retryable_nack(H1ParseError("Version", "invalid HTTP version"), reused=True, replayable=True)

    def test_h2_nacks_need_replayable(self):
        goaway = GoAwayError(0, int(H2Reason.NO_ERROR))
        assert _is_retryable_nack(goaway, reused=False, replayable=True)
        assert not _is_retryable_nack(goaway, reused=False, replayable=False)
        refused = StreamResetError(1, int(H2Reason.REFUSED_STREAM))
        assert _is_retryable_nack(refused, reused=False, replayable=True)
        assert not _is_retryable_nack(refused, reused=False, replayable=False)
        assert not _is_retryable_nack(GoAwayError(0, int(H2Reason.PROTOCOL_ERROR)), reused=True, replayable=True)

    def test_unrelated_errors_never_retry(self):
        assert not _is_retryable_nack(OSError("boom"), reused=True, replayable=True)


class TestExceptionMapping:
    """`map_httpunk_exception`: httpunk's taxonomy onto punkreq's."""

    request = Request("GET", "http://example.com/")

    def test_local_misuse_is_local(self):
        for exc in (H1UserError("unexpected_header", "unexpected header"), H2UserError("x", "bad"), ValueError("v")):
            mapped = map_httpunk_exception(exc, self.request)
            assert isinstance(mapped, LocalProtocolError), exc
            assert mapped.request is self.request

    def test_disconnect_is_remote_with_server_disconnected(self):
        for exc in (
            H1IncompleteMessageError("connection closed before message completed: no response head"),
            ConnectionClosedError("connection closed"),
        ):
            mapped = map_httpunk_exception(exc, self.request)
            assert isinstance(mapped, RemoteProtocolError), exc
            assert str(mapped).startswith("Server disconnected: ")

    def test_peer_violations_are_remote(self):
        for exc in (H1ParseError("Version", "invalid HTTP version"), H1UnexpectedMessageError("7 bytes")):
            mapped = map_httpunk_exception(exc, self.request)
            assert isinstance(mapped, RemoteProtocolError), exc
            assert not str(mapped).startswith("Server disconnected")

    def test_os_error_is_read_error(self):
        mapped = map_httpunk_exception(OSError("boom"), self.request)
        assert type(mapped).__name__ == "ReadError"


class _NackResponse:
    status = 200
    version = Version.HTTP_11

    def __init__(self):
        self.headers = HeaderMap()

    async def aiter_bytes(self):
        yield b"ok"

    async def aclose(self):
        pass


class _NackConnection:
    """A pool-facing h1 connection whose next send can fail with the
    `request_unsent` marker (httpunk raising before the writer spawn)."""

    multiplexed = False

    def __init__(self):
        self.closed = False
        self.busy = False
        self.fail_next = None  # exception to raise on the next send_request
        self.bodies_touched = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self.closed = True
        return False

    async def ready(self):
        if self.closed:
            raise RuntimeError("connection is closed")

    async def send_request(self, request):
        if self.fail_next is not None:
            exc, self.fail_next = self.fail_next, None
            raise exc
        if request.body is not None and not isinstance(request.body, bytes):
            self.bodies_touched += 1
            async for _ in request.body:
                pass
        return _NackResponse()


class TestNackRetry:
    """The transport's retry loop over `_is_retryable_nack`."""

    @staticmethod
    def _streamed_request(url):
        async def body():
            yield b"upload"

        return Request("POST", url, content=body())

    @staticmethod
    async def _park_conn(transport):
        # complete one exchange so the connection is parked (next acquire -> reused)
        response = await transport.send(Request("GET", "http://example.com/warm"))
        await response.read()

    def _setup(self):
        conns = []

        async def connector(origin):
            conn = _NackConnection()
            conns.append(conn)
            return conn

        pool = ConnectionPool(connector, backend=Backend.asyncio.create())
        return conns, pool, PoolTransport(pool, backend=Backend.asyncio.create())

    def test_streamed_body_retried_when_request_unsent(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                await self._park_conn(transport)
                conns[0].fail_next = _unsent(ConnectionClosedError("connection closed"))
                response = await transport.send(self._streamed_request("http://example.com/upload"))
                assert response.status_code == 200
                assert await response.read() == b"ok"
                assert len(conns) == 2  # dead conn discarded, retry redialed
                assert conns[0].closed
                assert conns[0].bodies_touched == 0  # the streamed body was never iterated
                assert conns[1].bodies_touched == 1

        run(main())

    def test_streamed_body_not_retried_without_marker(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                await self._park_conn(transport)
                conns[0].fail_next = H1IncompleteMessageError("connection closed before message completed")
                with pytest.raises(RemoteProtocolError):
                    await transport.send(self._streamed_request("http://example.com/upload"))
                assert len(conns) == 1  # no retry: the body may have been consumed

        run(main())

    def test_replayable_body_still_retried_without_marker(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                await self._park_conn(transport)
                conns[0].fail_next = H1IncompleteMessageError("connection closed before message completed")
                response = await transport.send(Request("POST", "http://example.com/x", content=b"data"))
                assert response.status_code == 200
                assert await response.read() == b"ok"
                assert len(conns) == 2

        run(main())

    def test_request_unsent_on_fresh_connection_not_retried(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                # no warm-up: the first acquire dials fresh (reused=False)
                async def connector_fail(origin):
                    conn = _NackConnection()
                    conn.fail_next = _unsent(ConnectionClosedError("connection closed"))
                    conns.append(conn)
                    return conn

                pool._connector = connector_fail
                with pytest.raises(RemoteProtocolError):
                    await transport.send(self._streamed_request("http://example.com/upload"))
                assert len(conns) == 1

        run(main())


class TestSharedLeaseRelease:
    """The transport releases an h2 stream lease exactly once — on body end,
    early close, or send failure — so the pool can see a stream-less shared
    connection as reclaimable capacity."""

    def _setup(self):
        conns = []

        async def connector(origin):
            conn = _NackConnection()
            conn.multiplexed = True
            conns.append(conn)
            return conn

        pool = ConnectionPool(connector, backend=Backend.asyncio.create())
        return conns, pool, PoolTransport(pool, backend=Backend.asyncio.create())

    def test_lease_released_after_body_read(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                response = await transport.send(Request("GET", "http://example.com/a"))
                host = next(iter(pool._hosts.values()))
                assert host.shared_leases == 1  # body not read yet
                assert await response.read() == b"ok"
                assert host.shared_leases == 0
                assert pool.connection_count == 1  # the conn itself stays pooled

        run(main())

    def test_lease_released_on_early_close(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                response = await transport.send(Request("GET", "http://example.com/a"))
                host = next(iter(pool._hosts.values()))
                await response.close()  # aborted body: lease back, conn NOT condemned
                assert host.shared_leases == 0
                assert not conns[0].closed
                assert pool.connection_count == 1

        run(main())

    def test_lease_released_on_send_failure(self):
        async def main():
            conns, pool, transport = self._setup()
            async with pool:
                response = await transport.send(Request("GET", "http://example.com/a"))
                await response.read()
                host = next(iter(pool._hosts.values()))
                conns[0].fail_next = ValueError("broken framing")  # non-retryable
                with pytest.raises(LocalProtocolError):
                    await transport.send(Request("GET", "http://example.com/b"))
                assert host.shared_leases == 0

        run(main())
