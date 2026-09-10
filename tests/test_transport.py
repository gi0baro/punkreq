import pytest
from httpunk import Backend, GoAwayError, H2Reason, StreamResetError
from httpunk.exceptions import (
    ConnectionClosedError,
    H1IncompleteMessageError,
    H1ParseError,
    H1UnexpectedMessageError,
    H1UserError,
    H2UserError,
)

from punkreq import LocalProtocolError, RemoteProtocolError, Request, Response
from punkreq._pool import ConnectionPool
from punkreq._transport import PoolTransport, _is_retryable_nack, _PooledStream, map_httpunk_exception
from tests.fakes import FakeConnection, FakeHttpunkResponse


REQUEST = Request("GET", "http://example.com/")


class ReleaseRecorder:
    def __init__(self):
        self.calls = []

    async def __call__(self, *, discard=False):
        self.calls.append(discard)


def make_stream(fake, recorder):
    return _PooledStream(
        fake, REQUEST, backend=Backend.asyncio.create(), read_timeout=None, deadline=None, release=recorder
    )


def make_response(fake, recorder):
    return Response(200, stream=make_stream(fake, recorder), request=REQUEST)


def unsent(exc):
    exc.request_unsent = True
    return exc


@pytest.fixture
def make_transport():
    """`(conns, pool, transport)`: a PoolTransport over a pool of plain
    FakeConnections (h1, or shared h2 with `multiplexed=True`), the dialed
    connections recorded in `conns`."""

    def factory(*, multiplexed=False):
        conns = []

        async def connector(origin):
            conn = FakeConnection(multiplexed=multiplexed)
            conns.append(conn)
            return conn

        pool = ConnectionPool(connector, backend=Backend.asyncio.create())
        return conns, pool, PoolTransport(pool, backend=Backend.asyncio.create())

    return factory


def streamed_request(url):
    async def body():
        yield b"upload"

    return Request("POST", url, content=body())


async def park_connection(transport):
    """Complete one exchange so the connection is parked (next acquire -> reused)."""
    response = await transport.send(Request("GET", "http://example.com/warm"))
    await response.read()


# ----- _PooledStream: releasing the connection -----


@pytest.mark.asyncio
async def test_stream_full_read_releases_reusable():
    fake = FakeHttpunkResponse(body=[b"aa", b"bb"])
    recorder = ReleaseRecorder()
    response = make_response(fake, recorder)
    assert await response.read() == b"aabb"
    assert recorder.calls == [False]
    assert fake.aclose_calls == 1
    assert fake.iter_finalized


@pytest.mark.asyncio
async def test_stream_aborted_read_releases_discard():
    fake = FakeHttpunkResponse(body=[b"aa", b"bb", b"cc"])
    recorder = ReleaseRecorder()
    response = make_response(fake, recorder)
    body = response.iter_bytes()
    assert await body.__anext__() == b"aa"
    await body.aclose()
    # the inner httpunk iterator was closed deterministically by the
    # aclose chain, not left suspended for GC to finalize
    assert fake.iter_finalized
    assert response.is_closed
    assert recorder.calls == [True]
    assert fake.aclose_calls == 1


@pytest.mark.asyncio
async def test_stream_interrupted_abort_still_discards():
    # the abort itself dies mid-teardown (stand-in for a cancellation or a
    # GC-driven unwind cutting the close path short): the connection must
    # still be released exactly once, as a discard
    fake = FakeHttpunkResponse(body=[b"aa", b"bb"], aclose_error=RuntimeError("boom mid-teardown"))
    recorder = ReleaseRecorder()
    response = make_response(fake, recorder)
    body = response.iter_bytes()
    assert await body.__anext__() == b"aa"
    with pytest.raises(RuntimeError):
        await body.aclose()
    assert recorder.calls == [True]


@pytest.mark.asyncio
async def test_stream_close_before_reading_discards():
    fake = FakeHttpunkResponse(body=[b"aa"])
    recorder = ReleaseRecorder()
    response = make_response(fake, recorder)
    await response.close()
    assert recorder.calls == [True]
    assert fake.aclose_calls == 1


@pytest.mark.asyncio
async def test_stream_close_is_idempotent():
    fake = FakeHttpunkResponse(body=[b"aa"])
    recorder = ReleaseRecorder()
    stream = make_stream(fake, recorder)
    await stream.close()
    await stream.close()
    assert recorder.calls == [True]
    assert fake.aclose_calls == 1


# ----- _is_retryable_nack: the failure classes where the server demonstrably
# never processed the request, gated on the body still being resendable -----


def test_nack_request_unsent_retries_any_body_on_reused():
    exc = unsent(ConnectionClosedError("connection closed"))
    assert _is_retryable_nack(exc, reused=True, replayable=False)
    assert _is_retryable_nack(exc, reused=True, replayable=True)


def test_nack_request_unsent_fresh_connection_never_retries():
    exc = unsent(ConnectionClosedError("connection closed"))
    assert not _is_retryable_nack(exc, reused=False, replayable=True)


def test_nack_request_unsent_marker_works_on_any_exception_type():
    # the idle-bytes poison error carries the marker on a ValueError
    exc = unsent(ValueError("received 7 unexpected bytes on an idle HTTP/1 connection"))
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
def test_nack_disconnected_without_marker_needs_replayable(exc):
    assert _is_retryable_nack(exc, reused=True, replayable=True)
    assert not _is_retryable_nack(exc, reused=True, replayable=False)
    assert not _is_retryable_nack(exc, reused=False, replayable=True)


def test_nack_other_h1_errors_never_retry():
    # unexpected bytes past a response poison the connection: a genuine
    # protocol violation, not a nack (only its `request_unsent` re-raise on
    # the next send is retried, and that goes through the marker branch)
    assert not _is_retryable_nack(H1UnexpectedMessageError("7 bytes"), reused=True, replayable=True)
    assert not _is_retryable_nack(H1ParseError("Version", "invalid HTTP version"), reused=True, replayable=True)


def test_nack_h2_errors_need_replayable():
    goaway = GoAwayError(0, int(H2Reason.NO_ERROR))
    assert _is_retryable_nack(goaway, reused=False, replayable=True)
    assert not _is_retryable_nack(goaway, reused=False, replayable=False)
    refused = StreamResetError(1, int(H2Reason.REFUSED_STREAM))
    assert _is_retryable_nack(refused, reused=False, replayable=True)
    assert not _is_retryable_nack(refused, reused=False, replayable=False)
    assert not _is_retryable_nack(GoAwayError(0, int(H2Reason.PROTOCOL_ERROR)), reused=True, replayable=True)


def test_nack_unrelated_errors_never_retry():
    assert not _is_retryable_nack(OSError("boom"), reused=True, replayable=True)


# ----- map_httpunk_exception: httpunk's taxonomy onto punkreq's -----


def test_map_local_misuse_is_local():
    for exc in (H1UserError("unexpected_header", "unexpected header"), H2UserError("x", "bad"), ValueError("v")):
        mapped = map_httpunk_exception(exc, REQUEST)
        assert isinstance(mapped, LocalProtocolError), exc
        assert mapped.request is REQUEST


def test_map_disconnect_is_remote_server_disconnected():
    for exc in (
        H1IncompleteMessageError("connection closed before message completed: no response head"),
        ConnectionClosedError("connection closed"),
    ):
        mapped = map_httpunk_exception(exc, REQUEST)
        assert isinstance(mapped, RemoteProtocolError), exc
        assert str(mapped).startswith("Server disconnected: ")


def test_map_peer_violations_are_remote():
    for exc in (H1ParseError("Version", "invalid HTTP version"), H1UnexpectedMessageError("7 bytes")):
        mapped = map_httpunk_exception(exc, REQUEST)
        assert isinstance(mapped, RemoteProtocolError), exc
        assert not str(mapped).startswith("Server disconnected")


def test_map_os_error_is_read_error():
    mapped = map_httpunk_exception(OSError("boom"), REQUEST)
    assert type(mapped).__name__ == "ReadError"


# ----- the transport's retry loop over _is_retryable_nack -----


@pytest.mark.asyncio
async def test_retry_streamed_body_when_request_unsent(make_transport):
    conns, pool, transport = make_transport()
    async with pool:
        await park_connection(transport)
        conns[0].fail_next = unsent(ConnectionClosedError("connection closed"))
        response = await transport.send(streamed_request("http://example.com/upload"))
        assert response.status_code == 200
        assert await response.read() == b"ok"
        assert len(conns) == 2  # dead conn discarded, retry redialed
        assert conns[0].closed
        assert conns[0].bodies_touched == 0  # the streamed body was never iterated
        assert conns[1].bodies_touched == 1


@pytest.mark.asyncio
async def test_no_retry_streamed_body_without_marker(make_transport):
    conns, pool, transport = make_transport()
    async with pool:
        await park_connection(transport)
        conns[0].fail_next = H1IncompleteMessageError("connection closed before message completed")
        with pytest.raises(RemoteProtocolError):
            await transport.send(streamed_request("http://example.com/upload"))
        assert len(conns) == 1  # no retry: the body may have been consumed


@pytest.mark.asyncio
async def test_retry_replayable_body_without_marker(make_transport):
    conns, pool, transport = make_transport()
    async with pool:
        await park_connection(transport)
        conns[0].fail_next = H1IncompleteMessageError("connection closed before message completed")
        response = await transport.send(Request("POST", "http://example.com/x", content=b"data"))
        assert response.status_code == 200
        assert await response.read() == b"ok"
        assert len(conns) == 2


@pytest.mark.asyncio
async def test_no_retry_request_unsent_on_fresh_connection(make_transport):
    conns, pool, transport = make_transport()
    async with pool:
        # no warm-up: the first acquire dials fresh (reused=False)
        async def connector_fail(origin):
            conn = FakeConnection()
            conn.fail_next = unsent(ConnectionClosedError("connection closed"))
            conns.append(conn)
            return conn

        pool._connector = connector_fail
        with pytest.raises(RemoteProtocolError):
            await transport.send(streamed_request("http://example.com/upload"))
        assert len(conns) == 1


# ----- h2 stream leases: released exactly once, on body end, early close or
# send failure, so the pool can see a stream-less shared connection as
# reclaimable capacity -----


@pytest.mark.asyncio
async def test_h2_lease_released_after_body_read(make_transport):
    conns, pool, transport = make_transport(multiplexed=True)
    async with pool:
        response = await transport.send(Request("GET", "http://example.com/a"))
        host = next(iter(pool._hosts.values()))
        assert host.shared_leases == 1  # body not read yet
        assert await response.read() == b"ok"
        assert host.shared_leases == 0
        assert pool.connection_count == 1  # the conn itself stays pooled


@pytest.mark.asyncio
async def test_h2_lease_released_on_early_close(make_transport):
    conns, pool, transport = make_transport(multiplexed=True)
    async with pool:
        response = await transport.send(Request("GET", "http://example.com/a"))
        host = next(iter(pool._hosts.values()))
        await response.close()  # aborted body: lease back, conn NOT condemned
        assert host.shared_leases == 0
        assert not conns[0].closed
        assert pool.connection_count == 1


@pytest.mark.asyncio
async def test_h2_lease_released_on_send_failure(make_transport):
    conns, pool, transport = make_transport(multiplexed=True)
    async with pool:
        response = await transport.send(Request("GET", "http://example.com/a"))
        await response.read()
        host = next(iter(pool._hosts.values()))
        conns[0].fail_next = ValueError("broken framing")  # non-retryable
        with pytest.raises(LocalProtocolError):
            await transport.send(Request("GET", "http://example.com/b"))
        assert host.shared_leases == 0
