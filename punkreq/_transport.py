from __future__ import annotations

import functools
import threading
import typing

import httpunk
from httpunk import GoAwayError, H2Reason, StreamResetError
from httpunk.exceptions import (
    ConnectionClosedError,
    H1IncompleteMessageError,
    H1UserError,
    H2Error,
    H2UserError,
    HTTPunkError,
)

from ._config import Timeout
from ._connect import origin_for_url
from ._content import AsyncByteStream, ByteStream
from ._exceptions import (
    HTTPError,
    LocalProtocolError,
    ReadError,
    ReadTimeout,
    RemoteProtocolError,
    RequestError,
    TimeoutException,
)
from ._headers import Headers
from ._models import Request, Response
from ._pool import ConnectionPool, _is_multiplexed
from ._proxies import PROXY_ATTR, ProxyInfo


__all__ = ["PoolTransport"]

_MAX_NACK_RETRIES = 2

# The peer went away with our exchange in flight: the transport failed (hyper's
# `Io`, both protocols), or on h1 it closed cleanly before the response head
# (hyper's `IncompleteMessage`, httpunk >= 0.3.0 — a sibling under `H1Error`, not
# a `ConnectionClosedError`). The same "server disconnected" verdict either way.
_DISCONNECTED = (ConnectionClosedError, H1IncompleteMessageError)


def map_httpunk_exception(exc: BaseException, request: Request) -> BaseException:
    """Translate an httpunk (or OS-level) failure into the punkreq hierarchy."""
    if isinstance(exc, HTTPError):
        if isinstance(exc, RequestError) and exc._request is None:
            exc.request = request
        return exc
    message = str(exc) or type(exc).__name__
    mapped: HTTPError
    if isinstance(exc, (H1UserError, H2UserError, ValueError)):
        # local misuse: hyper's `Kind::User` on h1, the h2 state machine's API
        # errors, and the `http` crate's constructor validation (`ValueError`)
        mapped = LocalProtocolError(message)
    elif isinstance(exc, _DISCONNECTED):
        mapped = RemoteProtocolError(f"Server disconnected: {message}")
    elif isinstance(exc, (H2Error, HTTPunkError)):
        mapped = RemoteProtocolError(message)
    elif isinstance(exc, OSError):
        mapped = ReadError(message)
    else:
        return exc
    mapped.request = request
    return mapped


def _is_retryable_nack(exc: BaseException, reused: bool, replayable: bool) -> bool:
    """reqwest's conservative retry policy: only failures where the server
    demonstrably never processed the request — and only when the body can
    still be resent (from memory, or because it was provably never touched)."""
    if isinstance(exc, GoAwayError):
        return replayable and exc.error_code == H2Reason.NO_ERROR
    if isinstance(exc, StreamResetError):
        return replayable and exc.error_code == H2Reason.REFUSED_STREAM
    if getattr(exc, "request_unsent", False):
        # httpunk >= 0.2.0 (h1): the failure was raised before the request was
        # handed to the body writer — nothing reached the wire and the body
        # was never iterated, so even a streamed body is intact and safe to
        # resend (hyper's `try_send_request` give-back, client/conn/http1.rs:
        # an error "before trying to serialize the request" returns the
        # message). The `reused` gate is punkreq's own policy: a give-back on a
        # fresh connection means the origin is failing at dial time, and a
        # redial would only fail the same way — only an idle keep-alive
        # connection dying underneath us is a nack worth one more try.
        return reused
    if isinstance(exc, _DISCONNECTED):
        # h1 keep-alive race past the writer hand-off: the server closed the
        # connection as we reused it (EOF before the response head, hyper's
        # `IncompleteMessage`), but body bytes may have been consumed.
        # Only safe when the connection had served traffic before AND the body
        # replays from memory.
        return reused and replayable
    return False


def _to_httpunk_request(request: Request, *, h2: bool, proxy: ProxyInfo | None = None) -> httpunk.Request:
    headers = request.headers._map
    needs_copy = proxy is not None or (h2 and ("host" in headers or "transfer-encoding" in headers))
    if needs_copy:
        headers = httpunk.HeaderMap(headers)
    if h2:
        for name in ("host", "transfer-encoding"):
            if name in headers:
                del headers[name]

    target = request.url.raw_path
    if proxy is not None:
        # plain-http proxying: absolute-form target + per-request proxy headers
        target = f"{request.url.scheme}://{request.url.netloc}{request.url.raw_path}"
        if proxy.auth is not None:
            headers.setdefault("proxy-authorization", proxy.auth)
        for key, value in proxy.headers.raw:
            headers.setdefault(key, value)

    body: typing.Any
    if isinstance(request.stream, ByteStream):
        body = request.stream.data or None
    else:
        body = request.stream

    return httpunk.Request(request.method, target, headers=headers, body=body)


class _PooledStream(AsyncByteStream):
    """Adapts an httpunk response body to `AsyncByteStream`, bounding each read
    with the `read` timeout and releasing the connection exactly once — on
    natural end, on error, or on early close. The release is gated on
    completion, not on the connection's own `.closed` state: httpunk's
    `Response.aclose()` closes/resets the connection on a partial read, but
    that teardown can be interrupted (cancellation, GC-driven unwind) before
    the connection learns it is dead — so anything short of a fully-read body
    releases with `discard=True` and is dropped instead of parked."""

    def __init__(
        self,
        httpunk_response: typing.Any,
        request: Request,
        *,
        backend: typing.Any,
        read_timeout: float | None,
        deadline: float | None,
        release: typing.Callable[..., typing.Awaitable[None]] | None,
    ) -> None:
        self._response = httpunk_response
        self._request = request
        self._backend = backend
        self._read_timeout = read_timeout
        self._deadline = deadline
        self._release = release
        self._finalized = False
        # `_complete` is flipped synchronously when the body ends naturally —
        # before any await in the close path — so it survives an interrupted
        # teardown. It is the release gate: a connection whose exchange did not
        # complete is discarded, never parked (`release(discard=True)`).
        self._complete = False
        # close() must be enter-once even across threads: a double entry would
        # release the connection to the pool twice (double `leased` decrement,
        # double-parked conn). Free-threaded GC can drive an unwind concurrently
        # with a user-driven close, so a plain flag check is not enough.
        self._close_lock = threading.Lock()
        self._on_finish: list[typing.Callable[[], typing.Any]] = []

    @property
    def closed(self) -> bool:
        return self._finalized

    def add_finish_callback(self, callback: typing.Callable[[], typing.Any]) -> None:
        """Register a callback fired exactly once when the stream finalizes
        (natural end, error, or close). May return an awaitable."""
        self._on_finish.append(callback)

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        return self._iterate()

    async def _iterate(self) -> typing.AsyncIterator[bytes]:
        # The inner iterator is closed deterministically in the finally: the
        # runtime does not finalize abandoned async generators (a GC-driven
        # unwind dies at its first suspension), so every wrapper in the chain
        # must aclose what it opened. The finally also runs on GeneratorExit,
        # making this stream self-cleaning when a consumer stops early.
        iterator = self._response.aiter_bytes()
        try:
            while True:
                try:
                    effective = self._read_timeout
                    if self._deadline is not None:
                        remaining = self._deadline - self._backend.monotonic()
                        if remaining <= 0:
                            raise TimeoutException("Exceeded the total request timeout", request=self._request)
                        effective = remaining if effective is None else min(effective, remaining)
                    if effective is None:
                        chunk = await iterator.__anext__()
                    else:
                        result, completed = await self._backend.timeout(iterator.__anext__(), effective)
                        if not completed:
                            raise ReadTimeout("Timed out reading the response body", request=self._request)
                        chunk = result
                except StopAsyncIteration:
                    self._complete = True  # sync flip, before any teardown await
                    break
                except BaseException as exc:
                    raise map_httpunk_exception(exc, self._request)
                yield chunk
        finally:
            try:
                await iterator.aclose()
            finally:
                await self.close()

    async def close(self) -> None:
        with self._close_lock:
            if self._finalized:
                return
            self._finalized = True
        discard = not self._complete
        try:
            await self._response.aclose()
        except BaseException:
            # the abort didn't complete; whatever the connection's own state
            # says, it must not be reused
            discard = True
            raise
        finally:
            try:
                if self._release is not None:
                    await self._release(discard=discard)
            finally:
                for callback in self._on_finish:
                    result = callback()
                    if result is not None and hasattr(result, "__await__"):
                        await result


class PoolTransport:
    """Sends single requests over a `ConnectionPool` (no redirects, cookies or
    auth — those live in the client pipeline above)."""

    def __init__(self, pool: ConnectionPool, *, backend: typing.Any) -> None:
        self._pool = pool
        self._backend = backend

    async def send(self, request: Request) -> Response:
        origin = origin_for_url(request.url)
        timeout = request.timeout if request.timeout is not None else Timeout(None)
        # the total-timeout deadline; the client pins it on the request so it
        # spans redirect hops, else it covers this exchange only
        deadline = request._deadline
        if deadline is None and timeout.total is not None:
            deadline = self._backend.monotonic() + timeout.total
        replayable = isinstance(request.stream, ByteStream)
        attempts = 0

        while True:
            conn, exclusive, reused = await self._pool.acquire(
                origin,
                connect_timeout=self._effective(timeout.connect, deadline, request),
                pool_timeout=self._effective(timeout.pool, deadline, request),
            )
            try:
                httpunk_response = await self._send_on(conn, request, self._effective(timeout.read, deadline, request))
            except BaseException as exc:
                # exclusive (h1): the connection is mid-exchange and unusable —
                # discard it; release updates the pool's books synchronously and
                # closes the connection itself, so even if the close is
                # interrupted the conn is already off the books, never parked.
                # shared (h2): gives back the stream lease; the connection
                # itself is only condemned by its own `closed` state.
                await self._pool.release(origin, conn, discard=True)
                if _is_retryable_nack(exc, reused, replayable) and attempts < _MAX_NACK_RETRIES:
                    attempts += 1
                    continue
                raise map_httpunk_exception(exc, request)

            release = functools.partial(self._pool.release, origin, conn)
            stream = _PooledStream(
                httpunk_response,
                request,
                backend=self._backend,
                read_timeout=timeout.read,
                deadline=deadline,
                release=release,
            )
            return Response(
                httpunk_response.status,
                headers=Headers(httpunk_response.headers),
                stream=stream,
                request=request,
                http_version=str(httpunk_response.version),  # httpunk.Version, `http::Version` names
            )

    def _effective(self, phase: float | None, deadline: float | None, request: Request) -> float | None:
        """The timeout for one operation: the phase timeout bounded by whatever
        remains of the total deadline. Raises once the deadline has passed."""
        if deadline is None:
            return phase
        remaining = deadline - self._backend.monotonic()
        if remaining <= 0:
            raise TimeoutException("Exceeded the total request timeout", request=request)
        return remaining if phase is None else min(phase, remaining)

    async def _send_on(self, conn: typing.Any, request: Request, read_timeout: float | None) -> typing.Any:
        proxy_info = getattr(conn, PROXY_ATTR, None)
        httpunk_request = _to_httpunk_request(request, h2=_is_multiplexed(conn), proxy=proxy_info)
        if read_timeout is None:
            return await conn.send_request(httpunk_request)
        result, completed = await self._backend.timeout(conn.send_request(httpunk_request), read_timeout)
        if not completed:
            raise ReadTimeout("Timed out waiting for the response", request=request)
        return result
