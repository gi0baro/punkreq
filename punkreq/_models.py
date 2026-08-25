from __future__ import annotations

import codecs
import datetime
import json as jsonlib
import re
import typing
import warnings
from http import HTTPStatus

from ._config import Timeout, TimeoutTypes
from ._content import AsyncByteStream, ByteStream, RequestContent, RequestData, encode_request
from ._cookies import Cookies
from ._decoders import (
    SUPPORTED_DECODERS,
    ByteChunker,
    ContentDecoder,
    IdentityDecoder,
    LineDecoder,
    MultiDecoder,
    TextChunker,
    TextDecoder,
)
from ._exceptions import HTTPStatusError, RequestNotRead, StreamClosed, StreamConsumed
from ._headers import Headers, HeaderTypes
from ._urls import URL, QueryParamTypes


__all__ = ["Request", "Response"]

_METHODS_WITH_BODY = ("POST", "PUT", "PATCH")


def host_header_value(url: URL) -> str:
    """The Host header for `url`: IPv6-bracketed, default port omitted."""
    host = f"[{url.host}]" if ":" in url.host else url.host
    return host if url.port is None else f"{host}:{url.port}"


class Request:
    def __init__(
        self,
        method: str,
        url: URL | str,
        *,
        params: QueryParamTypes = None,
        headers: HeaderTypes = None,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        files: typing.Any = None,
        json: typing.Any = None,
        stream: AsyncByteStream | None = None,
        timeout: TimeoutTypes | Timeout | None = None,
    ) -> None:
        self.method = method.upper()
        self.url = URL(url) if params is None else URL(url).copy_merge_params(params)
        self.headers = Headers(headers)
        # Per-request timeout (reqwest `Request::timeout`): None means "no
        # per-request override" — the transport applies no timeouts then, and
        # the client verb methods always resolve one in `build_request`.
        self.timeout: Timeout | None = Timeout(timeout) if timeout is not None else None
        # The total-timeout deadline (monotonic), pinned by `Client.send` before
        # the first hop so it spans the whole redirect chain. Internal.
        self._deadline: float | None = None

        if stream is None:
            content_headers, self.stream = encode_request(content, data, files, json)
            self._prepare(content_headers)
        else:
            # explicit stream: internal use (redirect/retry resends); no auto headers
            self.stream = stream
        self._content: bytes | None = self.stream.data if isinstance(self.stream, ByteStream) else None

    def _prepare(self, content_headers: dict[str, str]) -> None:
        for key, value in content_headers.items():
            self.headers.setdefault(key, value)
        if "host" not in self.headers and self.url.is_absolute_url:
            self.headers["host"] = host_header_value(self.url)
        has_framing = "content-length" in self.headers or "transfer-encoding" in self.headers
        if not has_framing and self.method in _METHODS_WITH_BODY:
            self.headers["content-length"] = "0"

    @property
    def content(self) -> bytes:
        if self._content is None:
            raise RequestNotRead()
        return self._content

    async def read(self) -> bytes:
        """Read (and buffer) the request body; afterwards the body is replayable."""
        if self._content is None:
            self._content = b"".join([chunk async for chunk in self.stream])
            self.stream = ByteStream(self._content)
        return self._content

    def __repr__(self) -> str:
        return f"<Request('{self.method}', '{self.url}')>"


class Response:
    def __init__(
        self,
        status_code: int,
        *,
        headers: HeaderTypes = None,
        content: bytes | None = None,
        text: str | None = None,
        json: typing.Any = None,
        stream: AsyncByteStream | None = None,
        request: Request | None = None,
        http_version: str = "HTTP/1.1",
        history: typing.Sequence[Response] | None = None,
        default_encoding: str | typing.Callable[[bytes], str | None] = "utf-8",
    ) -> None:
        self.status_code = int(status_code)
        self.headers = Headers(headers)
        self.http_version = http_version  # reqwest `Response::version`
        self.history: list[Response] = list(history) if history is not None else []
        self.default_encoding = default_encoding
        self._request = request
        self._encoding: str | None = None
        self._text: str | None = None
        self._elapsed: datetime.timedelta | None = None
        self._cookies: Cookies | None = None
        self._num_bytes_downloaded = 0
        # The iterator most recently handed out by a public iter_* method.
        # `async for ... break` abandons its iterator (the language never
        # closes it), and an abandoned suspended async generator is left to GC
        # — whose one-shot unwind can't drive the awaits on the teardown path.
        # Owning it here keeps it alive until close() acloses it in a real
        # await context. A single slot suffices: only one streaming chain can
        # exist per response (`is_stream_consumed`).
        self._body_iterator: typing.AsyncGenerator[typing.Any, None] | None = None

        self.is_closed = False
        self.is_stream_consumed = False

        if stream is not None:
            self.stream: AsyncByteStream = stream
            self._content: bytes | None = None
        else:
            body: bytes
            if json is not None:
                body = jsonlib.dumps(json, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
                self.headers.setdefault("content-type", "application/json")
            elif text is not None:
                body = text.encode("utf-8")
                self.headers.setdefault("content-type", "text/plain; charset=utf-8")
            else:
                body = content or b""
            if body and "content-length" not in self.headers:
                self.headers["content-length"] = str(len(body))
            self.stream = ByteStream(body)
            self._content = body
            self.is_closed = True
            self.is_stream_consumed = True

    # -- request / metadata ------------------------------------------------

    @property
    def request(self) -> Request:
        if self._request is None:
            raise RuntimeError("The request instance has not been set on this response.")
        return self._request

    @request.setter
    def request(self, request: Request) -> None:
        self._request = request

    @property
    def url(self) -> URL:
        """The URL the response came from (post-redirect)."""
        return self.request.url

    @property
    def reason_phrase(self) -> str:
        try:
            return HTTPStatus(self.status_code).phrase
        except ValueError:
            return ""

    @property
    def elapsed(self) -> datetime.timedelta:
        """Time from sending the request until the response body was read or closed."""
        if self._elapsed is None:
            raise RuntimeError("'.elapsed' may only be accessed after the response has been read or closed.")
        return self._elapsed

    @elapsed.setter
    def elapsed(self, elapsed: datetime.timedelta) -> None:
        self._elapsed = elapsed

    @property
    def num_bytes_downloaded(self) -> int:
        return self._num_bytes_downloaded

    # -- status predicates ---------------------------------------------------

    @property
    def is_informational(self) -> bool:
        return 100 <= self.status_code < 200

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def is_redirect(self) -> bool:
        return 300 <= self.status_code < 400

    @property
    def is_client_error(self) -> bool:
        return 400 <= self.status_code < 500

    @property
    def is_server_error(self) -> bool:
        return 500 <= self.status_code < 600

    @property
    def is_error(self) -> bool:
        return 400 <= self.status_code < 600

    @property
    def has_redirect_location(self) -> bool:
        return self.status_code in (301, 302, 303, 307, 308) and "location" in self.headers

    def raise_for_status(self) -> Response:
        """Raise `HTTPStatusError` for 4xx/5xx responses; return self otherwise."""
        if not self.is_error:
            return self
        error_type = "Client error" if self.is_client_error else "Server error"
        message = f"{error_type} '{self.status_code} {self.reason_phrase}' for url '{self.url}'"
        raise HTTPStatusError(message, request=self.request, response=self)

    # -- content access ------------------------------------------------------

    @property
    def charset_encoding(self) -> str | None:
        """The charset declared in the Content-Type header, if any."""
        content_type = self.headers.get("content-type")
        if content_type is None:
            return None
        for directive in content_type.split(";")[1:]:
            key, _, value = directive.strip().partition("=")
            if key.lower() == "charset":
                return value.strip("'\"") or None
        return None

    @property
    def encoding(self) -> str:
        """Encoding used by `text()`: explicit assignment, else the Content-Type
        charset, else `default_encoding` (a name, or a callable receiving the
        raw content — e.g. charset autodetection), else utf-8. When
        `default_encoding` is a callable, the body must have been read first."""
        if self._encoding is None:
            encoding = self.charset_encoding
            if encoding is None or not _is_known_encoding(encoding):
                if callable(self.default_encoding):
                    if self._content is None:
                        raise RuntimeError(
                            "'.encoding' with a callable default_encoding requires the body: call `await read()` first."
                        )
                    encoding = self.default_encoding(self._content)
                else:
                    encoding = self.default_encoding
            self._encoding = encoding or "utf-8"
        return self._encoding

    @encoding.setter
    def encoding(self, value: str) -> None:
        if self._text is not None:
            raise ValueError("Setting encoding after `text()` has been called is not allowed.")
        self._encoding = value

    async def text(self) -> str:
        """Read the body and decode it as text (cached)."""
        if self._text is None:
            content = await self.read()
            self._text = str(content, self.encoding, errors="replace") if content else ""
        return self._text

    async def json(self, **kwargs: typing.Any) -> typing.Any:
        """Read the body and parse it as JSON."""
        return jsonlib.loads(await self.read(), **kwargs)

    @property
    def cookies(self) -> Cookies:
        """The response's Set-Cookie headers, parsed into a `Cookies` jar (lazily)."""
        if self._cookies is None:
            self._cookies = Cookies()
            self._cookies.extract_cookies(self)
        return self._cookies

    @property
    def links(self) -> dict[str, dict[str, str]]:
        """The parsed Link header, keyed by `rel` (or url when no rel is present)."""
        header = self.headers.get("link")
        if not header:
            return {}
        links = []
        for value in re.split(", *<", header.strip(" '\"")):
            url, _, params = value.partition(";")
            link = {"url": url.strip("<> '\"")}
            for param in params.split(";"):
                key, sep, param_value = param.partition("=")
                if not sep:
                    break
                link[key.strip(" '\"")] = param_value.strip(" '\"")
            links.append(link)
        return {link.get("rel", link["url"]): link for link in links}

    # -- body reading / streaming ---------------------------------------------

    def _get_content_decoder(self) -> ContentDecoder:
        values = self.headers.get_list("content-encoding", split_commas=True)
        decoders = [SUPPORTED_DECODERS.get(value.lower(), IdentityDecoder)() for value in values if value]
        if not decoders:
            return IdentityDecoder()
        if len(decoders) == 1:
            return decoders[0]
        return MultiDecoder(decoders)

    async def read(self) -> bytes:
        """Read (and buffer) the whole decoded response body. Idempotent; the
        underlying connection is released when the read completes."""
        if self._content is None:
            self._content = b"".join([chunk async for chunk in self.iter_bytes()])
        return self._content

    def iter_bytes(self, chunk_size: int | None = None) -> typing.AsyncIterator[bytes]:
        """Stream the body with content decoding applied."""
        self._body_iterator = iterator = self._iter_bytes(chunk_size)
        return iterator

    async def _iter_bytes(self, chunk_size: int | None = None) -> typing.AsyncIterator[bytes]:
        if self._content is not None:
            size = len(self._content) if chunk_size is None else chunk_size
            for i in range(0, len(self._content), max(size, 1)):
                yield self._content[i : i + max(size, 1)]
            return
        decoder = self._get_content_decoder()
        chunker = ByteChunker(chunk_size)
        inner = self._iter_raw()
        try:
            async for raw in inner:
                for chunk in chunker.decode(decoder.decode(raw)):
                    yield chunk
        finally:
            await inner.aclose()
        for chunk in chunker.decode(decoder.flush()):
            yield chunk
        for chunk in chunker.flush():
            yield chunk

    def iter_raw(self, chunk_size: int | None = None) -> typing.AsyncIterator[bytes]:
        """Stream the body as received on the wire, without content decoding."""
        self._body_iterator = iterator = self._iter_raw(chunk_size)
        return iterator

    async def _iter_raw(self, chunk_size: int | None = None) -> typing.AsyncIterator[bytes]:
        if self.is_stream_consumed:
            raise StreamConsumed()
        if self.is_closed:
            raise StreamClosed()
        self.is_stream_consumed = True
        chunker = ByteChunker(chunk_size)
        # Own the stream's iterator and aclose it deterministically: `async for`
        # does not close its iterator on early exit, and an abandoned suspended
        # async generator is only finalized by GC — whose synchronous unwind
        # dies at the first await, cutting the connection teardown short.
        # `aclose` is part of the `AsyncByteStream.__aiter__` contract.
        inner = self.stream.__aiter__()
        try:
            async for raw in inner:
                self._num_bytes_downloaded += len(raw)
                for chunk in chunker.decode(raw):
                    yield chunk
            for chunk in chunker.flush():
                yield chunk
        finally:
            try:
                await inner.aclose()  # type: ignore[attr-defined]
            finally:
                await self.close()

    def iter_text(self, chunk_size: int | None = None) -> typing.AsyncIterator[str]:
        self._body_iterator = iterator = self._iter_text(chunk_size)
        return iterator

    async def _iter_text(self, chunk_size: int | None = None) -> typing.AsyncIterator[str]:
        decoder = TextDecoder(self.encoding)
        chunker = TextChunker(chunk_size)
        inner = self._iter_bytes()
        try:
            async for content in inner:
                for chunk in chunker.decode(decoder.decode(content)):
                    yield chunk
        finally:
            await inner.aclose()
        for chunk in chunker.decode(decoder.flush()):
            yield chunk
        for chunk in chunker.flush():
            yield chunk

    def iter_lines(self) -> typing.AsyncIterator[str]:
        self._body_iterator = iterator = self._iter_lines()
        return iterator

    async def _iter_lines(self) -> typing.AsyncIterator[str]:
        decoder = LineDecoder()
        inner = self._iter_text()
        try:
            async for text in inner:
                for line in decoder.decode(text):
                    yield line
        finally:
            await inner.aclose()
        for line in decoder.flush():
            yield line

    async def close(self) -> None:
        """Release the response. If the body was not fully read, the underlying
        exchange is aborted. Idempotent."""
        if not self.is_closed:
            self.is_closed = True
            iterator, self._body_iterator = self._body_iterator, None
            try:
                # Close the handed-out iterator: a consumer that broke out of
                # `async for` abandoned its suspended generator, and GC must
                # never be the one to unwind it. Skip it while running —
                # close() re-enters from its own unwind path (`_iter_raw`'s
                # finally), and that unwind is already doing the closing.
                if iterator is not None and not iterator.ag_running:
                    await iterator.aclose()
            finally:
                await self.stream.close()

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        await self.close()
        return False

    def __del__(self) -> None:
        if not self.is_closed and not self.stream.closed:
            warnings.warn(
                f"Unclosed response {self!r}: read the body or call `close()` "
                "(or use `async with`) so the connection is released.",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )

    def __repr__(self) -> str:
        return f"<Response [{self.status_code} {self.reason_phrase}]>"


def _is_known_encoding(encoding: str) -> bool:
    try:
        codecs.lookup(encoding)
    except LookupError:
        return False
    return True
