from __future__ import annotations

import typing


__all__ = [
    "CloseError",
    "ConnectError",
    "ConnectTimeout",
    "CookieConflict",
    "DecodingError",
    "HTTPError",
    "HTTPStatusError",
    "InvalidURL",
    "LocalProtocolError",
    "NetworkError",
    "PoolTimeout",
    "ProtocolError",
    "ProxyError",
    "ReadError",
    "ReadTimeout",
    "RemoteProtocolError",
    "RequestError",
    "RequestNotRead",
    "StreamClosed",
    "StreamConsumed",
    "StreamError",
    "TimeoutException",
    "TooManyRedirects",
    "TransportError",
    "UnsupportedProtocol",
    "WriteError",
]


class HTTPError(Exception):
    """Base class for `RequestError` and `HTTPStatusError`."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
        self._request: typing.Any = None

    @property
    def request(self) -> typing.Any:
        if self._request is None:
            raise RuntimeError("The .request property has not been set.")
        return self._request

    @request.setter
    def request(self, request: typing.Any) -> None:
        self._request = request


class RequestError(HTTPError):
    """Base class for all exceptions that may occur while issuing a request."""

    def __init__(self, message: str, *, request: typing.Any = None) -> None:
        super().__init__(message)
        self._request = request


class TransportError(RequestError):
    """Base class for exceptions occurring at the transport level (dialing, I/O, protocol)."""


class TimeoutException(TransportError):
    """A timeout occurred. Base class for the phase-specific flavors, and
    raised directly when the total request deadline is exceeded."""


class ConnectTimeout(TimeoutException):
    """Timed out while establishing a connection."""


class ReadTimeout(TimeoutException):
    """Timed out while receiving data."""


class PoolTimeout(TimeoutException):
    """Timed out while waiting for a connection from the pool."""


class NetworkError(TransportError):
    """A socket-level failure. Base class for the four I/O flavors."""


class ConnectError(NetworkError):
    """Failed to establish a connection."""


class ReadError(NetworkError):
    """Failed while receiving data."""


class WriteError(NetworkError):
    """Failed while sending data."""


class CloseError(NetworkError):
    """Failed while closing a connection."""


class ProtocolError(TransportError):
    """The HTTP protocol was violated."""


class LocalProtocolError(ProtocolError):
    """The client violated the protocol (e.g. an invalid request)."""


class RemoteProtocolError(ProtocolError):
    """The server violated the protocol (e.g. a malformed response)."""


class ProxyError(TransportError):
    """An error occurred while establishing a proxy connection."""


class UnsupportedProtocol(TransportError):
    """The request URL scheme is not supported (only http:// and https:// are)."""


class DecodingError(RequestError):
    """Failed to decode the response body (content decoding or charset)."""


class TooManyRedirects(RequestError):
    """The redirect hop limit was exceeded."""


class HTTPStatusError(HTTPError):
    """A 4xx or 5xx response, raised by `Response.raise_for_status()`.

    Carries both `.request` and `.response`.
    """

    def __init__(self, message: str, *, request: typing.Any, response: typing.Any) -> None:
        super().__init__(message)
        self._request = request
        self.response = response


class InvalidURL(Exception):
    """A URL was improperly formed or could not be parsed."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class CookieConflict(Exception):
    """Multiple cookies matched a `Cookies.get()` lookup ambiguously."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class StreamError(RuntimeError):
    """Misuse of a streaming API: a programming error, not a transport failure."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class StreamConsumed(StreamError):
    def __init__(self) -> None:
        super().__init__("Attempted to read or stream some content, but the content has already been streamed.")


class StreamClosed(StreamError):
    def __init__(self) -> None:
        super().__init__("Attempted to read or stream content, but the stream has been closed.")


class RequestNotRead(StreamError):
    def __init__(self) -> None:
        super().__init__("Attempted to access streaming request content, without having called `read()` or `aread()`.")
