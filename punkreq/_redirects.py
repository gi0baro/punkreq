from __future__ import annotations

from ._content import ByteStream
from ._exceptions import RemoteProtocolError, UnsupportedProtocol
from ._models import Request, Response, host_header_value
from ._urls import URL


__all__ = ["build_redirect_request"]

SENSITIVE_HEADERS = ("authorization", "cookie", "proxy-authorization")
_BODY_HEADERS = ("content-length", "content-type", "content-encoding", "transfer-encoding")


def _redirect_url(request: Request, response: Response) -> URL:
    location = response.headers["location"]
    try:
        url = request.url.join(location)
    except Exception as exc:
        raise RemoteProtocolError(f"Invalid Location header on redirect: {location!r} ({exc})", request=request)
    if url.scheme not in ("http", "https"):
        raise UnsupportedProtocol(f"Redirect to unsupported protocol {url.scheme!r}", request=request)
    if not url.fragment and request.url.fragment:
        url = url.copy_with(fragment=request.url.fragment)
    return url


def _redirect_method(request: Request, status: int) -> str:
    method = request.method
    if status == 303 and method != "HEAD":
        return "GET"
    if status in (301, 302) and method == "POST":
        return "GET"
    return method


def build_redirect_request(request: Request, response: Response, *, referer: bool = True) -> Request | None:
    """The request for the next hop, or None when the redirect must not be
    followed (a preserved body that cannot be replayed)."""
    status = response.status_code
    url = _redirect_url(request, response)
    method = _redirect_method(request, status)

    headers = request.headers.copy()

    body_dropped = method != request.method and request.method != "HEAD"
    if body_dropped:
        stream = ByteStream(b"")
        for name in _BODY_HEADERS:
            if name in headers:
                del headers[name]
    else:
        stream = request.stream
        has_body = not (isinstance(stream, ByteStream) and not stream.data)
        if has_body and not isinstance(stream, ByteStream):
            return None  # non-replayable body: do not follow

    cross_origin = (url.scheme, url.host, url.port) != (request.url.scheme, request.url.host, request.url.port)
    if cross_origin:
        for name in SENSITIVE_HEADERS:
            if name in headers:
                del headers[name]

    headers["host"] = host_header_value(url)

    downgrade = request.url.scheme == "https" and url.scheme == "http"
    if referer and not downgrade:
        headers["referer"] = str(request.url.copy_with(userinfo="", fragment=None))

    # the redirect hop inherits the per-request timeout and the pinned
    # total-timeout deadline, so `total` spans the whole chain
    next_request = Request(method, url, headers=headers, stream=stream, timeout=request.timeout)
    next_request._deadline = request._deadline
    return next_request
