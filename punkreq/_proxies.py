from __future__ import annotations

import ssl
import typing
from urllib.parse import urlsplit

import httpunk
from httpunk.h1.client import H1Connection
from httpunk.h2.client import H2Connection
from httpunk.util.proxy import Matcher

from ._config import Proxy
from ._connect import Connector, Origin, connect_errors
from ._exceptions import ConnectError, ProxyError
from ._headers import Headers
from ._urls import URL


__all__ = ["ProxyConfig", "ProxyConnector", "ProxyInfo"]

ProxyTypes = typing.Union[Proxy, URL, str, None]

# attribute name tagging plain-http proxied connections for the transport
PROXY_ATTR = "punkreq_proxy"


class ProxyInfo(typing.NamedTuple):
    """Per-request proxy metadata for absolute-form (plain http) proxying."""

    auth: str | None  # ready `Proxy-Authorization` value
    headers: Headers


class ProxyConfig:
    """Resolved proxy configuration: a matcher plus proxy-level extras."""

    def __init__(
        self, matcher: Matcher, *, headers: Headers | None = None, ssl_context: ssl.SSLContext | None = None
    ) -> None:
        self.matcher = matcher
        self.headers = headers if headers is not None else Headers()
        self.ssl_context = ssl_context  # for TLS to the proxy itself (https:// proxies)

    @classmethod
    def resolve(cls, proxy: ProxyTypes, trust_env: bool) -> ProxyConfig | None:
        if proxy is not None:
            proxy = proxy if isinstance(proxy, Proxy) else Proxy(proxy)
            url = proxy.url
            if proxy.auth is not None:
                url = url.copy_with(username=proxy.auth[0], password=proxy.auth[1])
            matcher = Matcher.from_parts(all=str(url))
            return cls(matcher, headers=proxy.headers, ssl_context=proxy.ssl_context)
        if trust_env:
            return cls(Matcher.from_env())
        return None


class ProxyConnector:
    """Connector that routes each origin through its matched proxy, delegating
    unmatched origins to the direct `Connector`."""

    def __init__(
        self,
        *,
        backend: typing.Any,
        ssl_context: ssl.SSLContext,
        http1: bool = True,
        http2: bool = True,
        config: ProxyConfig,
    ) -> None:
        self._backend = backend
        self._ssl_context = ssl_context
        self._http1 = http1
        self._http2 = http2
        self._config = config
        # the direct connector configures the ALPN offer on `ssl_context` at
        # construction; `wrap_tls` (the CONNECT tunnel) takes it from the context
        self._direct = Connector(backend=backend, ssl_context=ssl_context, http1=http1, http2=http2)

    async def __call__(self, origin: Origin) -> typing.Any:
        intercept = self._config.matcher.intercept(f"{origin.scheme}://{origin.authority}/")
        if intercept is None:
            return await self._direct(origin)
        try:
            if origin.scheme == "http":
                return await self._absolute_form(origin, intercept)
            return await self._tunnel(origin, intercept)
        except connect_errors(self._backend) as exc:
            raise ConnectError(f"Failed to connect to {origin} via proxy {intercept.uri}: {exc}")

    async def _dial_proxy(self, intercept: typing.Any) -> typing.Any:
        parts = urlsplit(intercept.uri)
        scheme, host = parts.scheme, parts.hostname
        port = parts.port or (443 if scheme == "https" else 80)
        if scheme == "https":
            stream, _ = await self._backend.connect_tls(
                host, port, alpn=("http/1.1",), ssl_context=self._config.ssl_context
            )
            return stream
        return await self._backend.connect_tcp(host, port)

    def _proxy_headers(self, intercept: typing.Any, authority: str) -> httpunk.HeaderMap:
        headers = httpunk.HeaderMap({"host": authority})
        auth = intercept.basic_auth()
        if auth is not None:
            headers["proxy-authorization"] = auth
        for key, value in self._config.headers.raw:
            headers.add(key, value)
        return headers

    async def _absolute_form(self, origin: Origin, intercept: typing.Any) -> typing.Any:
        transport = await self._dial_proxy(intercept)
        conn = H1Connection(transport, authority=origin.authority, backend=self._backend)
        setattr(conn, PROXY_ATTR, ProxyInfo(auth=intercept.basic_auth(), headers=self._config.headers))
        return conn

    async def _tunnel(self, origin: Origin, intercept: typing.Any) -> typing.Any:
        transport = await self._dial_proxy(intercept)
        host = f"[{origin.host}]" if ":" in origin.host else origin.host
        target = f"{host}:{origin.port}"  # CONNECT wants authority form with an explicit port
        try:
            proxy_conn = H1Connection(transport, backend=self._backend)
            await proxy_conn.__aenter__()
            response = await proxy_conn.send_request(
                httpunk.Request("CONNECT", target, headers=self._proxy_headers(intercept, target))
            )
            if not (200 <= response.status < 300):
                raise ProxyError(f"The proxy refused CONNECT to {target}: {response.status}")
            tunnel = response.upgraded
            if tunnel is None:
                raise ProxyError(f"The proxy did not open a CONNECT tunnel to {target}")
        except BaseException:
            self._backend.close_transport(transport)
            raise

        # The tunnel's IO, and whatever was read past the CONNECT response (the
        # start of the origin's TLS conversation), go to the backend's TLS layer
        # (httpunk >= 0.4.0). On a handshake failure the backend closes the
        # transport itself before raising; `__call__` maps the error.
        stream, read_buf = tunnel.downcast()
        try:
            tls, selected = await self._backend.wrap_tls(
                stream, server_hostname=origin.host, ssl_context=self._ssl_context, prefix=read_buf
            )
        except TypeError as exc:
            # the backend refuses the stream at setup (tonio: no TLS over TLS, so an
            # https:// proxy cannot carry a CONNECT tunnel there)
            self._backend.close_transport(stream)
            raise ProxyError(f"Cannot open a TLS tunnel to {origin} via proxy {intercept.uri}: {exc}")

        if selected == "h2" or not self._http1:
            return H2Connection(tls, authority=origin.authority, scheme="https", backend=self._backend)
        return H1Connection(tls, authority=origin.authority, backend=self._backend)
