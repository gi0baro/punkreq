from __future__ import annotations

import ssl
import typing
from urllib.parse import urlsplit

import httpunk
from httpunk.h1.client import H1Connection
from httpunk.h2.client import H2Connection
from httpunk.util.proxy import Matcher

from ._config import Proxy
from ._connect import Connector, Origin
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


class TunnelTLSStream:
    """TLS over an arbitrary bidirectional stream (the CONNECT tunnel), via
    `ssl.MemoryBIO`. Presents the httpunk stream interface (`receive_some`,
    `send_all`, `close`, `read_nowait`) so H1/H2 connections sit on it directly."""

    def __init__(
        self,
        inner: typing.Any,
        context: ssl.SSLContext,
        *,
        server_hostname: str,
        closer: typing.Callable[[], None],
    ) -> None:
        self._inner = inner
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl = context.wrap_bio(self._incoming, self._outgoing, server_hostname=server_hostname)
        self._closer = closer
        self._closed = False

    async def handshake(self) -> None:
        while True:
            try:
                self._ssl.do_handshake()
                break
            except ssl.SSLWantReadError:
                await self._flush()
                data = await self._inner.receive_some(65536)
                if not data:
                    raise ssl.SSLError("connection closed during TLS handshake")
                self._incoming.write(data)
        await self._flush()

    def selected_alpn_protocol(self) -> str | None:
        return self._ssl.selected_alpn_protocol()

    async def _flush(self) -> None:
        data = self._outgoing.read()
        if data:
            await self._inner.send_all(data)

    async def receive_some(self, max_bytes: int = 65536) -> bytes:
        while True:
            try:
                return self._ssl.read(max_bytes)
            except ssl.SSLWantReadError:
                await self._flush()
                raw = await self._inner.receive_some(65536)
                if not raw:
                    return b""  # ragged EOF: treated as end-of-stream
                self._incoming.write(raw)
            except ssl.SSLZeroReturnError:
                return b""

    async def send_all(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            try:
                sent = self._ssl.write(view)
            except ssl.SSLWantReadError:
                raw = await self._inner.receive_some(65536)
                if not raw:
                    raise ssl.SSLError("connection closed during TLS write")
                self._incoming.write(raw)
                continue
            await self._flush()
            view = view[sent:]
        await self._flush()

    def read_nowait(self, max_bytes: int = 65536) -> bytes:
        """Decrypted bytes already buffered in the SSL object, without touching
        the tunnel (the h1 driver's pre-request unexpected-bytes check)."""
        try:
            return self._ssl.read(max_bytes)
        except (ssl.SSLWantReadError, ssl.SSLZeroReturnError):
            return b""

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._closer()


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
        self._direct = Connector(backend=backend, ssl_context=ssl_context, http1=http1, http2=http2)
        if http1 and http2:
            alpn: list[str] = ["h2", "http/1.1"]
        elif http2:
            alpn = ["h2"]
        else:
            alpn = ["http/1.1"]
        # wrap_bio takes ALPN from the context; the direct dial path sets the
        # same list on the same context, so this stays consistent
        ssl_context.set_alpn_protocols(alpn)

    async def __call__(self, origin: Origin) -> typing.Any:
        intercept = self._config.matcher.intercept(f"{origin.scheme}://{origin.authority}/")
        if intercept is None:
            return await self._direct(origin)
        try:
            if origin.scheme == "http":
                return await self._absolute_form(origin, intercept)
            return await self._tunnel(origin, intercept)
        except (OSError, ssl.SSLError) as exc:
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

        tls = TunnelTLSStream(
            tunnel,
            self._ssl_context,
            server_hostname=origin.host,
            closer=lambda: self._backend.close_transport(transport),
        )
        try:
            await tls.handshake()
        except ssl.SSLError as exc:
            self._backend.close_transport(transport)
            raise ConnectError(f"TLS handshake with {origin} through the proxy failed: {exc}")

        if tls.selected_alpn_protocol() == "h2" or not self._http1:
            return H2Connection(tls, authority=origin.authority, scheme="https", backend=self._backend)
        return H1Connection(tls, authority=origin.authority, backend=self._backend)
