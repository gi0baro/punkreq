from __future__ import annotations

import os
import ssl
import typing

from httpunk.exceptions import HTTPunkError
from httpunk.h1.client import H1Connection
from httpunk.h2.client import H2Connection

from ._exceptions import ConnectError, UnsupportedProtocol


__all__ = ["Connector", "Origin", "connect_errors", "create_ssl_context", "enter_connection", "origin_for_url"]

DEFAULT_PORTS = {"http": 80, "https": 443}

CertTypes = typing.Union[
    str,
    typing.Tuple[str, typing.Optional[str]],
    typing.Tuple[str, typing.Optional[str], typing.Optional[str]],
]
VerifyTypes = typing.Union[ssl.SSLContext, str, bool]


class Origin(typing.NamedTuple):
    """A connection destination. Connections are pooled per origin."""

    scheme: str
    host: str
    port: int

    @property
    def authority(self) -> str:
        """host[:port] with the default port omitted and IPv6 hosts bracketed."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        if DEFAULT_PORTS.get(self.scheme) == self.port:
            return host
        return f"{host}:{self.port}"

    def __str__(self) -> str:
        return f"{self.scheme}://{self.authority}"


def origin_for_url(url: typing.Any) -> Origin:
    """The pooling key for a request URL. Rejects non-http(s) and relative URLs."""
    if url.is_relative_url:
        raise UnsupportedProtocol(f"Request URL is missing an 'http://' or 'https://' protocol: {str(url)!r}")
    if url.scheme not in DEFAULT_PORTS:
        raise UnsupportedProtocol(f"Request URL has an unsupported protocol {url.scheme!r}")
    return Origin(url.scheme, url.host, url.port or DEFAULT_PORTS[url.scheme])


def create_ssl_context(verify: VerifyTypes = True, cert: CertTypes | None = None) -> ssl.SSLContext:
    """Build the client TLS context.

    `verify` is True (system trust store), False (no verification), a CA bundle
    path (file or directory), or a ready `ssl.SSLContext` used as-is.
    `cert` is a client certificate: a path, or a (cert, key[, password]) tuple.
    """
    if isinstance(verify, ssl.SSLContext):
        context = verify
    elif verify is True:
        context = ssl.create_default_context()
    elif verify is False:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    elif isinstance(verify, str):
        context = ssl.create_default_context()
        if os.path.isdir(verify):
            context.load_verify_locations(capath=verify)
        else:
            context.load_verify_locations(cafile=verify)
    else:
        raise TypeError(f"Invalid type for 'verify': {type(verify)!r}")

    if cert is not None:
        if isinstance(cert, str):
            context.load_cert_chain(cert)
        else:
            context.load_cert_chain(*cert)
    return context


def connect_errors(backend: typing.Any) -> tuple[type[BaseException], ...]:
    """What a dial, a TLS handshake or the HTTP handshake fails with on
    `backend`: the OS and ssl errors, the backend's own transport failure, and
    httpunk's errors (the peer hanging up or misbehaving while the connection
    is entered, or during a proxy's CONNECT). tonio's `ResourceBroken`
    is a plain `Exception`, not an `OSError`, and its TLS layer raises it in
    place of the ssl error, so `(OSError, ssl.SSLError)` alone lets a failed
    handshake on tonio escape as an unmapped exception."""
    return (OSError, ssl.SSLError, HTTPunkError, *backend.broken_transport_errors)


async def enter_connection(conn: typing.Any, transport: typing.Any, backend: typing.Any) -> typing.Any:
    """Run `conn`'s HTTP handshake (`__aenter__`) and return it. Until that
    completes the transport we dialed is still ours to close: httpunk closes
    it on `__aexit__`, which a failed or interrupted `__aenter__` never
    reaches (h2 waits there for the peer's SETTINGS, pumps already running)."""
    try:
        await conn.__aenter__()
    except BaseException:
        backend.close_transport(transport)
        raise
    return conn


class Connector:
    """The default connector: dial `origin` and return the protocol-matching
    httpunk connection, entered."""

    def __init__(
        self,
        *,
        backend: typing.Any,
        ssl_context: ssl.SSLContext | None = None,
        http1: bool = True,
        http2: bool = True,
    ) -> None:
        if not http1 and not http2:
            raise ValueError("At least one of http1/http2 must be enabled")
        self._backend = backend
        self._ssl_context = ssl_context
        self._http1 = http1
        self._http2 = http2
        if http1 and http2:
            self.alpn: tuple[str, ...] = ("h2", "http/1.1")
        elif http2:
            self.alpn = ("h2",)
        else:
            self.alpn = ("http/1.1",)
        # The ALPN offer is configured on the context ONCE, here: a context is
        # shared by every dial that uses it (and by `ProxyConnector`'s CONNECT
        # tunnels, whose `wrap_tls` takes ALPN from the context alone), so the
        # dial path never mutates it (hyper-util / httpunk `util.connect`
        # discipline: a caller-supplied context is never touched per connect).
        if ssl_context is not None:
            ssl_context.set_alpn_protocols(list(self.alpn))

    async def __call__(self, origin: Origin) -> H1Connection | H2Connection:
        try:
            if origin.scheme == "https":
                conn, stream = await self._connect_tls(origin)
            else:
                conn, stream = await self._connect_tcp(origin)
            return await enter_connection(conn, stream, self._backend)
        except connect_errors(self._backend) as exc:
            raise ConnectError(f"Failed to connect to {origin}: {exc}")

    async def _connect_tls(self, origin: Origin) -> tuple[H1Connection | H2Connection, typing.Any]:
        # our context already carries the offer; a backend-created default one
        # (no context given) is configured per dial
        alpn = None if self._ssl_context is not None else self.alpn
        stream, selected = await self._backend.connect_tls(
            origin.host, origin.port, alpn=alpn, ssl_context=self._ssl_context
        )
        if selected == "h2" or not self._http1:
            return H2Connection(stream, authority=origin.authority, scheme="https", backend=self._backend), stream
        return H1Connection(stream, authority=origin.authority, backend=self._backend), stream

    async def _connect_tcp(self, origin: Origin) -> tuple[H1Connection | H2Connection, typing.Any]:
        stream = await self._backend.connect_tcp(origin.host, origin.port)
        if not self._http1:
            # h2 prior knowledge over cleartext (reqwest's http2_prior_knowledge)
            return H2Connection(stream, authority=origin.authority, scheme="http", backend=self._backend), stream
        return H1Connection(stream, authority=origin.authority, backend=self._backend), stream
