from __future__ import annotations

import ssl
import typing

from ._headers import Headers, HeaderTypes
from ._urls import URL


__all__ = ["Limits", "Proxy", "Timeout"]


class _UnsetType:
    def __repr__(self) -> str:
        return "UNSET"


UNSET = _UnsetType()

TimeoutTypes = typing.Union[
    float,
    None,
    typing.Tuple[typing.Optional[float], typing.Optional[float], typing.Optional[float], typing.Optional[float]],
    "Timeout",
]


class Timeout:
    """Timeout configuration. Each field is seconds, or None (disabled).

    * `total` — a deadline for the entire request: connecting, every redirect
      hop, and reading the full body (reqwest's primary timeout).
    * `connect` — establishing a connection (dial, TLS, protocol handshake).
    * `read` — each read: waiting for the response head, and each body chunk.
    * `pool` — waiting for a connection from the pool.

    There is deliberately no `write` timeout: request bodies are written from
    background writer tasks (httpunk is full-duplex, like hyper), so there is
    no foreground write operation to bound — `total` covers slow uploads.

    Usage:
        Timeout(None)               # no timeouts (the punkreq default)
        Timeout(5.0)                # 5s on every field, total included
        Timeout(None, total=30.0)   # only a whole-request deadline
        Timeout(5.0, total=30.0)    # 5s connect/read/pool within a 30s deadline
        Timeout(connect=5.0, read=5.0, pool=5.0, total=30.0)

    Also accepts a `(connect, read, pool, total)` tuple or another `Timeout`
    (keyword fields override the copy). Without a positional default, all four
    fields must be given explicitly.
    """

    __slots__ = ("connect", "read", "pool", "total")

    def __init__(
        self,
        timeout: TimeoutTypes | _UnsetType = UNSET,
        *,
        connect: float | None | _UnsetType = UNSET,
        read: float | None | _UnsetType = UNSET,
        pool: float | None | _UnsetType = UNSET,
        total: float | None | _UnsetType = UNSET,
    ) -> None:
        if isinstance(timeout, Timeout):
            self.connect = timeout.connect if isinstance(connect, _UnsetType) else connect
            self.read = timeout.read if isinstance(read, _UnsetType) else read
            self.pool = timeout.pool if isinstance(pool, _UnsetType) else pool
            self.total = timeout.total if isinstance(total, _UnsetType) else total
        elif isinstance(timeout, tuple):
            if len(timeout) != 4:
                raise ValueError("Timeout tuples must be (connect, read, pool, total)")
            self.connect, self.read, self.pool, self.total = timeout
        elif isinstance(timeout, _UnsetType):
            if any(isinstance(value, _UnsetType) for value in (connect, read, pool, total)):
                raise ValueError("Timeout must either include a default, or set all four parameters explicitly")
            self.connect = typing.cast("float | None", connect)
            self.read = typing.cast("float | None", read)
            self.pool = typing.cast("float | None", pool)
            self.total = typing.cast("float | None", total)
        else:
            self.connect = timeout if isinstance(connect, _UnsetType) else connect
            self.read = timeout if isinstance(read, _UnsetType) else read
            self.pool = timeout if isinstance(pool, _UnsetType) else pool
            self.total = timeout if isinstance(total, _UnsetType) else total

    def as_dict(self) -> dict[str, float | None]:
        return {"connect": self.connect, "read": self.read, "pool": self.pool, "total": self.total}

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, Timeout):
            return NotImplemented
        return self.as_dict() == other.as_dict()

    def __repr__(self) -> str:
        if self.connect == self.read == self.pool == self.total:
            return f"Timeout(timeout={self.connect})"
        return f"Timeout(connect={self.connect}, read={self.read}, pool={self.pool}, total={self.total})"


class Limits:
    """Connection pool limits.

    * `max_connections` — total concurrent connections; None means unlimited.
    * `max_keepalive_connections` — idle connections kept for reuse; None means
      unlimited.
    * `keepalive_expiry` — seconds an idle connection survives in the pool;
      None disables expiry. Defaults to 90s, matching reqwest.
    """

    __slots__ = ("max_connections", "max_keepalive_connections", "keepalive_expiry")

    def __init__(
        self,
        *,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = 90.0,
    ) -> None:
        self.max_connections = max_connections
        self.max_keepalive_connections = max_keepalive_connections
        self.keepalive_expiry = keepalive_expiry

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, Limits):
            return NotImplemented
        return (
            self.max_connections == other.max_connections
            and self.max_keepalive_connections == other.max_keepalive_connections
            and self.keepalive_expiry == other.keepalive_expiry
        )

    def __repr__(self) -> str:
        return (
            f"Limits(max_connections={self.max_connections}, "
            f"max_keepalive_connections={self.max_keepalive_connections}, "
            f"keepalive_expiry={self.keepalive_expiry})"
        )


DEFAULT_LIMITS = Limits()


class Proxy:
    """A proxy to route requests through.

    `url` must be http:// or https:// (SOCKS is not supported yet). Credentials
    may be given via `auth=(user, pass)` or inline in the URL userinfo, which is
    extracted and stripped.
    """

    __slots__ = ("url", "auth", "headers", "ssl_context")

    def __init__(
        self,
        url: URL | str,
        *,
        auth: tuple[str, str] | None = None,
        headers: HeaderTypes = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        url = URL(url)
        if url.scheme not in ("http", "https"):
            raise ValueError(f"Unsupported proxy scheme {url.scheme!r}")
        if url.username or url.password:
            auth = (url.username, url.password)
            url = url.copy_with(userinfo="")
        self.url = url
        self.auth = auth
        self.headers = Headers(headers)
        self.ssl_context = ssl_context

    def __repr__(self) -> str:
        auth = f"('{self.auth[0]}', '[secure]')" if self.auth else "None"
        return f"Proxy(url={str(self.url)!r}, auth={auth}, headers={self.headers!r})"
