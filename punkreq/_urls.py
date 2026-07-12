from __future__ import annotations

import typing
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit, urlunsplit

from ._exceptions import InvalidURL


__all__ = ["URL", "QueryParams"]

PrimitiveData = typing.Union[str, int, float, bool, None]

QueryParamTypes = typing.Union[
    "QueryParams",
    typing.Mapping[str, typing.Union[PrimitiveData, typing.Sequence[PrimitiveData]]],
    typing.Sequence[typing.Tuple[str, PrimitiveData]],
    str,
    bytes,
    None,
]

_SCHEME_DEFAULT_PORTS = {
    "ftp": 21,
    "http": 80,
    "https": 443,
    "ws": 80,
    "wss": 443,
}

# Characters left untouched when percent-encoding a path. '%' is included so
# already-encoded input is not double-encoded.
_PATH_SAFE = "/%:@!$&'()*+,;=~"

_URL_COMPONENTS = frozenset(
    {
        "scheme",
        "username",
        "password",
        "userinfo",
        "host",
        "port",
        "netloc",
        "path",
        "query",
        "raw_path",
        "fragment",
        "params",
    }
)


def _primitive_value_to_str(value: PrimitiveData) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return ""
    return str(value)


def _idna_encode(host: str) -> str:
    if host.isascii():
        return host.lower()
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError:
        raise InvalidURL(f"Invalid IDNA hostname: {host!r}")


def _normalize_port(port: int | str | None, scheme: str) -> int | None:
    if port is None or port == "":
        return None
    try:
        port_int = int(port)
    except ValueError:
        raise InvalidURL(f"Invalid port: {port!r}")
    if not 0 < port_int < 65536:
        raise InvalidURL(f"Port out of range: {port_int}")
    if _SCHEME_DEFAULT_PORTS.get(scheme) == port_int:
        return None
    return port_int


def _quote_path(path: str) -> str:
    return quote(path, safe=_PATH_SAFE)


def _parse_netloc(netloc: str) -> tuple[str, str, str | None]:
    """Split a netloc into (userinfo, host, port-or-None). Host keeps IPv6 brackets."""
    userinfo, _, hostport = netloc.rpartition("@")
    if hostport.startswith("["):
        host, _, rest = hostport.partition("]")
        host = host[1:]
        port = rest[1:] if rest.startswith(":") else None
    else:
        host, sep, port_str = hostport.partition(":")
        port = port_str if sep else None
    return userinfo, host, port


class QueryParams:
    """An immutable multidict of query parameters.

    Mutating operations (`set`, `add`, `remove`, `merge`) return new instances.
    """

    __slots__ = ("_dict",)

    def __init__(self, *args: QueryParamTypes, **kwargs: typing.Any) -> None:
        if len(args) > 1:
            raise TypeError("QueryParams() takes at most one positional argument")
        if args and args[0] is not None and kwargs:
            raise TypeError("Cannot mix positional and keyword arguments in QueryParams()")
        value = kwargs if kwargs else (args[0] if args else None)

        d: dict[str, list[str]] = {}
        if value is None:
            pass
        elif isinstance(value, QueryParams):
            d = {k: list(v) for k, v in value._dict.items()}
        elif isinstance(value, (str, bytes)):
            query = value.decode("ascii") if isinstance(value, bytes) else value
            query = query.removeprefix("?")
            for key, val in parse_qsl(query, keep_blank_values=True):
                d.setdefault(key, []).append(val)
        else:
            items: typing.Iterable[tuple[str, typing.Any]]
            items = value.items() if isinstance(value, typing.Mapping) else value
            for key, val in items:
                if isinstance(val, (list, tuple)):
                    for item in val:
                        d.setdefault(str(key), []).append(_primitive_value_to_str(item))
                else:
                    d.setdefault(str(key), []).append(_primitive_value_to_str(val))
        self._dict = d

    def keys(self) -> typing.KeysView[str]:
        return self._dict.keys()

    def values(self) -> list[str]:
        """The first value for each key."""
        return [values[0] for values in self._dict.values()]

    def items(self) -> list[tuple[str, str]]:
        """(key, first value) pairs."""
        return [(key, values[0]) for key, values in self._dict.items()]

    def multi_items(self) -> list[tuple[str, str]]:
        """Every (key, value) pair, duplicates included."""
        return [(key, value) for key, values in self._dict.items() for value in values]

    def get(self, key: typing.Any, default: typing.Any = None) -> typing.Any:
        values = self._dict.get(str(key))
        return values[0] if values else default

    def get_list(self, key: typing.Any) -> list[str]:
        return list(self._dict.get(str(key), []))

    def set(self, key: typing.Any, value: PrimitiveData = None) -> QueryParams:
        params = QueryParams(self)
        params._dict[str(key)] = [_primitive_value_to_str(value)]
        return params

    def add(self, key: typing.Any, value: PrimitiveData = None) -> QueryParams:
        params = QueryParams(self)
        params._dict.setdefault(str(key), []).append(_primitive_value_to_str(value))
        return params

    def remove(self, key: typing.Any) -> QueryParams:
        params = QueryParams(self)
        params._dict.pop(str(key), None)
        return params

    def merge(self, params: QueryParamTypes = None) -> QueryParams:
        """New instance with `params` merged in; keys present in `params` replace existing ones."""
        merged = QueryParams(params)
        merged._dict = {**self._dict, **merged._dict}
        return merged

    def __getitem__(self, key: typing.Any) -> str:
        return self._dict[str(key)][0]

    def __contains__(self, key: typing.Any) -> bool:
        return str(key) in self._dict

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def __bool__(self) -> bool:
        return bool(self._dict)

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, QueryParams):
            return NotImplemented
        return sorted(self.multi_items()) == sorted(other.multi_items())

    def __setitem__(self, key: typing.Any, value: typing.Any) -> typing.NoReturn:
        raise TypeError("QueryParams is immutable; use `.set()`, `.add()`, `.remove()` or `.merge()` instead")

    def update(self, params: typing.Any = None) -> typing.NoReturn:
        raise TypeError("QueryParams is immutable; use `.set()`, `.add()`, `.remove()` or `.merge()` instead")

    def __str__(self) -> str:
        return urlencode(self.multi_items())

    def __repr__(self) -> str:
        return f"QueryParams({str(self)!r})"


class URL:
    """An immutable URL, decomposed into components.

    `URL(url, **components)` parses `url` then applies component overrides;
    `copy_with(**components)` returns an updated copy. Percent-decoded values
    are exposed by `path`, `fragment`, `username` and `password`; `raw_path`,
    `query` and `userinfo` stay in wire form.
    """

    __slots__ = ("_scheme", "_userinfo", "_host", "_port", "_path", "_query", "_fragment")

    def __init__(self, url: URL | str = "", **components: typing.Any) -> None:
        if isinstance(url, URL):
            self._scheme = url._scheme
            self._userinfo = url._userinfo
            self._host = url._host
            self._port = url._port
            self._path = url._path
            self._query = url._query
            self._fragment = url._fragment
        elif isinstance(url, str):
            try:
                parts = urlsplit(url)
                port = parts.port
            except ValueError as exc:
                raise InvalidURL(str(exc))
            self._scheme = parts.scheme
            self._userinfo, host, _ = _parse_netloc(parts.netloc)
            self._host = _idna_encode(host)
            self._port = _normalize_port(port, parts.scheme)
            self._path = _quote_path(parts.path)
            self._query = parts.query or None
            self._fragment = parts.fragment or None
        else:
            raise TypeError(f"Invalid type for url: {type(url)!r}")

        if components:
            self._apply_components(components)

    def _apply_components(self, components: dict[str, typing.Any]) -> None:
        unknown = set(components) - _URL_COMPONENTS
        if unknown:
            raise TypeError(f"Invalid URL component(s): {sorted(unknown)!r}")

        if "scheme" in components:
            self._scheme = (components["scheme"] or "").lower()
            self._port = _normalize_port(self._port, self._scheme)
        if "netloc" in components:
            userinfo, host, port = _parse_netloc(components["netloc"] or "")
            self._userinfo = userinfo
            self._host = _idna_encode(host)
            self._port = _normalize_port(port, self._scheme)
        if "userinfo" in components:
            self._userinfo = components["userinfo"] or ""
        if "username" in components or "password" in components:
            username = components.get("username", self.username) or ""
            password = components.get("password", self.password) or ""
            userinfo = quote(username, safe="")
            if password:
                userinfo += ":" + quote(password, safe="")
            self._userinfo = userinfo
        if "host" in components:
            host = components["host"] or ""
            host = host.removeprefix("[").removesuffix("]")
            self._host = _idna_encode(host)
        if "port" in components:
            self._port = _normalize_port(components["port"], self._scheme)
        if "raw_path" in components:
            raw_path = components["raw_path"] or ""
            path, sep, query = raw_path.partition("?")
            self._path = path
            self._query = query if sep else None
        if "path" in components:
            self._path = _quote_path(components["path"] or "")
        if "query" in components:
            query = components["query"]
            self._query = query.removeprefix("?") if query else None
        if "params" in components:
            params = components["params"]
            self._query = str(QueryParams(params)) or None if params is not None else None
        if "fragment" in components:
            self._fragment = components["fragment"] or None

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def userinfo(self) -> str:
        """The userinfo component in wire (percent-encoded) form, e.g. `user:pass`."""
        return self._userinfo

    @property
    def username(self) -> str:
        return unquote(self._userinfo.partition(":")[0])

    @property
    def password(self) -> str:
        return unquote(self._userinfo.partition(":")[2])

    @property
    def host(self) -> str:
        """The hostname, lowercased; punycode form for IDNA hosts, unbracketed for IPv6."""
        return self._host

    @property
    def port(self) -> int | None:
        """The port, or None when absent or the default for the scheme."""
        return self._port

    @property
    def netloc(self) -> str:
        host = f"[{self._host}]" if ":" in self._host else self._host
        netloc = host if self._port is None else f"{host}:{self._port}"
        if self._userinfo:
            netloc = f"{self._userinfo}@{netloc}"
        return netloc

    @property
    def path(self) -> str:
        return unquote(self._path) or "/"

    @property
    def raw_path(self) -> str:
        """Path plus query in wire form: what goes into an HTTP/1 request target."""
        raw = self._path or "/"
        if self._query is not None:
            raw += f"?{self._query}"
        return raw

    @property
    def query(self) -> str:
        return self._query or ""

    @property
    def params(self) -> QueryParams:
        return QueryParams(self._query)

    @property
    def fragment(self) -> str:
        return unquote(self._fragment or "")

    @property
    def is_absolute_url(self) -> bool:
        return bool(self._scheme and self._host)

    @property
    def is_relative_url(self) -> bool:
        return not self.is_absolute_url

    def copy_with(self, **components: typing.Any) -> URL:
        return URL(self, **components)

    def copy_set_param(self, key: typing.Any, value: PrimitiveData = None) -> URL:
        return self.copy_with(params=self.params.set(key, value))

    def copy_add_param(self, key: typing.Any, value: PrimitiveData = None) -> URL:
        return self.copy_with(params=self.params.add(key, value))

    def copy_remove_param(self, key: typing.Any) -> URL:
        return self.copy_with(params=self.params.remove(key))

    def copy_merge_params(self, params: QueryParamTypes) -> URL:
        return self.copy_with(params=self.params.merge(params))

    def join(self, url: URL | str) -> URL:
        """Resolve `url` against this one, RFC 3986 style."""
        return URL(urljoin(str(self), str(URL(url))))

    def __str__(self) -> str:
        return urlunsplit((self._scheme, self.netloc, self._path, self._query or "", self._fragment or ""))

    def __eq__(self, other: typing.Any) -> bool:
        if isinstance(other, str):
            return str(self) == other
        if isinstance(other, URL):
            return str(self) == str(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(str(self))

    def __repr__(self) -> str:
        url = str(self)
        if ":" in self._userinfo:
            username = self._userinfo.partition(":")[0]
            url = url.replace(f"{self._userinfo}@", f"{username}:[secure]@", 1)
        return f"URL({url!r})"
