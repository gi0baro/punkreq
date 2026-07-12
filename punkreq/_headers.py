from __future__ import annotations

import typing
from collections.abc import Mapping, MutableMapping

from httpunk import HeaderMap


__all__ = ["Headers"]

HeaderTypes = typing.Union[
    "Headers",
    HeaderMap,
    typing.Mapping[str, str],
    typing.Iterable[typing.Tuple[str, str]],
    None,
]

_SENSITIVE_HEADERS = {"authorization", "proxy-authorization"}


def _decode(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.decode("latin-1")


def _encode(value: str | bytes) -> bytes:
    return value if isinstance(value, bytes) else value.encode("utf-8")


class Headers(MutableMapping[str, str]):
    def __init__(self, headers: HeaderTypes = None) -> None:
        if headers is None:
            self._map = HeaderMap()
        elif isinstance(headers, Headers):
            self._map = HeaderMap(headers._map)
        elif isinstance(headers, HeaderMap):
            self._map = HeaderMap(headers)
        else:
            self._map = HeaderMap()
            items = headers.items() if isinstance(headers, Mapping) else headers
            for key, value in items:
                self._map.add(key, _encode(value))

    @property
    def raw(self) -> list[tuple[str, bytes]]:
        """Every (name, value) pair with values as raw bytes, duplicates included."""
        return self._map.items()

    def get_list(self, key: str, split_commas: bool = False) -> list[str]:
        values = [_decode(value) for value in self._map.get_all(key)]
        if not split_commas:
            return values
        return [item.strip() for value in values for item in value.split(",")]

    def multi_items(self) -> list[tuple[str, str]]:
        return [(key, _decode(value)) for key, value in self._map.items()]

    def add(self, key: str, value: str | bytes) -> None:
        """Append a value for `key`, keeping any existing ones."""
        self._map.add(key, _encode(value))

    def update(self, headers: HeaderTypes = None) -> None:  # type: ignore[override]
        """Merge `headers` in: keys present in `headers` replace all existing values."""
        other = Headers(headers)
        for key in other._map.keys():
            if key in self._map:
                del self._map[key]
        for key, value in other._map.items():
            self._map.add(key, value)

    def setdefault(self, key: str, value: str | bytes = "") -> str:
        return _decode(self._map.setdefault(key, _encode(value)))

    def copy(self) -> Headers:
        return Headers(self)

    def __getitem__(self, key: str) -> str:
        """The value for `key`; multiple values are comma-joined."""
        values = self._map.get_all(key)
        if not values:
            raise KeyError(key)
        return ", ".join(_decode(value) for value in values)

    def __setitem__(self, key: str, value: str | bytes) -> None:
        self._map[key] = _encode(value)

    def __delitem__(self, key: str) -> None:
        if key not in self._map:
            raise KeyError(key)
        del self._map[key]

    def __contains__(self, key: typing.Any) -> bool:
        return key in self._map

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._map.keys())

    def __len__(self) -> int:
        return len(self._map.keys())

    def __eq__(self, other: typing.Any) -> bool:
        try:
            other_headers = other if isinstance(other, Headers) else Headers(other)
        except (TypeError, ValueError):
            return NotImplemented
        return sorted(self._map.items()) == sorted(other_headers._map.items())

    def __repr__(self) -> str:
        as_list = [
            (key, "[secure]" if key in _SENSITIVE_HEADERS else _decode(value)) for key, value in self._map.items()
        ]
        keys = [key for key, _ in as_list]
        if len(keys) == len(set(keys)):
            return f"Headers({dict(as_list)!r})"
        return f"Headers({as_list!r})"
