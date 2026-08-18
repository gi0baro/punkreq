from __future__ import annotations

import json as jsonlib
import os
import typing
from collections.abc import AsyncIterable, Iterable, Mapping
from urllib.parse import urlencode

from ._exceptions import StreamConsumed
from ._urls import _primitive_value_to_str


__all__ = ["AsyncByteStream", "ByteStream"]

RequestContent = typing.Union[str, bytes, typing.Iterable[bytes], typing.AsyncIterable[bytes]]
RequestData = typing.Mapping[str, typing.Any]

_CHUNK_SIZE = 65_536


class AsyncByteStream:
    """Base class for request/response byte streams.

    Contract: `__aiter__` must return an async iterator supporting `aclose()`
    (an async generator satisfies this). punkreq closes every iterator it
    opens deterministically — finalization is never left to the GC, whose
    unwind of a suspended async generator cannot be relied upon."""

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        raise NotImplementedError()

    async def close(self) -> None:
        pass

    @property
    def closed(self) -> bool:
        """True when the stream holds no releasable resources (default), or has
        already been finalized."""
        return True


class ByteStream(AsyncByteStream):
    """An in-memory body. Replayable: each iteration yields the same content."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    @property
    def data(self) -> bytes:
        return self._data

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        return self._iterate()

    async def _iterate(self) -> typing.AsyncIterator[bytes]:
        if self._data:
            yield self._data

    def __repr__(self) -> str:
        return f"ByteStream({self._data!r})"


class IteratorByteStream(AsyncByteStream):
    """A body backed by a sync iterable of bytes. Single-shot."""

    def __init__(self, iterable: typing.Iterable[bytes]) -> None:
        self._iterable = iterable
        self._is_stream_consumed = False

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        if self._is_stream_consumed:
            raise StreamConsumed()
        self._is_stream_consumed = True
        return self._iterate()

    async def _iterate(self) -> typing.AsyncIterator[bytes]:
        if hasattr(self._iterable, "read"):
            # file-like: read in chunks rather than iterating (which yields lines)
            while chunk := self._iterable.read(_CHUNK_SIZE):
                yield chunk
        else:
            for chunk in self._iterable:
                yield chunk


class AsyncIteratorByteStream(AsyncByteStream):
    """A body backed by an async iterable of bytes. Single-shot.

    The iterable must support `aclose()` (an async generator does) so an
    aborted upload can close it deterministically; anything else is rejected
    up front rather than failing mid-teardown."""

    def __init__(self, aiterable: typing.AsyncIterable[bytes]) -> None:
        if not hasattr(aiterable, "aclose"):
            raise TypeError(
                "Async iterable content must support 'aclose()' so the body can be "
                "closed deterministically: pass an async generator, or implement 'aclose()'."
            )
        self._aiterable = aiterable
        self._is_stream_consumed = False

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        if self._is_stream_consumed:
            raise StreamConsumed()
        self._is_stream_consumed = True
        return self._iterate()

    async def _iterate(self) -> typing.AsyncIterator[bytes]:
        async for chunk in self._aiterable:
            yield chunk

    async def close(self) -> None:
        # `aclose` here is the async-generator protocol method, not punkreq
        # naming; its presence is guaranteed by the constructor check
        await self._aiterable.aclose()  # type: ignore[attr-defined]


def peek_filelike_length(stream: typing.Any) -> int | None:
    """The number of bytes remaining in a file-like object, if determinable."""
    try:
        fd = stream.fileno()
        length = os.fstat(fd).st_size
        offset = stream.tell()
    except (AttributeError, OSError):
        try:
            offset = stream.tell()
            length = stream.seek(0, os.SEEK_END)
            stream.seek(offset)
        except (AttributeError, OSError):
            return None
    return max(length - offset, 0)


def encode_content(content: RequestContent) -> tuple[dict[str, str], AsyncByteStream]:
    if isinstance(content, (bytes, str)):
        body = content.encode("utf-8") if isinstance(content, str) else content
        headers = {"Content-Length": str(len(body))} if body else {}
        return headers, ByteStream(body)
    if isinstance(content, Iterable):
        length = peek_filelike_length(content)
        headers = {"Content-Length": str(length)} if length is not None else {"Transfer-Encoding": "chunked"}
        return headers, IteratorByteStream(content)
    if isinstance(content, AsyncIterable):
        return {"Transfer-Encoding": "chunked"}, AsyncIteratorByteStream(content)
    raise TypeError(f"Unexpected type for 'content': {type(content)!r}")


def encode_urlencoded_data(data: RequestData) -> tuple[dict[str, str], AsyncByteStream]:
    pairs: list[tuple[str, str]] = []
    for key, value in data.items():
        if isinstance(value, (list, tuple)):
            pairs.extend((key, _primitive_value_to_str(item)) for item in value)
        else:
            pairs.append((key, _primitive_value_to_str(value)))
    body = urlencode(pairs).encode("utf-8")
    headers = {"Content-Length": str(len(body)), "Content-Type": "application/x-www-form-urlencoded"}
    return headers, ByteStream(body)


def encode_json(json: typing.Any) -> tuple[dict[str, str], AsyncByteStream]:
    body = jsonlib.dumps(json, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    headers = {"Content-Length": str(len(body)), "Content-Type": "application/json"}
    return headers, ByteStream(body)


def encode_request(
    content: RequestContent | None = None,
    data: RequestData | None = None,
    files: typing.Any = None,
    json: typing.Any = None,
    boundary: bytes | None = None,
) -> tuple[dict[str, str], AsyncByteStream]:
    """Encode body parameters into `(headers, stream)`.

    Precedence: `content` (raw) → `files` (multipart, with `data` as extra
    fields) → `data` (urlencoded form) → `json` → empty body.
    """
    if data is not None and not isinstance(data, Mapping):
        raise TypeError("The 'data' argument must be a mapping. Use 'content=...' for raw bytes or iterator content.")
    if content is not None:
        return encode_content(content)
    if files:
        # deferred import: _multipart builds on this module's stream classes,
        # so a top-level import here would be circular
        from ._multipart import MultipartStream

        stream = MultipartStream(data or {}, files, boundary)
        return stream.get_headers(), stream
    if data:
        return encode_urlencoded_data(data)
    if json is not None:
        return encode_json(json)
    return {}, ByteStream(b"")
