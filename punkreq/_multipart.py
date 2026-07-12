from __future__ import annotations

import mimetypes
import os
import typing

from ._content import AsyncByteStream, peek_filelike_length
from ._urls import _primitive_value_to_str


__all__ = ["MultipartStream"]

FileContent = typing.Union[typing.IO[bytes], bytes, str]
FileTypes = typing.Union[
    FileContent,
    typing.Tuple[typing.Optional[str], FileContent],
    typing.Tuple[typing.Optional[str], FileContent, typing.Optional[str]],
    typing.Tuple[typing.Optional[str], FileContent, typing.Optional[str], typing.Mapping[str, str]],
]
RequestFiles = typing.Union[
    typing.Mapping[str, FileTypes],
    typing.Sequence[typing.Tuple[str, FileTypes]],
]

_CHUNK_SIZE = 65_536


def _format_param(name: str, value: str) -> str:
    """Render a Content-Disposition parameter, browser-style escaped."""
    value = value.replace('"', "%22").replace("\r", "%0D").replace("\n", "%0A")
    return f'{name}="{value}"'


class _DataField:
    def __init__(self, name: str, value: typing.Any) -> None:
        self.name = name
        if isinstance(value, bytes):
            self.value = value
        else:
            self.value = _primitive_value_to_str(value).encode("utf-8")

    def render_headers(self) -> bytes:
        disposition = _format_param("name", self.name)
        return f"Content-Disposition: form-data; {disposition}\r\n\r\n".encode()

    def render_data(self) -> typing.Iterator[bytes]:
        yield self.value

    def get_length(self) -> int:
        return len(self.render_headers()) + len(self.value)


class _FileField:
    def __init__(self, name: str, value: FileTypes) -> None:
        self.name = name
        filename: str | None
        content_type: str | None
        headers: typing.Mapping[str, str] = {}

        if isinstance(value, tuple):
            if len(value) == 2:
                filename, fileobj = value  # type: ignore[misc]
                content_type = None
            elif len(value) == 3:
                filename, fileobj, content_type = value  # type: ignore[misc]
            elif len(value) == 4:
                filename, fileobj, content_type, headers = value  # type: ignore[misc]
            else:
                raise TypeError(f"Expected a 2, 3 or 4 element tuple for file {name!r}, got {len(value)} elements")
        else:
            fileobj = value
            filename = None
            content_type = None

        if filename is None:
            name_attr = getattr(fileobj, "name", None)
            filename = os.path.basename(name_attr) if isinstance(name_attr, str) else None
        if content_type is None and filename:
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        if isinstance(fileobj, str):
            fileobj = fileobj.encode("utf-8")

        self.filename = filename
        self.fileobj: typing.IO[bytes] | bytes = fileobj
        self.content_type = content_type
        self.headers = dict(headers)

    def render_headers(self) -> bytes:
        parts = [_format_param("name", self.name)]
        if self.filename is not None:
            parts.append(_format_param("filename", self.filename))
        lines = [f"Content-Disposition: form-data; {'; '.join(parts)}"]
        if self.content_type is not None and "content-type" not in {key.lower() for key in self.headers}:
            lines.append(f"Content-Type: {self.content_type}")
        lines.extend(f"{key}: {value}" for key, value in self.headers.items())
        return ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")

    def render_data(self) -> typing.Iterator[bytes]:
        if isinstance(self.fileobj, bytes):
            yield self.fileobj
            return
        while chunk := self.fileobj.read(_CHUNK_SIZE):
            yield chunk

    def get_length(self) -> int | None:
        if isinstance(self.fileobj, bytes):
            data_length: int | None = len(self.fileobj)
        else:
            data_length = peek_filelike_length(self.fileobj)
        if data_length is None:
            return None
        return len(self.render_headers()) + data_length


class MultipartStream(AsyncByteStream):
    def __init__(
        self, data: typing.Mapping[str, typing.Any], files: RequestFiles, boundary: bytes | None = None
    ) -> None:
        self.boundary = boundary if boundary is not None else os.urandom(16).hex().encode("ascii")
        self.fields: list[_DataField | _FileField] = []
        for name, value in data.items():
            if isinstance(value, (list, tuple)):
                self.fields.extend(_DataField(name, item) for item in value)
            else:
                self.fields.append(_DataField(name, value))
        file_items = files.items() if isinstance(files, typing.Mapping) else files
        self.fields.extend(_FileField(name, value) for name, value in file_items)

    def _iter_chunks(self) -> typing.Iterator[bytes]:
        for field in self.fields:
            yield b"--" + self.boundary + b"\r\n"
            yield field.render_headers()
            yield from field.render_data()
            yield b"\r\n"
        yield b"--" + self.boundary + b"--\r\n"

    def get_content_length(self) -> int | None:
        total = 0
        for field in self.fields:
            length = field.get_length()
            if length is None:
                return None
            # per-part framing: --boundary\r\n ... \r\n
            total += 2 + len(self.boundary) + 2 + length + 2
        return total + 2 + len(self.boundary) + 4  # closing --boundary--\r\n

    def get_headers(self) -> dict[str, str]:
        content_type = f"multipart/form-data; boundary={self.boundary.decode('ascii')}"
        length = self.get_content_length()
        if length is None:
            return {"Content-Type": content_type, "Transfer-Encoding": "chunked"}
        return {"Content-Type": content_type, "Content-Length": str(length)}

    def __aiter__(self) -> typing.AsyncIterator[bytes]:
        return self._aiterate()

    async def _aiterate(self) -> typing.AsyncIterator[bytes]:
        for chunk in self._iter_chunks():
            yield chunk
