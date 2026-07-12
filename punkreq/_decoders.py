from __future__ import annotations

import codecs
import io
import typing
import zlib

from ._exceptions import DecodingError


try:
    import brotlicffi as brotli
except ImportError:
    try:
        import brotli
    except ImportError:
        brotli = None

try:
    from compression import zstd
except ImportError:
    zstd = None


class ContentDecoder:
    def decode(self, data: bytes) -> bytes:
        raise NotImplementedError()

    def flush(self) -> bytes:
        raise NotImplementedError()


class IdentityDecoder(ContentDecoder):
    def decode(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


class DeflateDecoder(ContentDecoder):
    """Handles both RFC 2616 flavors: zlib-wrapped data, and (from servers that
    got it wrong, historically common) raw deflate — detected on the first chunk."""

    def __init__(self) -> None:
        self._first_attempt = True
        self._decompressor = zlib.decompressobj()

    def decode(self, data: bytes) -> bytes:
        was_first_attempt = self._first_attempt
        self._first_attempt = False
        try:
            return self._decompressor.decompress(data)
        except zlib.error as exc:
            if was_first_attempt:
                self._decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                try:
                    return self._decompressor.decompress(data)
                except zlib.error as raw_exc:
                    raise DecodingError(str(raw_exc))
            raise DecodingError(str(exc))

    def flush(self) -> bytes:
        try:
            return self._decompressor.flush()
        except zlib.error as exc:
            raise DecodingError(str(exc))


class GzipDecoder(ContentDecoder):
    def __init__(self) -> None:
        self._decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)

    def decode(self, data: bytes) -> bytes:
        try:
            return self._decompressor.decompress(data)
        except zlib.error as exc:
            raise DecodingError(str(exc))

    def flush(self) -> bytes:
        try:
            return self._decompressor.flush()
        except zlib.error as exc:
            raise DecodingError(str(exc))


class BrotliDecoder(ContentDecoder):
    def __init__(self) -> None:
        self._decompressor = brotli.Decompressor()
        # google's `brotli` exposes `decompress`, `brotlicffi` exposes `process`
        self._decode = getattr(self._decompressor, "decompress", None) or self._decompressor.process

    def decode(self, data: bytes) -> bytes:
        try:
            return self._decode(data)
        except brotli.error as exc:
            raise DecodingError(str(exc))

    def flush(self) -> bytes:
        try:
            if hasattr(self._decompressor, "finish"):
                self._decompressor.finish()
            return b""
        except brotli.error as exc:
            raise DecodingError(str(exc))


class ZstdDecoder(ContentDecoder):
    def __init__(self) -> None:
        self._decompressor = zstd.ZstdDecompressor()
        self._at_frame_boundary = True

    def decode(self, data: bytes) -> bytes:
        output = []
        try:
            while data:
                output.append(self._decompressor.decompress(data))
                if self._decompressor.eof:
                    # frames may be concatenated; a decompressor handles one frame
                    data = self._decompressor.unused_data
                    self._decompressor = zstd.ZstdDecompressor()
                    self._at_frame_boundary = True
                else:
                    data = b""
                    self._at_frame_boundary = False
        except zstd.ZstdError as exc:
            raise DecodingError(str(exc))
        return b"".join(output)

    def flush(self) -> bytes:
        if not self._at_frame_boundary:
            raise DecodingError("Truncated zstd stream")
        return b""


class MultiDecoder(ContentDecoder):
    """Chained decoding for `Content-Encoding: gzip, br` style values.

    Encodings are listed in the order applied, so decoding runs in reverse.
    """

    def __init__(self, children: typing.Sequence[ContentDecoder]) -> None:
        self._children = list(reversed(children))

    def decode(self, data: bytes) -> bytes:
        for child in self._children:
            data = child.decode(data)
        return data

    def flush(self) -> bytes:
        data = b""
        for child in self._children:
            data = child.decode(data) + child.flush()
        return data


SUPPORTED_DECODERS: dict[str, type[ContentDecoder]] = {
    "identity": IdentityDecoder,
    "gzip": GzipDecoder,
    "deflate": DeflateDecoder,
}
if brotli is not None:
    SUPPORTED_DECODERS["br"] = BrotliDecoder
if zstd is not None:
    SUPPORTED_DECODERS["zstd"] = ZstdDecoder

ACCEPT_ENCODING = ", ".join(key for key in SUPPORTED_DECODERS if key != "identity")


class ByteChunker:
    """Re-chunks a byte stream into exactly `chunk_size`-long pieces (except the last)."""

    def __init__(self, chunk_size: int | None = None) -> None:
        self._buffer = io.BytesIO()
        self._chunk_size = chunk_size

    def decode(self, content: bytes) -> list[bytes]:
        if self._chunk_size is None:
            return [content] if content else []
        self._buffer.write(content)
        if self._buffer.tell() < self._chunk_size:
            return []
        value = self._buffer.getvalue()
        chunks = [value[i : i + self._chunk_size] for i in range(0, len(value), self._chunk_size)]
        if len(chunks[-1]) == self._chunk_size:
            self._buffer.seek(0)
            self._buffer.truncate()
            return chunks
        self._buffer.seek(0)
        self._buffer.write(chunks[-1])
        self._buffer.truncate()
        return chunks[:-1]

    def flush(self) -> list[bytes]:
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class TextChunker:
    """Re-chunks a text stream into exactly `chunk_size`-long pieces (except the last)."""

    def __init__(self, chunk_size: int | None = None) -> None:
        self._buffer = io.StringIO()
        self._chunk_size = chunk_size

    def decode(self, content: str) -> list[str]:
        if self._chunk_size is None:
            return [content] if content else []
        self._buffer.write(content)
        if self._buffer.tell() < self._chunk_size:
            return []
        value = self._buffer.getvalue()
        chunks = [value[i : i + self._chunk_size] for i in range(0, len(value), self._chunk_size)]
        if len(chunks[-1]) == self._chunk_size:
            self._buffer.seek(0)
            self._buffer.truncate()
            return chunks
        self._buffer.seek(0)
        self._buffer.write(chunks[-1])
        self._buffer.truncate()
        return chunks[:-1]

    def flush(self) -> list[str]:
        value = self._buffer.getvalue()
        self._buffer.seek(0)
        self._buffer.truncate()
        return [value] if value else []


class TextDecoder:
    """Incrementally decodes a byte stream into text."""

    def __init__(self, encoding: str = "utf-8") -> None:
        self._decoder = codecs.getincrementaldecoder(encoding)(errors="replace")

    def decode(self, data: bytes) -> str:
        return self._decoder.decode(data)

    def flush(self) -> str:
        return self._decoder.decode(b"", True)


_NEWLINE_CHARS = "\n\r\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029"


class LineDecoder:
    """Incrementally splits a text stream into lines, `str.splitlines()` style,
    handling `\\r\\n` sequences straddling chunk boundaries."""

    def __init__(self) -> None:
        self._buffer: list[str] = []
        self._trailing_cr = False

    def decode(self, text: str) -> list[str]:
        if self._trailing_cr:
            text = "\r" + text
            self._trailing_cr = False
        if text.endswith("\r"):
            self._trailing_cr = True
            text = text[:-1]

        if not text:
            return []

        trailing_newline = text[-1] in _NEWLINE_CHARS
        lines = text.splitlines()

        if len(lines) == 1 and not trailing_newline:
            self._buffer.append(lines[0])
            return []

        if self._buffer:
            lines = ["".join(self._buffer) + lines[0]] + lines[1:]
            self._buffer = []

        if not trailing_newline:
            self._buffer = [lines.pop()]

        return lines

    def flush(self) -> list[str]:
        if not self._buffer and not self._trailing_cr:
            return []
        lines = ["".join(self._buffer)]
        self._buffer = []
        self._trailing_cr = False
        return lines
