import gzip
import zlib

import pytest

from punkreq import DecodingError
from punkreq._decoders import (
    ACCEPT_ENCODING,
    SUPPORTED_DECODERS,
    ByteChunker,
    DeflateDecoder,
    GzipDecoder,
    LineDecoder,
    MultiDecoder,
    TextDecoder,
)


def decode_all(decoder, payload, chunk_size=5):
    out = b"" if isinstance(payload, bytes) else ""
    for i in range(0, len(payload), chunk_size):
        out += decoder.decode(payload[i : i + chunk_size])
    out += decoder.flush()
    return out


class TestContentDecoders:
    def test_gzip(self):
        assert decode_all(GzipDecoder(), gzip.compress(b"hello world")) == b"hello world"

    def test_gzip_invalid(self):
        with pytest.raises(DecodingError):
            GzipDecoder().decode(b"not gzip at all")

    def test_deflate_zlib_wrapped(self):
        assert decode_all(DeflateDecoder(), zlib.compress(b"hello world")) == b"hello world"

    def test_deflate_raw(self):
        compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        payload = compressor.compress(b"hello world") + compressor.flush()
        assert decode_all(DeflateDecoder(), payload) == b"hello world"

    def test_multi(self):
        # Content-Encoding: deflate, gzip → deflate applied first, gzip second
        payload = gzip.compress(zlib.compress(b"hello world"))
        decoder = MultiDecoder([DeflateDecoder(), GzipDecoder()])
        assert decode_all(decoder, payload) == b"hello world"

    def test_zstd_if_available(self):
        if "zstd" not in SUPPORTED_DECODERS:
            pytest.skip("no stdlib zstd on this Python")
        from compression import zstd

        decoder_cls = SUPPORTED_DECODERS["zstd"]
        assert decode_all(decoder_cls(), zstd.compress(b"hello world")) == b"hello world"
        # concatenated frames
        payload = zstd.compress(b"hello ") + zstd.compress(b"world")
        assert decode_all(decoder_cls(), payload) == b"hello world"
        with pytest.raises(DecodingError):
            truncated = zstd.compress(b"hello world" * 100)[:20]
            decoder = decoder_cls()
            decoder.decode(truncated)
            decoder.flush()

    def test_accept_encoding_matches_supported(self):
        assert "gzip" in ACCEPT_ENCODING
        assert "deflate" in ACCEPT_ENCODING
        assert "identity" not in ACCEPT_ENCODING


class TestByteChunker:
    def test_passthrough_without_size(self):
        chunker = ByteChunker()
        assert chunker.decode(b"ab") == [b"ab"]
        assert chunker.decode(b"") == []
        assert chunker.flush() == []

    def test_rechunk(self):
        chunker = ByteChunker(chunk_size=3)
        assert chunker.decode(b"ab") == []
        assert chunker.decode(b"cdefg") == [b"abc", b"def"]
        assert chunker.flush() == [b"g"]

    def test_exact_boundary(self):
        chunker = ByteChunker(chunk_size=2)
        assert chunker.decode(b"abcd") == [b"ab", b"cd"]
        assert chunker.flush() == []


class TestTextDecoder:
    def test_utf8(self):
        decoder = TextDecoder()
        # multi-byte char split across chunks
        payload = "héllo".encode()
        assert decoder.decode(payload[:2]) + decoder.decode(payload[2:]) + decoder.flush() == "héllo"

    def test_replacement_on_invalid(self):
        decoder = TextDecoder()
        assert "�" in decoder.decode(b"\xff\xfe invalid") + decoder.flush()


class TestLineDecoder:
    def test_lf(self):
        decoder = LineDecoder()
        assert decoder.decode("a\nb\nc") == ["a", "b"]
        assert decoder.flush() == ["c"]

    def test_crlf_straddling_chunks(self):
        decoder = LineDecoder()
        assert decoder.decode("a\r") == []
        assert decoder.decode("\nb") == ["a"]
        assert decoder.flush() == ["b"]

    def test_cr_only(self):
        decoder = LineDecoder()
        assert decoder.decode("a\rb\rc") == ["a", "b"]
        assert decoder.flush() == ["c"]

    def test_trailing_newline(self):
        decoder = LineDecoder()
        assert decoder.decode("a\nb\n") == ["a", "b"]
        assert decoder.flush() == []

    def test_trailing_cr_at_eof(self):
        decoder = LineDecoder()
        assert decoder.decode("a\r") == []
        assert decoder.flush() == ["a"]
