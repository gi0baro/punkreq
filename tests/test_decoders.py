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
    zstd,  # the stdlib module, or None when this Python lacks it
)


def decode_all(decoder, payload, chunk_size=5):
    out = b"" if isinstance(payload, bytes) else ""
    for i in range(0, len(payload), chunk_size):
        out += decoder.decode(payload[i : i + chunk_size])
    out += decoder.flush()
    return out


# ----- content decoders -----


def test_gzip_decoder_invalid_raises_decoding_error():
    with pytest.raises(DecodingError):
        GzipDecoder().decode(b"not gzip at all")


def test_deflate_decoder_accepts_raw_deflate():
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    payload = compressor.compress(b"hello world") + compressor.flush()
    assert decode_all(DeflateDecoder(), payload) == b"hello world"


def test_multi_decoder_applies_in_order():
    # Content-Encoding: deflate, gzip → deflate applied first, gzip second
    payload = gzip.compress(zlib.compress(b"hello world"))
    decoder = MultiDecoder([DeflateDecoder(), GzipDecoder()])
    assert decode_all(decoder, payload) == b"hello world"


@pytest.mark.skipif(zstd is None, reason="no stdlib zstd on this Python")
def test_zstd_decoder_concatenated_frames_and_truncation():
    decoder_cls = SUPPORTED_DECODERS["zstd"]
    # a decompressor handles one frame: the decoder must chain over frame boundaries
    payload = zstd.compress(b"hello ") + zstd.compress(b"world")
    assert decode_all(decoder_cls(), payload) == b"hello world"
    # ending mid-frame is a decoding error, not a silently short body
    truncated = zstd.compress(b"hello world" * 100)[:20]
    decoder = decoder_cls()
    decoder.decode(truncated)
    with pytest.raises(DecodingError):
        decoder.flush()


def test_accept_encoding_matches_supported():
    assert "gzip" in ACCEPT_ENCODING
    assert "deflate" in ACCEPT_ENCODING
    assert "identity" not in ACCEPT_ENCODING
    # zstd is offered exactly when the stdlib can decode it
    assert ("zstd" in ACCEPT_ENCODING) == ("zstd" in SUPPORTED_DECODERS) == (zstd is not None)


# ----- ByteChunker -----


def test_byte_chunker_passthrough_without_size():
    chunker = ByteChunker()
    assert chunker.decode(b"ab") == [b"ab"]
    assert chunker.decode(b"") == []
    assert chunker.flush() == []


def test_byte_chunker_rechunks():
    chunker = ByteChunker(chunk_size=3)
    assert chunker.decode(b"ab") == []
    assert chunker.decode(b"cdefg") == [b"abc", b"def"]
    assert chunker.flush() == [b"g"]


def test_byte_chunker_exact_boundary():
    chunker = ByteChunker(chunk_size=2)
    assert chunker.decode(b"abcd") == [b"ab", b"cd"]
    assert chunker.flush() == []


# ----- LineDecoder -----


def test_line_decoder_lf():
    decoder = LineDecoder()
    assert decoder.decode("a\nb\nc") == ["a", "b"]
    assert decoder.flush() == ["c"]


def test_line_decoder_crlf_straddling_chunks():
    decoder = LineDecoder()
    assert decoder.decode("a\r") == []
    assert decoder.decode("\nb") == ["a"]
    assert decoder.flush() == ["b"]


def test_line_decoder_cr_only():
    decoder = LineDecoder()
    assert decoder.decode("a\rb\rc") == ["a", "b"]
    assert decoder.flush() == ["c"]


def test_line_decoder_trailing_newline():
    decoder = LineDecoder()
    assert decoder.decode("a\nb\n") == ["a", "b"]
    assert decoder.flush() == []


def test_line_decoder_trailing_cr_at_eof():
    decoder = LineDecoder()
    assert decoder.decode("a\r") == []
    assert decoder.flush() == ["a"]
