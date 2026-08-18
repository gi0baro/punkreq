import asyncio
import io

import pytest

from punkreq import ByteStream, StreamConsumed
from punkreq._content import encode_request


async def collect(stream):
    return b"".join([chunk async for chunk in stream])


def run(coro):
    return asyncio.run(coro)


class TestEncodeContent:
    def test_bytes(self):
        headers, stream = encode_request(content=b"hello")
        assert headers == {"Content-Length": "5"}
        assert isinstance(stream, ByteStream)
        assert run(collect(stream)) == b"hello"
        assert run(collect(stream)) == b"hello"  # replayable

    def test_str_utf8(self):
        headers, stream = encode_request(content="héllo")
        assert headers == {"Content-Length": "6"}
        assert run(collect(stream)) == "héllo".encode()

    def test_empty_bytes_no_headers(self):
        headers, stream = encode_request(content=b"")
        assert headers == {}
        assert run(collect(stream)) == b""

    def test_sync_iterable_chunked(self):
        headers, stream = encode_request(content=iter([b"he", b"llo"]))
        assert headers == {"Transfer-Encoding": "chunked"}
        assert run(collect(stream)) == b"hello"
        with pytest.raises(StreamConsumed):
            run(collect(stream))

    def test_file_like_content_length(self):
        fileobj = io.BytesIO(b"hello world")
        headers, stream = encode_request(content=fileobj)
        assert headers == {"Content-Length": "11"}
        assert run(collect(stream)) == b"hello world"

    def test_file_like_respects_position(self):
        fileobj = io.BytesIO(b"hello world")
        fileobj.seek(6)
        headers, stream = encode_request(content=fileobj)
        assert headers == {"Content-Length": "5"}
        assert run(collect(stream)) == b"world"

    def test_async_iterable_chunked(self):
        async def gen():
            yield b"he"
            yield b"llo"

        headers, stream = encode_request(content=gen())
        assert headers == {"Transfer-Encoding": "chunked"}
        assert run(collect(stream)) == b"hello"
        with pytest.raises(StreamConsumed):
            run(collect(stream))

    def test_async_iterable_without_aclose_rejected(self):
        # contract: async content must be closable deterministically — reject
        # up front instead of failing (or silently skipping) at teardown
        class PlainAiterable:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield b"hello"

        with pytest.raises(TypeError, match="aclose"):
            encode_request(content=PlainAiterable())

    def test_invalid_type(self):
        with pytest.raises(TypeError):
            encode_request(content=123)


class TestEncodeData:
    def test_urlencoded(self):
        headers, stream = encode_request(data={"a": "1", "b": "hello world"})
        assert headers["Content-Type"] == "application/x-www-form-urlencoded"
        body = run(collect(stream))
        assert body == b"a=1&b=hello+world"
        assert headers["Content-Length"] == str(len(body))

    def test_list_values_expand(self):
        _, stream = encode_request(data={"a": [1, 2], "b": True, "c": None})
        assert run(collect(stream)) == b"a=1&a=2&b=true&c="

    def test_non_mapping_rejected(self):
        with pytest.raises(TypeError):
            encode_request(data=b"raw bytes")


class TestEncodeJson:
    def test_json(self):
        headers, stream = encode_request(json={"k": "vàl", "n": 1})
        assert headers["Content-Type"] == "application/json"
        assert run(collect(stream)) == '{"k":"vàl","n":1}'.encode()

    def test_json_null(self):
        # `json=None` means "no json argument", not JSON null
        headers, stream = encode_request(json=None)
        assert run(collect(stream)) == b""


class TestPrecedence:
    def test_content_wins_over_data_and_json(self):
        headers, stream = encode_request(content=b"raw", data={"a": "1"}, json={"b": 2})
        assert run(collect(stream)) == b"raw"

    def test_data_wins_over_json(self):
        headers, stream = encode_request(data={"a": "1"}, json={"b": 2})
        assert run(collect(stream)) == b"a=1"

    def test_empty(self):
        headers, stream = encode_request()
        assert headers == {}
        assert run(collect(stream)) == b""
