import io

import pytest

from punkreq import ByteStream, StreamConsumed
from punkreq._content import encode_request


async def collect(stream):
    return b"".join([chunk async for chunk in stream])


# ----- content= -----


@pytest.mark.asyncio
async def test_content_bytes():
    headers, stream = encode_request(content=b"hello")
    assert headers == {"Content-Length": "5"}
    assert isinstance(stream, ByteStream)
    assert await collect(stream) == b"hello"
    assert await collect(stream) == b"hello"  # replayable


@pytest.mark.asyncio
async def test_content_str_utf8():
    headers, stream = encode_request(content="héllo")
    assert headers == {"Content-Length": "6"}
    assert await collect(stream) == "héllo".encode()


@pytest.mark.asyncio
async def test_content_empty_bytes_no_headers():
    headers, stream = encode_request(content=b"")
    assert headers == {}
    assert await collect(stream) == b""


@pytest.mark.asyncio
async def test_content_sync_iterable_chunked():
    headers, stream = encode_request(content=iter([b"he", b"llo"]))
    assert headers == {"Transfer-Encoding": "chunked"}
    assert await collect(stream) == b"hello"
    with pytest.raises(StreamConsumed):
        await collect(stream)


@pytest.mark.asyncio
async def test_content_file_like_content_length():
    fileobj = io.BytesIO(b"hello world")
    headers, stream = encode_request(content=fileobj)
    assert headers == {"Content-Length": "11"}
    assert await collect(stream) == b"hello world"


@pytest.mark.asyncio
async def test_content_file_like_respects_position():
    fileobj = io.BytesIO(b"hello world")
    fileobj.seek(6)
    headers, stream = encode_request(content=fileobj)
    assert headers == {"Content-Length": "5"}
    assert await collect(stream) == b"world"


@pytest.mark.asyncio
async def test_content_async_iterable_chunked():
    async def gen():
        yield b"he"
        yield b"llo"

    headers, stream = encode_request(content=gen())
    assert headers == {"Transfer-Encoding": "chunked"}
    assert await collect(stream) == b"hello"
    with pytest.raises(StreamConsumed):
        await collect(stream)


def test_content_async_iterable_without_aclose_rejected():
    # contract: async content must be closable deterministically — reject
    # up front instead of failing (or silently skipping) at teardown
    class PlainAiterable:
        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            yield b"hello"

    with pytest.raises(TypeError, match="aclose"):
        encode_request(content=PlainAiterable())


def test_content_invalid_type():
    with pytest.raises(TypeError):
        encode_request(content=123)


# ----- data= -----


@pytest.mark.asyncio
async def test_data_urlencoded():
    headers, stream = encode_request(data={"a": "1", "b": "hello world"})
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"
    body = await collect(stream)
    assert body == b"a=1&b=hello+world"
    assert headers["Content-Length"] == str(len(body))


@pytest.mark.asyncio
async def test_data_list_values_expand():
    _, stream = encode_request(data={"a": [1, 2], "b": True, "c": None})
    assert await collect(stream) == b"a=1&a=2&b=true&c="


def test_data_non_mapping_rejected():
    with pytest.raises(TypeError):
        encode_request(data=b"raw bytes")


# ----- json= -----


@pytest.mark.asyncio
async def test_json_encoded():
    headers, stream = encode_request(json={"k": "vàl", "n": 1})
    assert headers["Content-Type"] == "application/json"
    assert await collect(stream) == '{"k":"vàl","n":1}'.encode()


@pytest.mark.asyncio
async def test_json_none_means_no_body():
    # `json=None` means "no json argument", not JSON null
    headers, stream = encode_request(json=None)
    assert await collect(stream) == b""


# ----- precedence -----


@pytest.mark.asyncio
async def test_precedence_content_over_data_and_json():
    headers, stream = encode_request(content=b"raw", data={"a": "1"}, json={"b": 2})
    assert await collect(stream) == b"raw"


@pytest.mark.asyncio
async def test_precedence_data_over_json():
    headers, stream = encode_request(data={"a": "1"}, json={"b": 2})
    assert await collect(stream) == b"a=1"


@pytest.mark.asyncio
async def test_encode_nothing_is_empty():
    headers, stream = encode_request()
    assert headers == {}
    assert await collect(stream) == b""
