import io

import pytest

from punkreq._multipart import MultipartStream


BOUNDARY = b"testboundary"


async def render(data, files):
    stream = MultipartStream(data, files, boundary=BOUNDARY)
    headers = stream.get_headers()
    body = b"".join([chunk async for chunk in stream])
    return headers, body


@pytest.mark.asyncio
async def test_multipart_data_fields():
    headers, body = await render({"a": "1", "b": ["x", "y"]}, {})
    assert headers["Content-Type"] == "multipart/form-data; boundary=testboundary"
    assert body == (
        b"--testboundary\r\n"
        b'Content-Disposition: form-data; name="a"\r\n\r\n1\r\n'
        b"--testboundary\r\n"
        b'Content-Disposition: form-data; name="b"\r\n\r\nx\r\n'
        b"--testboundary\r\n"
        b'Content-Disposition: form-data; name="b"\r\n\r\ny\r\n'
        b"--testboundary--\r\n"
    )
    assert headers["Content-Length"] == str(len(body))


@pytest.mark.asyncio
async def test_multipart_bytes_file():
    headers, body = await render({}, {"upload": ("f.txt", b"contents", "text/plain")})
    assert (
        b'Content-Disposition: form-data; name="upload"; filename="f.txt"\r\nContent-Type: text/plain\r\n\r\ncontents\r\n'
    ) in body
    assert headers["Content-Length"] == str(len(body))


@pytest.mark.asyncio
async def test_multipart_content_type_guessed_from_filename():
    _, body = await render({}, {"upload": ("photo.png", b"xxx")})
    assert b"Content-Type: image/png" in body


@pytest.mark.asyncio
async def test_multipart_unknown_extension_defaults_octet_stream():
    _, body = await render({}, {"upload": ("blob.xyzunknown", b"xxx")})
    assert b"Content-Type: application/octet-stream" in body


@pytest.mark.asyncio
async def test_multipart_file_like_with_length():
    fileobj = io.BytesIO(b"file contents")
    headers, body = await render({}, {"upload": ("f.bin", fileobj)})
    assert b"file contents" in body
    assert headers["Content-Length"] == str(len(body))


@pytest.mark.asyncio
async def test_multipart_filename_from_name_attr():
    fileobj = io.BytesIO(b"data")
    fileobj.name = "/data/some/path/report.csv"
    _, body = await render({}, {"upload": fileobj})
    assert b'filename="report.csv"' in body
    assert b"Content-Type: text/csv" in body


@pytest.mark.asyncio
async def test_multipart_no_filename_no_content_type():
    _, body = await render({}, {"field": io.BytesIO(b"data")})
    assert b'Content-Disposition: form-data; name="field"\r\n\r\ndata' in body
    assert b"filename=" not in body


@pytest.mark.asyncio
async def test_multipart_extra_part_headers():
    _, body = await render({}, {"u": ("f.txt", b"x", "text/plain", {"X-Custom": "1"})})
    assert b"X-Custom: 1\r\n" in body


@pytest.mark.asyncio
async def test_multipart_name_escaping():
    _, body = await render({}, {"f": ('quo"te\r\n.txt', b"x")})
    assert b'filename="quo%22te%0D%0A.txt"' in body


@pytest.mark.asyncio
async def test_multipart_files_as_sequence():
    _, body = await render({}, [("a", b"1"), ("a", b"2")])
    assert body.count(b'name="a"') == 2


@pytest.mark.asyncio
async def test_multipart_str_file_content():
    _, body = await render({}, {"f": ("f.txt", "tèxt")})
    assert "tèxt".encode() in body


def test_multipart_invalid_tuple_length():
    with pytest.raises(TypeError):
        MultipartStream({}, {"f": ("a", b"b", "c", {}, "extra")}, boundary=BOUNDARY)


def test_multipart_random_boundary():
    stream = MultipartStream({}, {"f": b"x"})
    other = MultipartStream({}, {"f": b"x"})
    assert stream.boundary != other.boundary
    assert len(stream.boundary) == 32
