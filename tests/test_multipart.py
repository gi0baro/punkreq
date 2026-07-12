import asyncio
import io

import pytest

from punkreq._multipart import MultipartStream


BOUNDARY = b"testboundary"


def render(data, files):
    stream = MultipartStream(data, files, boundary=BOUNDARY)
    headers = stream.get_headers()
    body = asyncio.run(_collect(stream))
    return headers, body


async def _collect(stream):
    return b"".join([chunk async for chunk in stream])


class TestMultipart:
    def test_data_fields(self):
        headers, body = render({"a": "1", "b": ["x", "y"]}, {})
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

    def test_bytes_file(self):
        headers, body = render({}, {"upload": ("f.txt", b"contents", "text/plain")})
        assert (
            b'Content-Disposition: form-data; name="upload"; filename="f.txt"\r\nContent-Type: text/plain\r\n\r\ncontents\r\n'
        ) in body
        assert headers["Content-Length"] == str(len(body))

    def test_content_type_guessed_from_filename(self):
        _, body = render({}, {"upload": ("photo.png", b"xxx")})
        assert b"Content-Type: image/png" in body

    def test_unknown_extension_defaults_octet_stream(self):
        _, body = render({}, {"upload": ("blob.xyzunknown", b"xxx")})
        assert b"Content-Type: application/octet-stream" in body

    def test_file_like_with_length(self):
        fileobj = io.BytesIO(b"file contents")
        headers, body = render({}, {"upload": ("f.bin", fileobj)})
        assert b"file contents" in body
        assert headers["Content-Length"] == str(len(body))

    def test_filename_from_name_attr(self):
        fileobj = io.BytesIO(b"data")
        fileobj.name = "/data/some/path/report.csv"
        _, body = render({}, {"upload": fileobj})
        assert b'filename="report.csv"' in body
        assert b"Content-Type: text/csv" in body

    def test_no_filename_no_content_type(self):
        _, body = render({}, {"field": io.BytesIO(b"data")})
        assert b'Content-Disposition: form-data; name="field"\r\n\r\ndata' in body
        assert b"filename=" not in body

    def test_extra_part_headers(self):
        _, body = render({}, {"u": ("f.txt", b"x", "text/plain", {"X-Custom": "1"})})
        assert b"X-Custom: 1\r\n" in body

    def test_name_escaping(self):
        _, body = render({}, {"f": ('quo"te\r\n.txt', b"x")})
        assert b'filename="quo%22te%0D%0A.txt"' in body

    def test_files_as_sequence(self):
        _, body = render({}, [("a", b"1"), ("a", b"2")])
        assert body.count(b'name="a"') == 2

    def test_str_file_content(self):
        _, body = render({}, {"f": ("f.txt", "tèxt")})
        assert "tèxt".encode() in body

    def test_invalid_tuple_length(self):
        with pytest.raises(TypeError):
            MultipartStream({}, {"f": ("a", b"b", "c", {}, "extra")}, boundary=BOUNDARY)

    def test_random_boundary(self):
        stream = MultipartStream({}, {"f": b"x"})
        other = MultipartStream({}, {"f": b"x"})
        assert stream.boundary != other.boundary
        assert len(stream.boundary) == 32
