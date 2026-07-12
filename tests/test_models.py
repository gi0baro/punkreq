import asyncio
import gzip

import pytest

import punkreq
from punkreq import Request, Response
from punkreq._content import AsyncIteratorByteStream


def run(coro):
    return asyncio.run(coro)


def stream_of(*chunks):
    async def gen():
        for chunk in chunks:
            yield chunk

    return AsyncIteratorByteStream(gen())


class TestRequest:
    def test_basic(self):
        request = Request("get", "https://example.com/path")
        assert request.method == "GET"
        assert request.url == "https://example.com/path"
        assert request.headers["host"] == "example.com"
        assert request.content == b""

    def test_host_header_with_port_and_ipv6(self):
        assert Request("GET", "https://example.com:8443/").headers["host"] == "example.com:8443"
        assert Request("GET", "http://[::1]:8080/").headers["host"] == "[::1]:8080"

    def test_host_not_overridden(self):
        request = Request("GET", "https://example.com/", headers={"Host": "override.example"})
        assert request.headers["host"] == "override.example"

    def test_params_merged_into_url(self):
        request = Request("GET", "https://example.com/?a=1", params={"b": 2})
        assert str(request.url) == "https://example.com/?a=1&b=2"

    def test_content_length_zero_for_bodyless_post(self):
        assert Request("POST", "https://example.com/").headers["content-length"] == "0"
        assert "content-length" not in Request("GET", "https://example.com/").headers

    def test_json_body(self):
        request = Request("POST", "https://example.com/", json={"a": 1})
        assert request.headers["content-type"] == "application/json"
        assert request.content == b'{"a":1}'
        assert request.headers["content-length"] == "7"

    def test_explicit_content_type_wins(self):
        request = Request(
            "POST", "https://example.com/", json={"a": 1}, headers={"content-type": "application/vnd.custom+json"}
        )
        assert request.headers["content-type"] == "application/vnd.custom+json"

    def test_streaming_body(self):
        request = Request("POST", "https://example.com/", content=iter([b"a", b"b"]))
        assert request.headers["transfer-encoding"] == "chunked"
        with pytest.raises(punkreq.RequestNotRead):
            request.content
        assert run(request.read()) == b"ab"
        assert request.content == b"ab"
        # after read() the body is repackaged and replayable
        assert run(request.read()) == b"ab"

    def test_repr(self):
        assert repr(Request("GET", "https://example.com/")) == "<Request('GET', 'https://example.com/')>"


class TestResponseContent:
    def test_from_content(self):
        response = Response(200, content=b"body", request=Request("GET", "https://example.com/"))
        assert response.status_code == 200
        assert response.reason_phrase == "OK"
        assert run(response.read()) == b"body"
        assert run(response.text()) == "body"
        assert response.headers["content-length"] == "4"
        assert response.is_closed
        assert repr(response) == "<Response [200 OK]>"

    def test_from_text(self):
        response = Response(200, text="héllo")
        assert response.headers["content-type"] == "text/plain; charset=utf-8"
        assert run(response.read()) == "héllo".encode()

    def test_from_json(self):
        response = Response(200, json={"a": 1})
        assert response.headers["content-type"] == "application/json"
        assert run(response.json()) == {"a": 1}

    def test_charset_from_content_type(self):
        response = Response(
            200, content="olá".encode("iso-8859-1"), headers={"content-type": "text/plain; charset=iso-8859-1"}
        )
        assert response.encoding == "iso-8859-1"
        assert run(response.text()) == "olá"

    def test_unknown_charset_falls_back(self):
        response = Response(200, content=b"x", headers={"content-type": "text/plain; charset=zebra"})
        assert response.encoding == "utf-8"

    def test_callable_default_encoding(self):
        response = Response(200, content="olá".encode("iso-8859-1"), default_encoding=lambda content: "iso-8859-1")
        assert run(response.text()) == "olá"

    def test_explicit_encoding_setter(self):
        response = Response(200, content="olá".encode("iso-8859-1"))
        response.encoding = "iso-8859-1"
        assert run(response.text()) == "olá"
        with pytest.raises(ValueError):
            response.encoding = "utf-8"

    def test_status_predicates(self):
        assert Response(102).is_informational
        assert Response(200).is_success
        assert Response(302).is_redirect
        assert Response(404).is_client_error
        assert Response(503).is_server_error
        assert Response(404).is_error
        assert Response(301, headers={"location": "/x"}).has_redirect_location
        assert not Response(301).has_redirect_location

    def test_raise_for_status(self):
        request = Request("GET", "https://example.com/")
        assert Response(200, request=request).raise_for_status() is not None
        # reqwest semantics: 3xx does not raise
        assert Response(302, request=request, headers={"location": "/x"}).raise_for_status() is not None
        with pytest.raises(punkreq.HTTPStatusError) as exc_info:
            Response(404, request=request).raise_for_status()
        assert exc_info.value.response.status_code == 404
        assert exc_info.value.request is request
        assert "404 Not Found" in str(exc_info.value)
        with pytest.raises(punkreq.HTTPStatusError):
            Response(500, request=request).raise_for_status()

    def test_links(self):
        response = Response(
            200,
            headers={"link": '<https://example.com/next>; rel="next", <https://example.com/last>; rel="last"'},
        )
        assert response.links["next"]["url"] == "https://example.com/next"
        assert response.links["last"]["url"] == "https://example.com/last"
        assert Response(200).links == {}

    def test_elapsed_unset_raises(self):
        with pytest.raises(RuntimeError):
            Response(200).elapsed

    def test_cookies_parsed_from_set_cookie(self):
        request = Request("GET", "https://example.com/")
        response = Response(
            200,
            headers=[("set-cookie", "session=abc; Path=/"), ("set-cookie", "theme=dark; Path=/")],
            request=request,
        )
        assert response.cookies["session"] == "abc"
        assert response.cookies["theme"] == "dark"
        assert Response(200, request=request).cookies.get("nope") is None


class TestResponseStreaming:
    def test_read(self):
        response = Response(200, stream=stream_of(b"he", b"llo"))
        assert run(response.read()) == b"hello"
        assert run(response.read()) == b"hello"  # idempotent, cached
        assert response.is_closed

    def test_iter_bytes_decodes_gzip(self):
        payload = gzip.compress(b"hello world")
        response = Response(200, headers={"content-encoding": "gzip"}, stream=stream_of(payload))

        async def collect():
            return b"".join([chunk async for chunk in response.iter_bytes()])

        assert run(collect()) == b"hello world"
        assert response.num_bytes_downloaded == len(payload)

    def test_read_decodes_gzip(self):
        payload = gzip.compress(b"hello world")
        response = Response(200, headers={"content-encoding": "gzip"}, stream=stream_of(payload))
        assert run(response.read()) == b"hello world"

    def test_iter_raw_undecoded(self):
        payload = gzip.compress(b"hello world")
        response = Response(200, headers={"content-encoding": "gzip"}, stream=stream_of(payload))

        async def collect():
            return b"".join([chunk async for chunk in response.iter_raw()])

        assert run(collect()) == payload

    def test_stream_consumed_twice_raises(self):
        response = Response(200, stream=stream_of(b"x"))
        run(response.read())

        async def iterate_raw():
            async for _ in response.iter_raw():
                pass

        with pytest.raises(punkreq.StreamConsumed):
            run(iterate_raw())

    def test_iter_bytes_rechunks(self):
        response = Response(200, stream=stream_of(b"abcdefg"))

        async def collect():
            return [chunk async for chunk in response.iter_bytes(chunk_size=3)]

        assert run(collect()) == [b"abc", b"def", b"g"]

    def test_iter_bytes_from_cached_content(self):
        response = Response(200, content=b"abcdef")

        async def collect():
            return [chunk async for chunk in response.iter_bytes(chunk_size=4)]

        assert run(collect()) == [b"abcd", b"ef"]

    def test_iter_lines(self):
        response = Response(200, stream=stream_of(b"line1\nli", b"ne2\r\nline3"))

        async def collect():
            return [line async for line in response.iter_lines()]

        assert run(collect()) == ["line1", "line2", "line3"]

    def test_iter_text(self):
        response = Response(200, stream=stream_of("hé".encode(), b"llo"))

        async def collect():
            return "".join([chunk async for chunk in response.iter_text()])

        assert run(collect()) == "héllo"

    def test_unknown_content_encoding_passes_through(self):
        response = Response(200, headers={"content-encoding": "myzip"}, stream=stream_of(b"raw"))
        assert run(response.read()) == b"raw"

    def test_history_default(self):
        assert Response(200).history == []
