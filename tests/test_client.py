import asyncio
import gzip

import pytest
from httpunk import GoAwayError, H2Reason, HeaderMap, StreamResetError, Version
from httpunk.exceptions import ConnectionClosedError

import punkreq
from punkreq.asyncio import Client


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=10))


class FakeHTTPunkResponse:
    def __init__(self, status=200, headers=None, body=b"", chunk_delay=0.0):
        self.status = status
        self.headers = HeaderMap(headers or {})
        self._chunks = [body] if isinstance(body, bytes) else list(body)
        self._chunk_delay = chunk_delay
        self._conn = None  # set by ScriptedConnection.send_request
        self.version = Version.HTTP_11  # stamped per protocol by ScriptedConnection.send_request
        self._consumed = False
        self.closed = False

    async def aiter_bytes(self):
        for chunk in self._chunks:
            if self._chunk_delay:
                await asyncio.sleep(self._chunk_delay)
            yield chunk
        self._consumed = True

    async def aclose(self):
        self.closed = True
        # mirror httpunk h1: aborting a partially-read body closes the connection
        if not self._consumed and self._conn is not None and not self._conn.multiplexed:
            self._conn.closed = True


class ScriptedConnection:
    """handler(httpunk_request, conn) -> FakeHTTPunkResponse (or raises)."""

    def __init__(self, handler, multiplexed=False):
        self.handler = handler
        self.multiplexed = multiplexed
        self.closed = False
        self.busy = False  # httpunk >= 0.1.4: exchange holds the in-flight slot
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self.closed = True
        return False

    async def ready(self):
        if self.closed:
            raise RuntimeError("closed")

    async def send_request(self, request):
        self.requests.append(request)
        result = self.handler(request, self)
        if asyncio.iscoroutine(result):
            result = await result
        if isinstance(result, FakeHTTPunkResponse):
            result._conn = self
            result.version = Version.HTTP_2 if self.multiplexed else Version.HTTP_11
        return result


class FakeConnector:
    """Routes origins to scripted connections; a fresh connection per dial."""

    def __init__(self, handler, multiplexed=False):
        self.handler = handler
        self.multiplexed = multiplexed
        self.dials = 0
        self.connections = []

    async def __call__(self, origin):
        self.dials += 1
        conn = ScriptedConnection(self.handler, multiplexed=self.multiplexed)
        self.connections.append(conn)
        return conn


def ok_handler(body=b"hello", headers=None):
    def handler(request, conn):
        return FakeHTTPunkResponse(200, headers=headers, body=body)

    return handler


def make_client(handler, multiplexed=False, **kwargs):
    connector = FakeConnector(handler, multiplexed=multiplexed)
    client = Client(connector=connector, **kwargs)
    return client, connector


def sent_requests(connector):
    return [request for conn in connector.connections for request in conn.requests]


class TestBasics:
    def test_get_wire_shape(self):
        client, connector = make_client(ok_handler())

        async def main():
            async with client:
                response = await client.get("https://example.com/path?x=1")
                assert response.status_code == 200
                assert await response.text() == "hello"
                assert response.http_version == "HTTP/1.1"

        run(main())
        (request,) = sent_requests(connector)
        assert request.method == "GET"
        assert request.target == "/path?x=1"
        assert request.headers["host"] == b"example.com"
        assert request.headers["accept"] == b"*/*"
        assert b"gzip" in request.headers["accept-encoding"]
        assert request.headers["user-agent"].startswith(b"python-punkreq/")

    def test_range_request_suppresses_auto_accept_encoding(self):
        client, connector = make_client(ok_handler())

        async def main():
            async with client:
                await (await client.get("https://example.com/file", headers={"range": "bytes=0-99"})).read()

        run(main())
        (request,) = sent_requests(connector)
        assert "accept-encoding" not in request.headers
        assert request.headers["range"] == b"bytes=0-99"

    def test_explicit_accept_encoding_kept_with_range(self):
        client, connector = make_client(ok_handler())

        async def main():
            async with client:
                response = await client.get(
                    "https://example.com/file", headers={"range": "bytes=0-99", "accept-encoding": "identity"}
                )
                await response.read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.headers["accept-encoding"] == b"identity"

    def test_explicit_accept_encoding_not_overridden(self):
        client, connector = make_client(ok_handler(), headers={"accept-encoding": "gzip"})

        async def main():
            async with client:
                await (await client.get("https://example.com/")).read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.headers["accept-encoding"] == b"gzip"

    def test_h2_drops_host_header(self):
        client, connector = make_client(ok_handler(), multiplexed=True)

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                assert response.http_version == "HTTP/2"
                await response.read()

        run(main())
        (request,) = sent_requests(connector)
        assert "host" not in request.headers

    def test_base_url_and_params_merge(self):
        client, connector = make_client(ok_handler(), base_url="https://api.example.com/v1", params={"key": "K"})

        async def main():
            async with client:
                await (await client.get("users", params={"page": 2})).read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.target == "/v1/users?key=K&page=2"

    def test_post_json(self):
        client, connector = make_client(ok_handler())

        async def main():
            async with client:
                await (await client.post("https://example.com/items", json={"a": 1})).read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.method == "POST"
        assert request.headers["content-type"] == b"application/json"
        assert request.body == b'{"a":1}'

    def test_gzip_response_decoded(self):
        payload = gzip.compress(b"decompressed!")
        client, _ = make_client(ok_handler(body=payload, headers={"content-encoding": "gzip"}))

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                assert await response.text() == "decompressed!"

        run(main())

    def test_unsupported_protocol(self):
        client, _ = make_client(ok_handler())

        async def main():
            async with client:
                with pytest.raises(punkreq.UnsupportedProtocol):
                    await client.get("ftp://example.com/")
                with pytest.raises(punkreq.UnsupportedProtocol):
                    await client.get("/relative-without-base")

        run(main())

    def test_elapsed_set_after_read(self):
        client, _ = make_client(ok_handler())

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                await response.read()
                assert response.elapsed.total_seconds() >= 0

        run(main())

    def test_streaming_body_iteration(self):
        client, connector = make_client(ok_handler(body=[b"chunk1", b"chunk2"]))

        async def main():
            async with client:
                response = await client.get("https://example.com/big")
                chunks = [chunk async for chunk in response.iter_bytes()]
                assert chunks == [b"chunk1", b"chunk2"]
                # reading to EOF released the h1 connection back to the pool
                assert client._pool.idle_count == 1

        run(main())

    def test_verb_as_context_manager(self):
        client, connector = make_client(ok_handler(body=[b"chunk1", b"chunk2"]))

        async def main():
            async with client:
                async with client.get("https://example.com/big") as response:
                    async for _chunk in response.iter_bytes():
                        break  # abandon the body early
                # the context manager closed the response; connection evicted, not leaked
                assert response.is_closed
                assert client._pool.connection_count == 0

        run(main())


class TestRedirects:
    def test_redirect_followed_with_history(self):
        def handler(request, conn):
            if request.target == "/old":
                return FakeHTTPunkResponse(302, headers={"location": "/new"})
            return FakeHTTPunkResponse(200, body=b"arrived")

        client, connector = make_client(handler)

        async def main():
            async with client:
                response = await client.get("https://example.com/old")
                assert response.status_code == 200
                assert await response.text() == "arrived"
                assert [r.status_code for r in response.history] == [302]
                assert str(response.url) == "https://example.com/new"

        run(main())
        assert [r.target for r in sent_requests(connector)] == ["/old", "/new"]

    def test_post_302_becomes_get(self):
        def handler(request, conn):
            if request.method == "POST":
                return FakeHTTPunkResponse(302, headers={"location": "/done"})
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler)

        async def main():
            async with client:
                await (await client.post("https://example.com/form", data={"a": "1"})).read()

        run(main())
        second = sent_requests(connector)[1]
        assert second.method == "GET"
        assert second.body is None
        assert "content-type" not in second.headers

    def test_307_replays_body(self):
        def handler(request, conn):
            if request.target == "/a":
                return FakeHTTPunkResponse(307, headers={"location": "/b"})
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler)

        async def main():
            async with client:
                await (await client.post("https://example.com/a", content=b"payload")).read()

        run(main())
        requests = sent_requests(connector)
        assert [r.method for r in requests] == ["POST", "POST"]
        assert requests[1].body == b"payload"

    def test_307_with_streaming_body_not_followed(self):
        def handler(request, conn):
            return FakeHTTPunkResponse(307, headers={"location": "/b"})

        client, _ = make_client(handler)

        async def main():
            async with client:
                response = await client.post("https://example.com/a", content=iter([b"x"]))
                assert response.status_code == 307
                await response.close()

        run(main())

    def test_follow_disabled(self):
        client, _ = make_client(lambda request, conn: FakeHTTPunkResponse(302, headers={"location": "/x"}))

        async def main():
            async with client:
                response = await client.get("https://example.com/", follow_redirects=False)
                assert response.status_code == 302
                await response.close()

        run(main())

    def test_too_many_redirects(self):
        client, _ = make_client(
            lambda request, conn: FakeHTTPunkResponse(302, headers={"location": "/loop"}), max_redirects=3
        )

        async def main():
            async with client:
                with pytest.raises(punkreq.TooManyRedirects):
                    await client.get("https://example.com/loop")

        run(main())

    def test_auth_stripped_cross_origin(self):
        def handler(request, conn):
            if request.headers.get("host") == b"example.com":
                return FakeHTTPunkResponse(302, headers={"location": "https://other.org/next"})
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler, auth=("user", "pass"))

        async def main():
            async with client:
                await (await client.get("https://example.com/")).read()

        run(main())
        first, second = sent_requests(connector)
        assert "authorization" in first.headers
        assert "authorization" not in second.headers


class TestCookiesPipeline:
    def test_jar_roundtrip_across_requests(self):
        def handler(request, conn):
            if request.target == "/login":
                return FakeHTTPunkResponse(200, headers={"set-cookie": "session=abc; Path=/"})
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler, cookies={})

        async def main():
            async with client:
                await (await client.get("https://example.com/login")).read()
                await (await client.get("https://example.com/dash")).read()

        run(main())
        second = sent_requests(connector)[1]
        assert second.headers["cookie"] == b"session=abc"

    def test_cookie_set_on_redirect_hop(self):
        def handler(request, conn):
            if request.target == "/a":
                return FakeHTTPunkResponse(302, headers={"location": "/b", "set-cookie": "hop=1; Path=/"})
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler, cookies={})

        async def main():
            async with client:
                await (await client.get("https://example.com/a")).read()

        run(main())
        second = sent_requests(connector)[1]
        assert second.headers["cookie"] == b"hop=1"

    def test_no_jar_by_default(self):
        def handler(request, conn):
            if request.target == "/login":
                return FakeHTTPunkResponse(200, headers={"set-cookie": "session=abc; Path=/"})
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler)

        async def main():
            async with client:
                await (await client.get("https://example.com/login")).read()
                await (await client.get("https://example.com/dash")).read()

        run(main())
        second = sent_requests(connector)[1]
        assert "cookie" not in second.headers


class TestAuth:
    def test_client_basic_auth(self):
        client, connector = make_client(ok_handler(), auth=("user", "pass"))

        async def main():
            async with client:
                await (await client.get("https://example.com/")).read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.headers["authorization"] == b"Basic dXNlcjpwYXNz"

    def test_per_request_auth_overrides(self):
        client, connector = make_client(ok_handler(), auth=("user", "pass"))

        async def main():
            async with client:
                await (await client.get("https://example.com/", auth=punkreq.BearerAuth("tok"))).read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.headers["authorization"] == b"Bearer tok"

    def test_url_userinfo_becomes_basic_auth(self):
        client, connector = make_client(ok_handler())

        async def main():
            async with client:
                response = await client.get("https://user:pass@example.com/")
                assert "@" not in str(response.url)
                await response.read()

        run(main())
        (request,) = sent_requests(connector)
        assert request.headers["authorization"] == b"Basic dXNlcjpwYXNz"
        assert request.headers["host"] == b"example.com"


class TestRetries:
    def test_h2_goaway_retried_on_fresh_connection(self):
        state = {"calls": 0}

        def handler(request, conn):
            state["calls"] += 1
            if state["calls"] == 1:
                conn.closed = True
                raise GoAwayError(0, H2Reason.NO_ERROR, b"")
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler, multiplexed=True)

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                assert response.status_code == 200
                await response.read()

        run(main())
        assert connector.dials == 2

    def test_h2_refused_stream_retried(self):
        state = {"calls": 0}

        def handler(request, conn):
            state["calls"] += 1
            if state["calls"] == 1:
                conn.closed = True
                raise StreamResetError(1, H2Reason.REFUSED_STREAM)
            return FakeHTTPunkResponse(200)

        client, _ = make_client(handler, multiplexed=True)

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                assert response.status_code == 200
                await response.read()

        run(main())

    def test_goaway_with_error_code_not_retried(self):
        def handler(request, conn):
            conn.closed = True
            raise GoAwayError(0, H2Reason.INTERNAL_ERROR, b"")

        client, _ = make_client(handler, multiplexed=True)

        async def main():
            async with client:
                with pytest.raises(punkreq.RemoteProtocolError):
                    await client.get("https://example.com/")

        run(main())

    def test_h1_idle_reuse_race_retried(self):
        def handler(request, conn):
            # the reused first connection dies on the second exchange
            if request.target == "/second" and conn is connector.connections[0]:
                conn.closed = True
                raise ConnectionClosedError("server closed idle connection")
            return FakeHTTPunkResponse(200)

        client, connector = make_client(handler)

        async def main():
            async with client:
                await (await client.get("https://example.com/first")).read()
                response = await client.get("https://example.com/second")
                assert response.status_code == 200
                await response.read()

        run(main())
        assert connector.dials == 2

    def test_fresh_h1_connection_close_not_retried(self):
        def handler(request, conn):
            conn.closed = True
            raise ConnectionClosedError("boom")

        client, connector = make_client(handler)

        async def main():
            async with client:
                with pytest.raises(punkreq.RemoteProtocolError):
                    await client.get("https://example.com/")

        run(main())
        assert connector.dials == 1

    def test_non_replayable_body_not_retried(self):
        def handler(request, conn):
            conn.closed = True
            raise GoAwayError(0, H2Reason.NO_ERROR, b"")

        client, connector = make_client(handler, multiplexed=True)

        async def main():
            async with client:
                with pytest.raises(punkreq.RemoteProtocolError):
                    await client.post("https://example.com/", content=iter([b"x"]))

        run(main())
        assert connector.dials == 1


class TestTimeouts:
    def test_read_timeout_on_response_head(self):
        async def slow(request, conn):
            await asyncio.sleep(1.0)
            return FakeHTTPunkResponse(200)

        client, _ = make_client(slow, timeout=punkreq.Timeout(None, read=0.02))

        async def main():
            async with client:
                with pytest.raises(punkreq.ReadTimeout):
                    await client.get("https://example.com/")

        run(main())

    def test_per_request_timeout_overrides_client(self):
        async def slow(request, conn):
            await asyncio.sleep(0.05)
            return FakeHTTPunkResponse(200)

        client, _ = make_client(slow, timeout=punkreq.Timeout(None, read=0.01))

        async def main():
            async with client:
                response = await client.get("https://example.com/", timeout=punkreq.Timeout(None, read=5.0))
                assert response.status_code == 200
                await response.read()

        run(main())

    def test_total_timeout_on_slow_head(self):
        async def slow(request, conn):
            await asyncio.sleep(1.0)
            return FakeHTTPunkResponse(200)

        client, _ = make_client(slow, timeout=punkreq.Timeout(None, total=0.03))

        async def main():
            async with client:
                with pytest.raises(punkreq.TimeoutException):
                    await client.get("https://example.com/")

        run(main())

    def test_total_timeout_covers_body_reads(self):
        def handler(request, conn):
            return FakeHTTPunkResponse(200, body=[b"a", b"b", b"c", b"d"], chunk_delay=0.02)

        client, _ = make_client(handler, timeout=punkreq.Timeout(None, total=0.05))

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                with pytest.raises(punkreq.TimeoutException):
                    await response.read()

        run(main())

    def test_total_timeout_spans_redirect_hops(self):
        async def handler(request, conn):
            await asyncio.sleep(0.03)
            if request.target == "/a":
                return FakeHTTPunkResponse(302, headers={"location": "/b"})
            return FakeHTTPunkResponse(200)

        client, _ = make_client(handler, timeout=punkreq.Timeout(None, total=0.05))

        async def main():
            async with client:
                # each hop takes 0.03s; the shared 0.05s deadline dies on hop two
                with pytest.raises(punkreq.TimeoutException):
                    await client.get("https://example.com/a")

        run(main())

    def test_total_timeout_generous_enough_passes(self):
        async def handler(request, conn):
            await asyncio.sleep(0.01)
            return FakeHTTPunkResponse(200, body=b"ok")

        client, _ = make_client(handler, timeout=punkreq.Timeout(None, total=5.0))

        async def main():
            async with client:
                response = await client.get("https://example.com/")
                assert await response.text() == "ok"

        run(main())
