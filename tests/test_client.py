import asyncio

import pytest
from httpunk import GoAwayError, H2Reason, StreamResetError
from httpunk.exceptions import ConnectionClosedError

import punkreq
from tests.fakes import FakeHttpunkResponse, ok_handler


pytestmark = pytest.mark.asyncio


# ----- request shape -----


async def test_get_request_wire_shape(make_client):
    client, connector = make_client(ok_handler())
    async with client:
        response = await client.get("https://example.com/path?x=1")
        assert response.status_code == 200
        assert await response.text() == "hello"
        assert response.http_version == "HTTP/1.1"
    (request,) = connector.requests
    assert request.method == "GET"
    assert request.target == "/path?x=1"
    assert request.headers["host"] == b"example.com"
    assert request.headers["accept"] == b"*/*"
    assert b"gzip" in request.headers["accept-encoding"]
    assert request.headers["user-agent"].startswith(b"python-punkreq/")


async def test_range_request_suppresses_auto_accept_encoding(make_client):
    client, connector = make_client(ok_handler())
    async with client:
        await (await client.get("https://example.com/file", headers={"range": "bytes=0-99"})).read()
    (request,) = connector.requests
    assert "accept-encoding" not in request.headers
    assert request.headers["range"] == b"bytes=0-99"


async def test_explicit_accept_encoding_kept_with_range(make_client):
    client, connector = make_client(ok_handler())
    async with client:
        response = await client.get(
            "https://example.com/file", headers={"range": "bytes=0-99", "accept-encoding": "identity"}
        )
        await response.read()
    (request,) = connector.requests
    assert request.headers["accept-encoding"] == b"identity"


async def test_explicit_accept_encoding_not_overridden(make_client):
    client, connector = make_client(ok_handler(), headers={"accept-encoding": "gzip"})
    async with client:
        await (await client.get("https://example.com/")).read()
    (request,) = connector.requests
    assert request.headers["accept-encoding"] == b"gzip"


async def test_h2_request_drops_host_header(make_client):
    client, connector = make_client(ok_handler(), multiplexed=True)
    async with client:
        response = await client.get("https://example.com/")
        assert response.http_version == "HTTP/2"
        await response.read()
    (request,) = connector.requests
    assert "host" not in request.headers


async def test_base_url_and_params_merge(make_client):
    client, connector = make_client(ok_handler(), base_url="https://api.example.com/v1", params={"key": "K"})
    async with client:
        await (await client.get("users", params={"page": 2})).read()
    (request,) = connector.requests
    assert request.target == "/v1/users?key=K&page=2"


async def test_post_json_body(make_client):
    client, connector = make_client(ok_handler())
    async with client:
        await (await client.post("https://example.com/items", json={"a": 1})).read()
    (request,) = connector.requests
    assert request.method == "POST"
    assert request.headers["content-type"] == b"application/json"
    assert request.body == b'{"a":1}'


async def test_unsupported_url_scheme_raises(make_client):
    client, _ = make_client(ok_handler())
    async with client:
        with pytest.raises(punkreq.UnsupportedProtocol):
            await client.get("ftp://example.com/")
        with pytest.raises(punkreq.UnsupportedProtocol):
            await client.get("/relative-without-base")


async def test_elapsed_set_after_read(make_client):
    client, _ = make_client(ok_handler())
    async with client:
        response = await client.get("https://example.com/")
        await response.read()
        assert response.elapsed.total_seconds() >= 0


async def test_streamed_body_releases_connection_at_eof(make_client):
    client, _ = make_client(ok_handler(body=[b"chunk1", b"chunk2"]))
    async with client:
        response = await client.get("https://example.com/big")
        chunks = [chunk async for chunk in response.iter_bytes()]
        assert chunks == [b"chunk1", b"chunk2"]
        # reading to EOF released the h1 connection back to the pool
        assert client._pool.idle_count == 1


async def test_response_context_manager_closes_abandoned_body(make_client):
    client, _ = make_client(ok_handler(body=[b"chunk1", b"chunk2"]))
    async with client:
        async with client.get("https://example.com/big") as response:
            async for _chunk in response.iter_bytes():
                break  # abandon the body early
        # the context manager closed the response; connection evicted, not leaked
        assert response.is_closed
        assert client._pool.connection_count == 0


# ----- redirects -----


async def test_redirect_followed_with_history(make_client):
    def handler(request, conn):
        if request.target == "/old":
            return FakeHttpunkResponse(302, headers={"location": "/new"})
        return FakeHttpunkResponse(200, body=b"arrived")

    client, connector = make_client(handler)
    async with client:
        response = await client.get("https://example.com/old")
        assert response.status_code == 200
        assert await response.text() == "arrived"
        assert [r.status_code for r in response.history] == [302]
        assert str(response.url) == "https://example.com/new"
    assert [r.target for r in connector.requests] == ["/old", "/new"]


async def test_redirect_302_turns_post_into_get(make_client):
    def handler(request, conn):
        if request.method == "POST":
            return FakeHttpunkResponse(302, headers={"location": "/done"})
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler)
    async with client:
        await (await client.post("https://example.com/form", data={"a": "1"})).read()
    second = connector.requests[1]
    assert second.method == "GET"
    assert second.body is None
    assert "content-type" not in second.headers


async def test_redirect_307_replays_body(make_client):
    def handler(request, conn):
        if request.target == "/a":
            return FakeHttpunkResponse(307, headers={"location": "/b"})
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler)
    async with client:
        await (await client.post("https://example.com/a", content=b"payload")).read()
    requests = connector.requests
    assert [r.method for r in requests] == ["POST", "POST"]
    assert requests[1].body == b"payload"


async def test_redirect_307_with_streaming_body_not_followed(make_client):
    client, _ = make_client(lambda request, conn: FakeHttpunkResponse(307, headers={"location": "/b"}))
    async with client:
        response = await client.post("https://example.com/a", content=iter([b"x"]))
        assert response.status_code == 307
        await response.close()


async def test_redirect_follow_disabled(make_client):
    client, _ = make_client(lambda request, conn: FakeHttpunkResponse(302, headers={"location": "/x"}))
    async with client:
        response = await client.get("https://example.com/", follow_redirects=False)
        assert response.status_code == 302
        await response.close()


async def test_redirect_too_many_raises(make_client):
    client, _ = make_client(
        lambda request, conn: FakeHttpunkResponse(302, headers={"location": "/loop"}), max_redirects=3
    )
    async with client:
        with pytest.raises(punkreq.TooManyRedirects):
            await client.get("https://example.com/loop")


async def test_redirect_strips_auth_cross_origin(make_client):
    def handler(request, conn):
        if request.headers.get("host") == b"example.com":
            return FakeHttpunkResponse(302, headers={"location": "https://other.org/next"})
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler, auth=("user", "pass"))
    async with client:
        await (await client.get("https://example.com/")).read()
    first, second = connector.requests
    assert "authorization" in first.headers
    assert "authorization" not in second.headers


# ----- cookies -----


async def test_cookie_jar_roundtrip_across_requests(make_client):
    def handler(request, conn):
        if request.target == "/login":
            return FakeHttpunkResponse(200, headers={"set-cookie": "session=abc; Path=/"})
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler, cookies={})
    async with client:
        await (await client.get("https://example.com/login")).read()
        await (await client.get("https://example.com/dash")).read()
    second = connector.requests[1]
    assert second.headers["cookie"] == b"session=abc"


async def test_cookie_set_on_redirect_hop(make_client):
    def handler(request, conn):
        if request.target == "/a":
            return FakeHttpunkResponse(302, headers={"location": "/b", "set-cookie": "hop=1; Path=/"})
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler, cookies={})
    async with client:
        await (await client.get("https://example.com/a")).read()
    second = connector.requests[1]
    assert second.headers["cookie"] == b"hop=1"


async def test_no_cookie_jar_by_default(make_client):
    def handler(request, conn):
        if request.target == "/login":
            return FakeHttpunkResponse(200, headers={"set-cookie": "session=abc; Path=/"})
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler)
    async with client:
        await (await client.get("https://example.com/login")).read()
        await (await client.get("https://example.com/dash")).read()
    second = connector.requests[1]
    assert "cookie" not in second.headers


# ----- auth -----


async def test_client_basic_auth(make_client):
    client, connector = make_client(ok_handler(), auth=("user", "pass"))
    async with client:
        await (await client.get("https://example.com/")).read()
    (request,) = connector.requests
    assert request.headers["authorization"] == b"Basic dXNlcjpwYXNz"


async def test_per_request_auth_overrides_client_auth(make_client):
    client, connector = make_client(ok_handler(), auth=("user", "pass"))
    async with client:
        await (await client.get("https://example.com/", auth=punkreq.BearerAuth("tok"))).read()
    (request,) = connector.requests
    assert request.headers["authorization"] == b"Bearer tok"


async def test_url_userinfo_becomes_basic_auth(make_client):
    client, connector = make_client(ok_handler())
    async with client:
        response = await client.get("https://user:pass@example.com/")
        assert "@" not in str(response.url)
        await response.read()
    (request,) = connector.requests
    assert request.headers["authorization"] == b"Basic dXNlcjpwYXNz"
    assert request.headers["host"] == b"example.com"


# ----- retries -----


async def test_retry_h2_goaway_on_fresh_connection(make_client):
    state = {"calls": 0}

    def handler(request, conn):
        state["calls"] += 1
        if state["calls"] == 1:
            conn.closed = True
            raise GoAwayError(0, H2Reason.NO_ERROR, b"")
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler, multiplexed=True)
    async with client:
        response = await client.get("https://example.com/")
        assert response.status_code == 200
        await response.read()
    assert connector.dials == 2


async def test_retry_h2_refused_stream(make_client):
    state = {"calls": 0}

    def handler(request, conn):
        state["calls"] += 1
        if state["calls"] == 1:
            conn.closed = True
            raise StreamResetError(1, H2Reason.REFUSED_STREAM)
        return FakeHttpunkResponse(200)

    client, _ = make_client(handler, multiplexed=True)
    async with client:
        response = await client.get("https://example.com/")
        assert response.status_code == 200
        await response.read()


async def test_no_retry_h2_goaway_with_error_code(make_client):
    def handler(request, conn):
        conn.closed = True
        raise GoAwayError(0, H2Reason.INTERNAL_ERROR, b"")

    client, _ = make_client(handler, multiplexed=True)
    async with client:
        with pytest.raises(punkreq.RemoteProtocolError):
            await client.get("https://example.com/")


async def test_retry_h1_idle_reuse_race(make_client):
    def handler(request, conn):
        # the reused first connection dies on the second exchange
        if request.target == "/second" and conn is connector.connections[0]:
            conn.closed = True
            raise ConnectionClosedError("server closed idle connection")
        return FakeHttpunkResponse(200)

    client, connector = make_client(handler)
    async with client:
        await (await client.get("https://example.com/first")).read()
        response = await client.get("https://example.com/second")
        assert response.status_code == 200
        await response.read()
    assert connector.dials == 2


async def test_no_retry_h1_close_on_fresh_connection(make_client):
    def handler(request, conn):
        conn.closed = True
        raise ConnectionClosedError("boom")

    client, connector = make_client(handler)
    async with client:
        with pytest.raises(punkreq.RemoteProtocolError):
            await client.get("https://example.com/")
    assert connector.dials == 1


async def test_no_retry_non_replayable_body(make_client):
    def handler(request, conn):
        conn.closed = True
        raise GoAwayError(0, H2Reason.NO_ERROR, b"")

    client, connector = make_client(handler, multiplexed=True)
    async with client:
        with pytest.raises(punkreq.RemoteProtocolError):
            await client.post("https://example.com/", content=iter([b"x"]))
    assert connector.dials == 1


# ----- timeouts -----


async def test_read_timeout_on_response_head(make_client):
    async def slow(request, conn):
        await asyncio.sleep(1.0)
        return FakeHttpunkResponse(200)

    client, _ = make_client(slow, timeout=punkreq.Timeout(None, read=0.02))
    async with client:
        with pytest.raises(punkreq.ReadTimeout):
            await client.get("https://example.com/")


async def test_per_request_timeout_overrides_client_timeout(make_client):
    async def slow(request, conn):
        await asyncio.sleep(0.05)
        return FakeHttpunkResponse(200)

    client, _ = make_client(slow, timeout=punkreq.Timeout(None, read=0.01))
    async with client:
        response = await client.get("https://example.com/", timeout=punkreq.Timeout(None, read=5.0))
        assert response.status_code == 200
        await response.read()


async def test_total_timeout_on_slow_head(make_client):
    async def slow(request, conn):
        await asyncio.sleep(1.0)
        return FakeHttpunkResponse(200)

    client, _ = make_client(slow, timeout=punkreq.Timeout(None, total=0.03))
    async with client:
        with pytest.raises(punkreq.TimeoutException):
            await client.get("https://example.com/")


async def test_total_timeout_covers_body_reads(make_client):
    def handler(request, conn):
        return FakeHttpunkResponse(200, body=[b"a", b"b", b"c", b"d"], chunk_delay=0.02)

    client, _ = make_client(handler, timeout=punkreq.Timeout(None, total=0.05))
    async with client:
        response = await client.get("https://example.com/")
        with pytest.raises(punkreq.TimeoutException):
            await response.read()


async def test_total_timeout_spans_redirect_hops(make_client):
    async def handler(request, conn):
        await asyncio.sleep(0.03)
        if request.target == "/a":
            return FakeHttpunkResponse(302, headers={"location": "/b"})
        return FakeHttpunkResponse(200)

    client, _ = make_client(handler, timeout=punkreq.Timeout(None, total=0.05))
    async with client:
        # each hop takes 0.03s; the shared 0.05s deadline dies on hop two
        with pytest.raises(punkreq.TimeoutException):
            await client.get("https://example.com/a")


async def test_total_timeout_generous_enough_passes(make_client):
    async def handler(request, conn):
        await asyncio.sleep(0.01)
        return FakeHttpunkResponse(200, body=b"ok")

    client, _ = make_client(handler, timeout=punkreq.Timeout(None, total=5.0))
    async with client:
        response = await client.get("https://example.com/")
        assert await response.text() == "ok"
