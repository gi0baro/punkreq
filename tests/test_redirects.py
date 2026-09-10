import pytest

import punkreq
from punkreq import Request, Response
from punkreq._content import IteratorByteStream
from punkreq._redirects import build_redirect_request


def redirect_response(request, status=302, location="https://example.com/next", **headers):
    return Response(status, headers={"location": location, **headers}, request=request)


# ----- method and body -----


def test_redirect_get_follows_get():
    request = Request("GET", "https://example.com/a")
    next_request = build_redirect_request(request, redirect_response(request))
    assert next_request.method == "GET"
    assert next_request.url == "https://example.com/next"


@pytest.mark.parametrize("status", [301, 302, 303])
def test_redirect_post_becomes_get_and_body_dropped(status):
    request = Request("POST", "https://example.com/a", json={"x": 1})
    next_request = build_redirect_request(request, redirect_response(request, status=status))
    assert next_request.method == "GET"
    assert next_request.content == b""
    assert "content-length" not in next_request.headers
    assert "content-type" not in next_request.headers


def test_redirect_303_turns_put_into_get():
    request = Request("PUT", "https://example.com/a", content=b"x")
    next_request = build_redirect_request(request, redirect_response(request, status=303))
    assert next_request.method == "GET"


def test_redirect_303_head_stays_head():
    request = Request("HEAD", "https://example.com/a")
    next_request = build_redirect_request(request, redirect_response(request, status=303))
    assert next_request.method == "HEAD"


@pytest.mark.parametrize("status", [307, 308])
def test_redirect_307_preserves_method_and_body(status):
    request = Request("POST", "https://example.com/a", content=b"payload")
    next_request = build_redirect_request(request, redirect_response(request, status=status))
    assert next_request.method == "POST"
    assert next_request.content == b"payload"
    assert next_request.headers["content-length"] == "7"


def test_redirect_307_with_non_replayable_body_not_followed():
    request = Request("POST", "https://example.com/a", content=iter([b"a", b"b"]))
    assert isinstance(request.stream, IteratorByteStream)
    response = redirect_response(request, status=307)
    assert build_redirect_request(request, response) is None


# ----- headers -----


def test_redirect_sensitive_headers_kept_same_origin():
    request = Request("GET", "https://example.com/a", headers={"authorization": "Basic x", "cookie": "k=v"})
    next_request = build_redirect_request(request, redirect_response(request, location="/next"))
    assert next_request.headers["authorization"] == "Basic x"
    assert next_request.headers["cookie"] == "k=v"


@pytest.mark.parametrize(
    "location",
    [
        "https://other.org/next",  # host change
        "https://example.com:8443/next",  # port change
        "http://example.com/next",  # scheme change
    ],
)
def test_redirect_sensitive_headers_stripped_cross_origin(location):
    request = Request(
        "GET",
        "https://example.com/a",
        headers={"authorization": "Basic x", "cookie": "k=v", "proxy-authorization": "y", "x-custom": "kept"},
    )
    next_request = build_redirect_request(request, redirect_response(request, location=location))
    assert "authorization" not in next_request.headers
    assert "cookie" not in next_request.headers
    assert "proxy-authorization" not in next_request.headers
    assert next_request.headers["x-custom"] == "kept"


def test_redirect_host_header_updated():
    request = Request("GET", "https://example.com/a")
    next_request = build_redirect_request(request, redirect_response(request, location="https://other.org:8443/next"))
    assert next_request.headers["host"] == "other.org:8443"


def test_redirect_referer_set():
    request = Request("GET", "https://user:pass@example.com/a#frag")
    response = redirect_response(request, location="https://other.org/next")
    next_request = build_redirect_request(request, response)
    assert next_request.headers["referer"] == "https://example.com/a"  # userinfo/fragment stripped


def test_redirect_referer_not_set_on_https_to_http_downgrade():
    request = Request("GET", "https://example.com/a")
    response = redirect_response(request, location="http://example.com/next")
    next_request = build_redirect_request(request, response)
    assert "referer" not in next_request.headers


def test_redirect_referer_disabled():
    request = Request("GET", "https://example.com/a")
    next_request = build_redirect_request(request, redirect_response(request), referer=False)
    assert "referer" not in next_request.headers


# ----- location -----


def test_redirect_relative_location_joined():
    request = Request("GET", "https://example.com/a/b")
    next_request = build_redirect_request(request, redirect_response(request, location="../c"))
    assert next_request.url == "https://example.com/c"


def test_redirect_fragment_inherited():
    request = Request("GET", "https://example.com/a#section")
    next_request = build_redirect_request(request, redirect_response(request, location="/next"))
    assert next_request.url.fragment == "section"


def test_redirect_unsupported_scheme_raises():
    request = Request("GET", "https://example.com/a")
    response = redirect_response(request, location="ftp://example.com/file")
    with pytest.raises(punkreq.UnsupportedProtocol):
        build_redirect_request(request, response)


def test_redirect_timeout_and_deadline_propagated():
    request = Request("GET", "https://example.com/a", timeout=punkreq.Timeout(5.0, total=30.0))
    request._deadline = 123.45  # pinned by Client.send before the first hop
    next_request = build_redirect_request(request, redirect_response(request))
    assert next_request.timeout == punkreq.Timeout(5.0, total=30.0)
    assert next_request._deadline == 123.45
