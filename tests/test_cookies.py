import pytest

import punkreq
from punkreq import Cookies, Request, Response


class TestCookiesAPI:
    def test_dict_init_and_mapping(self):
        cookies = Cookies({"k": "v"})
        assert cookies["k"] == "v"
        assert "k" in cookies
        assert len(cookies) == 1
        cookies["other"] = "x"
        assert cookies.get("other") == "x"
        del cookies["other"]
        assert "other" not in cookies

    def test_get_missing(self):
        cookies = Cookies()
        assert cookies.get("nope") is None
        assert cookies.get("nope", "default") == "default"
        with pytest.raises(KeyError):
            cookies["nope"]

    def test_domain_scoping_and_conflict(self):
        cookies = Cookies()
        cookies.set("k", "a", domain="example.com")
        cookies.set("k", "b", domain="other.org")
        with pytest.raises(punkreq.CookieConflict):
            cookies.get("k")
        assert cookies.get("k", domain="example.com") == "a"
        assert cookies.get("k", domain="other.org") == "b"

    def test_update_and_copy_init(self):
        cookies = Cookies({"a": "1"})
        other = Cookies(cookies)
        other.set("b", "2")
        assert "b" not in cookies
        cookies.update(other)
        assert cookies["b"] == "2"

    def test_clear(self):
        cookies = Cookies({"a": "1", "b": "2"})
        cookies.clear()
        assert len(cookies) == 0


class TestWireBehavior:
    def test_extract_and_send(self):
        cookies = Cookies()
        request = Request("GET", "https://example.com/login")
        response = Response(200, headers={"set-cookie": "session=abc123; Path=/"}, request=request)
        cookies.extract_cookies(response)
        assert cookies["session"] == "abc123"

        next_request = Request("GET", "https://example.com/dash")
        cookies.set_cookie_header(next_request)
        assert next_request.headers["cookie"] == "session=abc123"

    def test_explicit_cookie_header_not_overwritten(self):
        cookies = Cookies({"session": "from-jar"})
        request = Request("GET", "https://example.com/", headers={"cookie": "session=explicit"})
        cookies.set_cookie_header(request)
        assert request.headers["cookie"] == "session=explicit"

    def test_domain_isolation(self):
        cookies = Cookies()
        request = Request("GET", "https://example.com/")
        response = Response(200, headers={"set-cookie": "k=v; Path=/"}, request=request)
        cookies.extract_cookies(response)

        other = Request("GET", "https://other.org/")
        cookies.set_cookie_header(other)
        assert "cookie" not in other.headers

        same = Request("GET", "https://example.com/else")
        cookies.set_cookie_header(same)
        assert same.headers["cookie"] == "k=v"
