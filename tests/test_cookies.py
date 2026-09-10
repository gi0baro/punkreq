import threading
import time

import pytest

import punkreq
from punkreq import Cookies, Request, Response


# ----- mapping API -----


def test_cookies_dict_init_and_mapping():
    cookies = Cookies({"k": "v"})
    assert cookies["k"] == "v"
    assert "k" in cookies
    assert len(cookies) == 1
    cookies["other"] = "x"
    assert cookies.get("other") == "x"
    del cookies["other"]
    assert "other" not in cookies


def test_cookies_get_missing():
    cookies = Cookies()
    assert cookies.get("nope") is None
    assert cookies.get("nope", "default") == "default"
    with pytest.raises(KeyError):
        cookies["nope"]


def test_cookies_domain_scoping_and_conflict():
    cookies = Cookies()
    cookies.set("k", "a", domain="example.com")
    cookies.set("k", "b", domain="other.org")
    with pytest.raises(punkreq.CookieConflict):
        cookies.get("k")
    assert cookies.get("k", domain="example.com") == "a"
    assert cookies.get("k", domain="other.org") == "b"


def test_cookies_update_and_copy_init():
    cookies = Cookies({"a": "1"})
    other = Cookies(cookies)
    other.set("b", "2")
    assert "b" not in cookies
    cookies.update(other)
    assert cookies["b"] == "2"


def test_cookies_clear():
    cookies = Cookies({"a": "1", "b": "2"})
    cookies.clear()
    assert len(cookies) == 0


# ----- request/response wiring -----


def test_cookies_extract_and_send():
    cookies = Cookies()
    request = Request("GET", "https://example.com/login")
    response = Response(200, headers={"set-cookie": "session=abc123; Path=/"}, request=request)
    cookies.extract_cookies(response)
    assert cookies["session"] == "abc123"

    next_request = Request("GET", "https://example.com/dash")
    cookies.set_cookie_header(next_request)
    assert next_request.headers["cookie"] == "session=abc123"


def test_cookies_explicit_cookie_header_not_overwritten():
    cookies = Cookies({"session": "from-jar"})
    request = Request("GET", "https://example.com/", headers={"cookie": "session=explicit"})
    cookies.set_cookie_header(request)
    assert request.headers["cookie"] == "session=explicit"


# ----- thread safety -----


def test_cookies_concurrent_mutation_and_read():
    """Reads must not blow up while another thread mutates the jar — a real
    parallelism test on free-threaded builds, a smoke test elsewhere."""
    cookies = Cookies()
    request = Request("GET", "https://example.com/")
    stop = threading.Event()
    errors: list[BaseException] = []

    def writer():
        i = 0
        while not stop.is_set():
            response = Response(200, headers={"set-cookie": f"k{i % 20}=v{i}; Path=/"}, request=request)
            cookies.extract_cookies(response)
            i += 1
            if i % 50 == 0:
                cookies.clear()

    def reader():
        while not stop.is_set():
            try:
                cookies.get("k0")
                len(cookies)
                _ = "k1" in cookies
                list(cookies)
                bool(cookies)
            except punkreq.CookieConflict:
                pass
            except BaseException as exc:  # noqa: B036 - the racing RuntimeError is the point
                errors.append(exc)
                stop.set()

    threads = [threading.Thread(target=writer), threading.Thread(target=reader), threading.Thread(target=reader)]
    for thread in threads:
        thread.start()
    time.sleep(0.3)
    stop.set()
    for thread in threads:
        thread.join(timeout=5)
    assert errors == []
