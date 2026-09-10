import pytest
from httpunk import HeaderMap

from punkreq import Headers


# ----- construction -----


def test_headers_from_dict():
    headers = Headers({"Accept": "*/*"})
    assert headers["accept"] == "*/*"
    assert headers["Accept"] == "*/*"


def test_headers_from_tuples_multi():
    headers = Headers([("set-cookie", "a=1"), ("Set-Cookie", "b=2")])
    assert headers.get_list("set-cookie") == ["a=1", "b=2"]


def test_headers_from_headermap_and_back():
    header_map = HeaderMap({"x-a": "1"})
    headers = Headers(header_map)
    assert headers["x-a"] == "1"
    assert headers.raw == [("x-a", b"1")]


def test_headers_copy_is_independent():
    headers = Headers({"a": "1"})
    copy = headers.copy()
    copy["a"] = "2"
    assert headers["a"] == "1"


def test_headers_empty():
    headers = Headers()
    assert len(headers) == 0
    assert not headers


# ----- mapping API -----


def test_headers_multi_values_comma_joined():
    headers = Headers([("accept-encoding", "gzip"), ("accept-encoding", "br")])
    assert headers["accept-encoding"] == "gzip, br"


def test_headers_len_and_iter_distinct_keys():
    headers = Headers([("a", "1"), ("a", "2"), ("b", "3")])
    assert len(headers) == 2
    assert list(headers) == ["a", "b"]
    assert dict(headers) == {"a": "1, 2", "b": "3"}


def test_headers_delitem():
    headers = Headers({"a": "1"})
    del headers["A"]
    assert "a" not in headers
    with pytest.raises(KeyError):
        del headers["a"]


def test_headers_missing_key():
    headers = Headers()
    with pytest.raises(KeyError):
        headers["nope"]
    assert headers.get("nope") is None
    assert headers.get("nope", "d") == "d"
    assert headers.get_list("nope") == []


def test_headers_update_replaces_all_values():
    headers = Headers([("a", "1"), ("a", "2"), ("b", "3")])
    headers.update([("a", "9"), ("c", "4")])
    assert headers.get_list("a") == ["9"]
    assert headers["b"] == "3"
    assert headers["c"] == "4"


def test_headers_get_list_split_commas():
    headers = Headers([("via", "a, b"), ("via", "c")])
    assert headers.get_list("via") == ["a, b", "c"]
    assert headers.get_list("via", split_commas=True) == ["a", "b", "c"]


# ----- equality and repr -----


def test_headers_eq():
    assert Headers({"a": "1"}) == Headers({"A": "1"})
    assert Headers({"a": "1"}) == {"A": "1"}
    assert Headers({"a": "1"}) == [("a", "1")]
    assert Headers({"a": "1"}) != Headers({"a": "2"})


def test_headers_repr_dict_form():
    assert repr(Headers({"accept": "*/*"})) == "Headers({'accept': '*/*'})"


def test_headers_repr_list_form_on_duplicates():
    assert repr(Headers([("a", "1"), ("a", "2")])) == "Headers([('a', '1'), ('a', '2')])"


def test_headers_repr_obfuscates_sensitive():
    headers = Headers({"Authorization": "Basic dTpw", "Proxy-Authorization": "x"})
    assert "dTpw" not in repr(headers)
    assert "[secure]" in repr(headers)


# ----- bytes values -----


def test_headers_bytes_values_accepted():
    headers = Headers()
    headers["x-a"] = b"raw"
    assert headers["x-a"] == "raw"


def test_headers_non_utf8_value_decodes_latin1():
    headers = Headers()
    headers["x-a"] = b"caf\xe9"
    assert headers["x-a"] == "café"


def test_headers_utf8_str_roundtrip():
    headers = Headers({"x-a": "café"})
    assert headers["x-a"] == "café"
    assert headers.raw == [("x-a", "café".encode())]
