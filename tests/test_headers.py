import pytest
from httpunk import HeaderMap

from punkreq import Headers


class TestConstruction:
    def test_from_dict(self):
        headers = Headers({"Accept": "*/*"})
        assert headers["accept"] == "*/*"
        assert headers["Accept"] == "*/*"

    def test_from_tuples_multi(self):
        headers = Headers([("set-cookie", "a=1"), ("Set-Cookie", "b=2")])
        assert headers.get_list("set-cookie") == ["a=1", "b=2"]

    def test_from_headermap_and_back(self):
        header_map = HeaderMap({"x-a": "1"})
        headers = Headers(header_map)
        assert headers["x-a"] == "1"
        assert headers.raw == [("x-a", b"1")]

    def test_copy_is_independent(self):
        headers = Headers({"a": "1"})
        copy = headers.copy()
        copy["a"] = "2"
        assert headers["a"] == "1"

    def test_empty(self):
        headers = Headers()
        assert len(headers) == 0
        assert not headers

    def test_invalid_name_raises(self):
        with pytest.raises(ValueError):
            Headers({"bad name": "x"})


class TestMappingAPI:
    def test_multi_values_comma_joined(self):
        headers = Headers([("accept-encoding", "gzip"), ("accept-encoding", "br")])
        assert headers["accept-encoding"] == "gzip, br"

    def test_len_and_iter_distinct_keys(self):
        headers = Headers([("a", "1"), ("a", "2"), ("b", "3")])
        assert len(headers) == 2
        assert list(headers) == ["a", "b"]
        assert dict(headers) == {"a": "1, 2", "b": "3"}

    def test_setitem_replaces(self):
        headers = Headers([("a", "1"), ("a", "2")])
        headers["a"] = "3"
        assert headers.get_list("a") == ["3"]

    def test_add_appends(self):
        headers = Headers({"a": "1"})
        headers.add("a", "2")
        assert headers.get_list("a") == ["1", "2"]

    def test_delitem(self):
        headers = Headers({"a": "1"})
        del headers["A"]
        assert "a" not in headers
        with pytest.raises(KeyError):
            del headers["a"]

    def test_missing_key(self):
        headers = Headers()
        with pytest.raises(KeyError):
            headers["nope"]
        assert headers.get("nope") is None
        assert headers.get("nope", "d") == "d"
        assert headers.get_list("nope") == []

    def test_setdefault(self):
        headers = Headers({"a": "1"})
        assert headers.setdefault("a", "9") == "1"
        assert headers.setdefault("b", "2") == "2"
        assert headers["b"] == "2"

    def test_update_replaces_all_values(self):
        headers = Headers([("a", "1"), ("a", "2"), ("b", "3")])
        headers.update([("a", "9"), ("c", "4")])
        assert headers.get_list("a") == ["9"]
        assert headers["b"] == "3"
        assert headers["c"] == "4"

    def test_get_list_split_commas(self):
        headers = Headers([("via", "a, b"), ("via", "c")])
        assert headers.get_list("via") == ["a, b", "c"]
        assert headers.get_list("via", split_commas=True) == ["a", "b", "c"]


class TestEqualityAndRepr:
    def test_eq(self):
        assert Headers({"a": "1"}) == Headers({"A": "1"})
        assert Headers({"a": "1"}) == {"A": "1"}
        assert Headers({"a": "1"}) == [("a", "1")]
        assert Headers({"a": "1"}) != Headers({"a": "2"})

    def test_repr_dict_form(self):
        assert repr(Headers({"accept": "*/*"})) == "Headers({'accept': '*/*'})"

    def test_repr_list_form_on_duplicates(self):
        assert repr(Headers([("a", "1"), ("a", "2")])) == "Headers([('a', '1'), ('a', '2')])"

    def test_repr_obfuscates_sensitive(self):
        headers = Headers({"Authorization": "Basic dTpw", "Proxy-Authorization": "x"})
        assert "dTpw" not in repr(headers)
        assert "[secure]" in repr(headers)


class TestBytesHandling:
    def test_bytes_values_accepted(self):
        headers = Headers()
        headers["x-a"] = b"raw"
        assert headers["x-a"] == "raw"

    def test_non_utf8_value_decodes_latin1(self):
        headers = Headers()
        headers["x-a"] = b"caf\xe9"
        assert headers["x-a"] == "café"

    def test_utf8_str_roundtrip(self):
        headers = Headers({"x-a": "café"})
        assert headers["x-a"] == "café"
        assert headers.raw == [("x-a", "café".encode())]
