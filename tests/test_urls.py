import pytest

from punkreq import URL, InvalidURL, QueryParams


# ----- URL: parsing -----


def test_url_components():
    url = URL("https://example.com/path/to/somewhere?abc=123#anchor")
    assert url.scheme == "https"
    assert url.host == "example.com"
    assert url.port is None
    assert url.path == "/path/to/somewhere"
    assert url.query == "abc=123"
    assert url.raw_path == "/path/to/somewhere?abc=123"
    assert url.fragment == "anchor"
    assert str(url) == "https://example.com/path/to/somewhere?abc=123#anchor"


def test_url_empty_path():
    url = URL("http://example.com")
    assert url.path == "/"
    assert url.raw_path == "/"
    assert str(url) == "http://example.com"


def test_url_default_port_normalized():
    assert URL("http://example.com:80/").port is None
    assert URL("https://example.com:443/").port is None
    assert URL("https://example.com:80/").port == 80
    assert str(URL("http://example.com:80/")) == "http://example.com/"


def test_url_userinfo():
    url = URL("https://user:pa%20ss@example.com/")
    assert url.username == "user"
    assert url.password == "pa ss"
    assert url.userinfo == "user:pa%20ss"
    assert url.netloc == "user:pa%20ss@example.com"


def test_url_host_lowercased():
    assert URL("http://EXAMPLE.com/").host == "example.com"


def test_url_ipv6():
    url = URL("http://[::1]:8080/x")
    assert url.host == "::1"
    assert url.port == 8080
    assert url.netloc == "[::1]:8080"
    assert str(url) == "http://[::1]:8080/x"


def test_url_path_encoding():
    url = URL("https://example.com/path to/some where")
    assert url.raw_path == "/path%20to/some%20where"
    assert url.path == "/path to/some where"


def test_url_no_double_encoding():
    url = URL("https://example.com/path%20to/thing")
    assert url.raw_path == "/path%20to/thing"


def test_url_relative():
    url = URL("/path?a=1")
    assert url.is_relative_url
    assert not url.is_absolute_url
    assert url.path == "/path"
    assert str(url) == "/path?a=1"


def test_url_invalid_port():
    with pytest.raises(InvalidURL):
        URL("http://example.com:xyz/")
    with pytest.raises(InvalidURL):
        URL("http://example.com/", port=99999)


def test_url_invalid_type():
    with pytest.raises(TypeError):
        URL(123)


def test_url_params_property():
    url = URL("https://example.com/?a=1&a=2&b=3")
    assert url.params.get_list("a") == ["1", "2"]
    assert url.params["b"] == "3"


# ----- URL: copying -----


def test_url_copy_with_components():
    url = URL("https://example.com/path")
    assert str(url.copy_with(scheme="http")) == "http://example.com/path"
    assert str(url.copy_with(host="other.org")) == "https://other.org/path"
    assert str(url.copy_with(path="/other place")) == "https://example.com/other%20place"
    assert str(url.copy_with(port=8443)) == "https://example.com:8443/path"
    assert str(url.copy_with(fragment="frag")) == "https://example.com/path#frag"


def test_url_copy_with_does_not_mutate():
    url = URL("https://example.com/path")
    url.copy_with(host="other.org")
    assert url.host == "example.com"


def test_url_scheme_change_renormalizes_port():
    url = URL("http://example.com:443/")
    assert url.port == 443
    assert url.copy_with(scheme="https").port is None


def test_url_copy_with_netloc():
    url = URL("https://example.com/path").copy_with(netloc="user@other.org:8443")
    assert url.username == "user"
    assert url.host == "other.org"
    assert url.port == 8443


def test_url_copy_with_username_password():
    url = URL("https://example.com/").copy_with(username="u", password="p:s")
    assert url.username == "u"
    assert url.password == "p:s"
    assert url.userinfo == "u:p%3As"


def test_url_remove_userinfo():
    url = URL("https://user:pass@example.com/").copy_with(userinfo="")
    assert url.username == ""
    assert url.netloc == "example.com"


def test_url_copy_with_params():
    url = URL("https://example.com/?old=1").copy_with(params={"new": "2"})
    assert url.query == "new=2"


def test_url_copy_with_raw_path():
    url = URL("https://example.com/").copy_with(raw_path="/a%20b?x=1")
    assert url.raw_path == "/a%20b?x=1"
    assert url.query == "x=1"


def test_url_copy_with_invalid_component():
    with pytest.raises(TypeError):
        URL("https://example.com/").copy_with(hostname="x")


def test_url_param_helpers():
    url = URL("https://example.com/?a=1")
    assert str(url.copy_set_param("a", 2)) == "https://example.com/?a=2"
    assert str(url.copy_add_param("b", True)) == "https://example.com/?a=1&b=true"
    assert str(url.copy_remove_param("a")) == "https://example.com/"
    assert str(url.copy_merge_params({"b": 2})) == "https://example.com/?a=1&b=2"


def test_url_join_absolute():
    url = URL("https://example.com/a")
    assert url.join("http://other.org/x") == "http://other.org/x"


# ----- URL: equality and repr -----


def test_url_eq():
    assert URL("https://example.com/") == URL("https://example.com/")
    assert URL("https://example.com/") == "https://example.com/"
    assert URL("https://example.com/") != URL("http://example.com/")
    assert hash(URL("https://x.org/")) == hash(URL("https://x.org/"))


def test_url_repr_masks_password():
    assert repr(URL("https://user:secret@example.com/")) == "URL('https://user:[secure]@example.com/')"
    assert "secret" not in repr(URL("https://user:secret@example.com/"))


# ----- QueryParams -----


def test_query_params_from_str():
    params = QueryParams("a=1&a=2&b=3")
    assert params.get_list("a") == ["1", "2"]
    assert params["a"] == "1"
    assert params["b"] == "3"
    assert str(params) == "a=1&a=2&b=3"


def test_query_params_leading_question_mark():
    assert QueryParams("?a=1") == QueryParams("a=1")


def test_query_params_from_dict_and_kwargs():
    assert QueryParams({"a": 1, "b": True, "c": None}) == QueryParams("a=1&b=true&c=")
    assert QueryParams(a=1, b="x") == QueryParams("a=1&b=x")


def test_query_params_from_list_values():
    params = QueryParams({"a": [1, 2]})
    assert params.get_list("a") == ["1", "2"]


def test_query_params_from_tuples():
    params = QueryParams([("a", 1), ("a", 2)])
    assert params.get_list("a") == ["1", "2"]


def test_query_params_empty():
    params = QueryParams()
    assert not params
    assert str(params) == ""
    assert len(params) == 0


def test_query_params_accessors():
    params = QueryParams("a=1&a=2&b=3")
    assert list(params.keys()) == ["a", "b"]
    assert params.values() == ["1", "3"]
    assert params.items() == [("a", "1"), ("b", "3")]
    assert params.multi_items() == [("a", "1"), ("a", "2"), ("b", "3")]
    assert params.get("missing", "default") == "default"
    assert "a" in params
    assert "z" not in params


def test_query_params_immutable_updates():
    params = QueryParams("a=1")
    assert params.set("a", 2) == QueryParams("a=2")
    assert params.add("a", 2) == QueryParams("a=1&a=2")
    assert params.remove("a") == QueryParams()
    assert params.merge({"b": 2}) == QueryParams("a=1&b=2")
    assert params.merge({"a": 9}) == QueryParams("a=9")
    assert params == QueryParams("a=1")  # original untouched


def test_query_params_mutation_forbidden():
    params = QueryParams("a=1")
    with pytest.raises(TypeError):
        params["a"] = "2"
    with pytest.raises(TypeError):
        params.update({"a": "2"})


def test_query_params_encoding_roundtrip():
    params = QueryParams({"k": "a b&c"})
    assert QueryParams(str(params)) == params
