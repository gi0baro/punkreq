import pytest

from punkreq import Limits, Proxy, Timeout
from punkreq._proxies import ProxyConfig


# ----- Timeout -----


def test_timeout_default_applies_everywhere():
    timeout = Timeout(5.0)
    assert timeout.as_dict() == {"connect": 5.0, "read": 5.0, "pool": 5.0, "total": 5.0}


def test_timeout_none_disables_everything():
    assert Timeout(None).as_dict() == {"connect": None, "read": None, "pool": None, "total": None}


def test_timeout_default_with_override():
    timeout = Timeout(5.0, connect=10.0)
    assert timeout.connect == 10.0
    assert timeout.read == 5.0
    assert timeout.pool == 5.0
    assert timeout.total == 5.0


def test_timeout_total_only():
    timeout = Timeout(None, total=30.0)
    assert timeout.total == 30.0
    assert timeout.connect is None
    assert timeout.read is None
    assert timeout.pool is None


def test_timeout_all_explicit():
    timeout = Timeout(connect=1.0, read=2.0, pool=3.0, total=4.0)
    assert timeout.as_dict() == {"connect": 1.0, "read": 2.0, "pool": 3.0, "total": 4.0}


def test_timeout_missing_parameters_rejected():
    with pytest.raises(ValueError):
        Timeout(connect=5.0)


def test_timeout_from_tuple():
    assert Timeout((1.0, 2.0, 3.0, 4.0)) == Timeout(connect=1.0, read=2.0, pool=3.0, total=4.0)
    with pytest.raises(ValueError):
        Timeout((1.0, 2.0))


def test_timeout_copy_with_override():
    base = Timeout(5.0)
    assert Timeout(base, read=10.0) == Timeout(5.0, read=10.0)


def test_timeout_repr():
    assert repr(Timeout(5.0)) == "Timeout(timeout=5.0)"
    assert repr(Timeout(5.0, read=2.0)) == "Timeout(connect=5.0, read=2.0, pool=5.0, total=5.0)"


# ----- Limits -----


def test_limits_defaults():
    limits = Limits()
    assert limits.max_connections is None
    assert limits.max_keepalive_connections is None
    assert limits.keepalive_expiry == 90.0


def test_limits_eq():
    assert Limits(max_connections=10) == Limits(max_connections=10)
    assert Limits(max_connections=10) != Limits()


# ----- Proxy -----


def test_proxy_basic():
    proxy = Proxy("http://proxy.example.com:3128")
    assert proxy.url == "http://proxy.example.com:3128"
    assert proxy.auth is None


def test_proxy_userinfo_extracted():
    proxy = Proxy("http://user:pass@proxy.example.com")
    assert proxy.auth == ("user", "pass")
    assert proxy.url == "http://proxy.example.com"


def test_proxy_explicit_auth():
    proxy = Proxy("http://proxy.example.com", auth=("u", "p"))
    assert proxy.auth == ("u", "p")


def test_proxy_invalid_scheme():
    with pytest.raises(ValueError):
        Proxy("socks5://proxy.example.com")


def test_proxy_repr_masks_password():
    proxy = Proxy("http://user:secret@proxy.example.com")
    assert "secret" not in repr(proxy)


def test_proxy_config_disabled(monkeypatch):
    monkeypatch.setenv("HTTP_PROXY", "http://should-not-matter:1")
    assert ProxyConfig.resolve(None, trust_env=False) is None
