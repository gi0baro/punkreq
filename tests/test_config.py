import pytest

from punkreq import Limits, Proxy, Timeout


class TestTimeout:
    def test_default_applies_everywhere(self):
        timeout = Timeout(5.0)
        assert timeout.as_dict() == {"connect": 5.0, "read": 5.0, "pool": 5.0, "total": 5.0}

    def test_none_disables_everything(self):
        assert Timeout(None).as_dict() == {"connect": None, "read": None, "pool": None, "total": None}

    def test_default_with_override(self):
        timeout = Timeout(5.0, connect=10.0)
        assert timeout.connect == 10.0
        assert timeout.read == 5.0
        assert timeout.pool == 5.0
        assert timeout.total == 5.0

    def test_total_only(self):
        timeout = Timeout(None, total=30.0)
        assert timeout.total == 30.0
        assert timeout.connect is None
        assert timeout.read is None
        assert timeout.pool is None

    def test_all_explicit(self):
        timeout = Timeout(connect=1.0, read=2.0, pool=3.0, total=4.0)
        assert timeout.as_dict() == {"connect": 1.0, "read": 2.0, "pool": 3.0, "total": 4.0}

    def test_missing_parameters_rejected(self):
        with pytest.raises(ValueError):
            Timeout(connect=5.0)

    def test_from_tuple(self):
        assert Timeout((1.0, 2.0, 3.0, 4.0)) == Timeout(connect=1.0, read=2.0, pool=3.0, total=4.0)
        with pytest.raises(ValueError):
            Timeout((1.0, 2.0))

    def test_copy_with_override(self):
        base = Timeout(5.0)
        assert Timeout(base, read=10.0) == Timeout(5.0, read=10.0)

    def test_no_write_field(self):
        with pytest.raises(TypeError):
            Timeout(5.0, write=1.0)
        assert not hasattr(Timeout(5.0), "write")

    def test_repr(self):
        assert repr(Timeout(5.0)) == "Timeout(timeout=5.0)"
        assert repr(Timeout(5.0, read=2.0)) == "Timeout(connect=5.0, read=2.0, pool=5.0, total=5.0)"


class TestLimits:
    def test_reqwest_shaped_defaults(self):
        limits = Limits()
        assert limits.max_connections is None
        assert limits.max_keepalive_connections is None
        assert limits.keepalive_expiry == 90.0

    def test_eq(self):
        assert Limits(max_connections=10) == Limits(max_connections=10)
        assert Limits(max_connections=10) != Limits()


class TestProxy:
    def test_basic(self):
        proxy = Proxy("http://proxy.example.com:3128")
        assert proxy.url == "http://proxy.example.com:3128"
        assert proxy.auth is None

    def test_userinfo_extracted(self):
        proxy = Proxy("http://user:pass@proxy.example.com")
        assert proxy.auth == ("user", "pass")
        assert proxy.url == "http://proxy.example.com"

    def test_explicit_auth(self):
        proxy = Proxy("http://proxy.example.com", auth=("u", "p"))
        assert proxy.auth == ("u", "p")

    def test_invalid_scheme(self):
        with pytest.raises(ValueError):
            Proxy("socks5://proxy.example.com")

    def test_repr_masks_password(self):
        proxy = Proxy("http://user:secret@proxy.example.com")
        assert "secret" not in repr(proxy)
