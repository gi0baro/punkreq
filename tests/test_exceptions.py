import pytest

import punkreq


class TestHierarchy:
    @pytest.mark.parametrize(
        "exc,parent",
        [
            (punkreq.ConnectTimeout, punkreq.TimeoutException),
            (punkreq.ReadTimeout, punkreq.TimeoutException),
            (punkreq.PoolTimeout, punkreq.TimeoutException),
            (punkreq.TimeoutException, punkreq.TransportError),
            (punkreq.ConnectError, punkreq.NetworkError),
            (punkreq.ReadError, punkreq.NetworkError),
            (punkreq.WriteError, punkreq.NetworkError),
            (punkreq.CloseError, punkreq.NetworkError),
            (punkreq.NetworkError, punkreq.TransportError),
            (punkreq.LocalProtocolError, punkreq.ProtocolError),
            (punkreq.RemoteProtocolError, punkreq.ProtocolError),
            (punkreq.ProtocolError, punkreq.TransportError),
            (punkreq.ProxyError, punkreq.TransportError),
            (punkreq.UnsupportedProtocol, punkreq.TransportError),
            (punkreq.TransportError, punkreq.RequestError),
            (punkreq.DecodingError, punkreq.RequestError),
            (punkreq.TooManyRedirects, punkreq.RequestError),
            (punkreq.RequestError, punkreq.HTTPError),
            (punkreq.HTTPStatusError, punkreq.HTTPError),
        ],
    )
    def test_parents(self, exc, parent):
        assert issubclass(exc, parent)

    def test_stream_errors_are_runtime_errors(self):
        for exc in (
            punkreq.StreamConsumed,
            punkreq.StreamClosed,
            punkreq.RequestNotRead,
        ):
            assert issubclass(exc, punkreq.StreamError)
            assert issubclass(exc, RuntimeError)
            assert not issubclass(exc, punkreq.HTTPError)

    def test_invalid_url_not_under_http_error(self):
        assert not issubclass(punkreq.InvalidURL, punkreq.HTTPError)


class TestRequestAttachment:
    def test_request_error_carries_request(self):
        request = object()
        error = punkreq.ConnectError("boom", request=request)
        assert error.request is request
        assert error.message == "boom"

    def test_unset_request_raises(self):
        error = punkreq.ConnectError("boom")
        with pytest.raises(RuntimeError):
            error.request

    def test_request_settable(self):
        error = punkreq.ConnectError("boom")
        request = object()
        error.request = request
        assert error.request is request

    def test_http_status_error(self):
        request, response = object(), object()
        error = punkreq.HTTPStatusError("404", request=request, response=response)
        assert error.request is request
        assert error.response is response

    def test_stream_errors_have_default_messages(self):
        assert "streamed" in str(punkreq.StreamConsumed())
        assert "closed" in str(punkreq.StreamClosed())
