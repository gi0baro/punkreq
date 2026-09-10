import pytest

import punkreq


def test_stream_errors_are_runtime_errors():
    for exc in (
        punkreq.StreamConsumed,
        punkreq.StreamClosed,
        punkreq.RequestNotRead,
    ):
        assert issubclass(exc, punkreq.StreamError)
        assert issubclass(exc, RuntimeError)
        assert not issubclass(exc, punkreq.HTTPError)


def test_invalid_url_not_under_http_error():
    assert not issubclass(punkreq.InvalidURL, punkreq.HTTPError)


def test_request_error_carries_request():
    request = object()
    error = punkreq.ConnectError("boom", request=request)
    assert error.request is request
    assert error.message == "boom"


def test_request_error_unset_request_raises():
    error = punkreq.ConnectError("boom")
    with pytest.raises(RuntimeError):
        error.request


def test_request_error_request_settable():
    error = punkreq.ConnectError("boom")
    request = object()
    error.request = request
    assert error.request is request


def test_http_status_error_carries_request_and_response():
    request, response = object(), object()
    error = punkreq.HTTPStatusError("404", request=request, response=response)
    assert error.request is request
    assert error.response is response


def test_stream_errors_have_default_messages():
    assert "streamed" in str(punkreq.StreamConsumed())
    assert "closed" in str(punkreq.StreamClosed())
