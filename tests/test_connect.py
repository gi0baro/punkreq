import ssl

import pytest
from httpunk import Backend

from punkreq._connect import Connector, Origin


pytestmark = pytest.mark.asyncio


class RecordingBackend:
    """The asyncio backend with `connect_tls` recorded and answered with an inert
    stream (the connection is never entered)."""

    def __init__(self, selected):
        self._inner = Backend.asyncio.create()
        self.selected = selected
        self.calls = []

    async def connect_tls(self, host, port, *, alpn=None, ssl_context=None):
        self.calls.append((alpn, ssl_context))
        return object(), self.selected

    def __getattr__(self, name):
        return getattr(self._inner, name)


# The ALPN offer lives on the SSL context, set once at construction: a
# caller-supplied context is never mutated by a dial (two concurrent dials
# configuring it would negotiate each other's offer); a backend-created default
# context (no context given) is configured per dial.


@pytest.mark.parametrize(
    ("http1", "http2", "offer"),
    [(True, True, ("h2", "http/1.1")), (False, True, ("h2",)), (True, False, ("http/1.1",))],
)
async def test_connector_alpn_offer_from_flags(http1, http2, offer):
    backend = RecordingBackend("h2" if http2 else "http/1.1")
    context = ssl.create_default_context()
    connector = Connector(backend=backend, ssl_context=context, http1=http1, http2=http2)
    assert connector.alpn == offer
    await connector(Origin("https", "example.com", 443))
    assert backend.calls == [(None, context)]  # already on the context: no per-dial offer


async def test_connector_default_context_configured_per_dial():
    backend = RecordingBackend("h2")
    connector = Connector(backend=backend)
    await connector(Origin("https", "example.com", 443))
    assert backend.calls == [(("h2", "http/1.1"), None)]
