# punkreq

punkreq is an async HTTP client for Python, built on top of
[httpunk](https://github.com/gi0baro/httpunk).

> **Warning:** punkreq is in an early stage and still work in progress.

> **Note:** punkreq was built with substantial help from LLMs, under human supervision.

## In a nutshell

- Async only, with multiple runtime backends: import the client from the
  module matching your runtime – `punkreq.asyncio` or `punkreq.tonio`
- HTTP/1.1 and HTTP/2, negotiated via TLS ALPN by default; HTTP/2 prior
  knowledge with `http1=False`
- Connection pooling with keep-alive (h1) and multiplexing (h2)
- Transparent response decompression: gzip and deflate everywhere, zstd on
  Python 3.14+, brotli with the `punkreq[brotli]` extra
- Conservative automatic retries — only requests the server provably never
  processed (HTTP/2 graceful GOAWAY, refused streams, keep-alive races)
- Opt-in cookie jar, basic/bearer auth, multipart uploads
- HTTP proxies: `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` environment support,
  CONNECT tunneling for https destinations.
- Zero runtime dependencies beyond httpunk

## Installation

```shell
pip install punkreq
```

## Quickstart

```python
import asyncio
from punkreq.asyncio import Client

async def main():
    async with Client(base_url="https://api.example.com") as client:
        response = await client.post("/items", json={"name": "widget"})
        response.raise_for_status()
        print(await response.json())

asyncio.run(main())
```

One-off requests without managing a client:

```python
from punkreq import asyncio as punkreq

response = await punkreq.get("https://www.example.com")
print(response.status_code, response.headers["content-type"])
```

Responses resolve as soon as the head arrives; the body is read on demand —
`await response.read()` / `text()` / `json()`, or streamed:

```python
async with Client() as client:
    response = await client.get("https://example.com/large.bin")
    async for chunk in response.iter_bytes(chunk_size=65536):
        ...
```

Reading to the end releases the connection automatically. When you might stop
early, use the request as a context manager so the response is always closed:

```python
async with client.get("https://example.com/huge.ndjson") as response:
    async for line in response.iter_lines():
        if found(line):
            break  # the block exit closes the response and frees the connection
```

## Configuration

```python
import punkreq
from punkreq.asyncio import Client

client = Client(
    base_url="https://api.example.com",
    headers={"x-api-key": "..."},
    auth=("user", "pass"),                       # or punkreq.BearerAuth("token")
    cookies={},                                  # enables the cookie jar (off by default)
    timeout=punkreq.Timeout(5.0, total=30.0),    # no timeouts by default
    limits=punkreq.Limits(max_connections=100),
    proxy="http://proxy.internal:3128",          # env vars honored by default
    verify=True,                                 # bool | CA bundle path | ssl.SSLContext
)
```

By default: redirects on (max 10), no timeouts, no cookie jar unless requested, 
HTTP/2 enabled, 90s keep-alive expiry, unlimited connections.

`Timeout` has four independent fields — `total` (a deadline for the whole
request, redirects included), `connect`, `read` and `pool`. There is no write
timeout: request bodies are written full-duplex from background tasks, so
`total` is the bound for slow uploads.

## Backends

punkreq never asks for a backend at call sites — pick it by import:

```python
from punkreq.asyncio import Client   # asyncio, available everywhere
from punkreq.tonio import Client     # tonio, free-threaded CPython >= 3.14 on Unix
```

## License

punkreq is released under the BSD License.
