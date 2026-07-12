from __future__ import annotations

import datetime
import typing

from httpunk import Backend

from ._auth import AuthTypes, BasicAuth, coerce_auth
from ._config import DEFAULT_LIMITS, Limits, Timeout, TimeoutTypes
from ._connect import CertTypes, Connector, VerifyTypes, create_ssl_context
from ._content import RequestContent, RequestData
from ._cookies import Cookies, CookieTypes
from ._decoders import ACCEPT_ENCODING
from ._exceptions import TooManyRedirects
from ._headers import Headers, HeaderTypes
from ._models import Request, Response
from ._pool import ConnectionPool
from ._proxies import ProxyConfig, ProxyConnector, ProxyTypes
from ._redirects import build_redirect_request
from ._transport import PoolTransport
from ._urls import URL, QueryParams, QueryParamTypes
from ._version import __version__


__all__ = ["USE_CLIENT_DEFAULT", "BaseClient", "ResponseHandle", "UseClientDefault"]

DEFAULT_MAX_REDIRECTS = 10


class UseClientDefault:
    """Sentinel distinguishing "argument omitted → use the client's setting"
    from an explicit None (which disables the feature)."""

    def __repr__(self) -> str:
        return "USE_CLIENT_DEFAULT"


USE_CLIENT_DEFAULT = UseClientDefault()


class ResponseHandle:
    """An in-flight request: awaitable, and an async context manager.

    response = await client.get(url)          # then read/close it yourself

    async with client.get(url) as response:   # closed on block exit,
        async for chunk in response.iter_bytes():  # even on early break
            ...
    """

    __slots__ = ("_coro", "_response")

    def __init__(self, coro: typing.Coroutine[typing.Any, typing.Any, Response]) -> None:
        self._coro = coro
        self._response: Response | None = None

    def __await__(self) -> typing.Generator[typing.Any, None, Response]:
        return self._coro.__await__()

    async def __aenter__(self) -> Response:
        self._response = await self._coro
        return self._response

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        if self._response is not None:
            await self._response.close()
        return False


class BaseClient:
    _backend_type: typing.ClassVar[Backend | None] = None

    def __init__(
        self,
        *,
        auth: AuthTypes = None,
        params: QueryParamTypes = None,
        headers: HeaderTypes = None,
        cookies: CookieTypes = None,
        base_url: URL | str = "",
        timeout: TimeoutTypes = None,
        follow_redirects: bool = True,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        referer: bool = True,
        verify: VerifyTypes = True,
        cert: CertTypes | None = None,
        proxy: ProxyTypes = None,
        trust_env: bool = True,
        http1: bool = True,
        http2: bool = True,
        limits: Limits = DEFAULT_LIMITS,
        default_encoding: str | typing.Callable[[bytes], str | None] = "utf-8",
        connector: typing.Callable[..., typing.Awaitable[typing.Any]] | None = None,
    ) -> None:
        if self._backend_type is None:
            raise TypeError("BaseClient is not usable directly; use punkreq.asyncio.Client or punkreq.tonio.Client")
        self._backend = self._backend_type.create()
        self.base_url = base_url
        self.auth = coerce_auth(auth)
        self.params = QueryParams(params)
        self.headers = self._merge_default_headers(headers)
        self.cookies: Cookies | None = Cookies(cookies) if cookies is not None else None
        self.timeout = Timeout(timeout) if timeout is not None else Timeout(None)
        self.follow_redirects = follow_redirects
        self.max_redirects = max_redirects
        self.referer = referer
        self.default_encoding = default_encoding

        if connector is None:
            ssl_context = create_ssl_context(verify, cert)
            proxy_config = ProxyConfig.resolve(proxy, trust_env)
            if proxy_config is not None:
                connector = ProxyConnector(
                    backend=self._backend, ssl_context=ssl_context, http1=http1, http2=http2, config=proxy_config
                )
            else:
                connector = Connector(backend=self._backend, ssl_context=ssl_context, http1=http1, http2=http2)
        self._pool = ConnectionPool(connector, backend=self._backend, limits=limits)
        self._transport = PoolTransport(self._pool, backend=self._backend)

    def _merge_default_headers(self, headers: HeaderTypes) -> Headers:
        merged = Headers(headers)
        merged.setdefault("accept", "*/*")
        merged.setdefault("user-agent", f"python-punkreq/{__version__}")
        return merged

    @property
    def base_url(self) -> URL:
        return self._base_url

    @base_url.setter
    def base_url(self, url: URL | str) -> None:
        url = URL(url)
        if url.is_absolute_url:
            path, sep, query = url.raw_path.partition("?")
            if not path.endswith("/"):
                url = url.copy_with(raw_path=f"{path}/{sep}{query}")
        self._base_url = url

    def _merge_url(self, url: URL | str) -> URL:
        url = URL(url)
        if url.is_relative_url and self._base_url.is_absolute_url:
            base_path = self._base_url.raw_path.partition("?")[0]
            return self._base_url.copy_with(raw_path=base_path + url.raw_path.lstrip("/"))
        return url

    def build_request(
        self,
        method: str,
        url: URL | str,
        *,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        files: typing.Any = None,
        json: typing.Any = None,
        params: QueryParamTypes = None,
        headers: HeaderTypes = None,
        timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        extensions: typing.Mapping[str, typing.Any] | None = None,
    ) -> Request:
        """A `Request` with the client's configuration merged in. Pair with `send()`."""
        merged_url = self._merge_url(url)
        merged_params = self.params.merge(params) if (self.params or params is not None) else None
        merged_headers = self.headers.copy()
        merged_headers.update(headers)
        # reqwest behavior: advertise compression only when the user set neither
        # Accept-Encoding nor Range (compressed range responses are incoherent)
        if "accept-encoding" not in merged_headers and "range" not in merged_headers:
            merged_headers["accept-encoding"] = ACCEPT_ENCODING

        if isinstance(timeout, UseClientDefault):
            resolved_timeout = self.timeout
        else:
            resolved_timeout = Timeout(timeout) if timeout is not None else Timeout(None)
        merged_extensions = dict(extensions) if extensions is not None else {}
        merged_extensions.setdefault("timeout", resolved_timeout)

        request = Request(
            method,
            merged_url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=merged_params,
            headers=merged_headers,
            extensions=merged_extensions,
        )

        # URL userinfo becomes basic auth and is stripped from the wire (reqwest)
        if request.url.userinfo:
            if "authorization" not in request.headers:
                BasicAuth(request.url.username, request.url.password).apply(request)
            request.url = request.url.copy_with(userinfo="")
        return request

    async def send(
        self,
        request: Request,
        *,
        auth: AuthTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
    ) -> Response:
        """Send a request built with `build_request()`, returning once the
        response head arrives. Unlike the verb methods, `send()` does not merge
        client configuration into the request."""
        follow = self.follow_redirects if isinstance(follow_redirects, UseClientDefault) else follow_redirects

        if isinstance(auth, UseClientDefault):
            if self.auth is not None and "authorization" not in request.headers:
                self.auth.apply(request)
        else:
            resolved_auth = coerce_auth(auth)
            if resolved_auth is not None:
                resolved_auth.apply(request)

        start = self._backend.monotonic()
        # pin the total-timeout deadline before the first hop so it spans the
        # whole redirect chain (redirect requests inherit extensions)
        raw_timeout = request.extensions.get("timeout")
        total = Timeout(raw_timeout).total if raw_timeout is not None else None
        if total is not None:
            request.extensions.setdefault("deadline", start + total)

        history: list[Response] = []
        while True:
            if self.cookies is not None:
                self.cookies.set_cookie_header(request)

            response = await self._transport.send(request)
            response.default_encoding = self.default_encoding
            response.history = list(history)
            self._bind_elapsed(response, start)
            if self.cookies is not None:
                self.cookies.extract_cookies(response)

            next_request = None
            if follow and response.has_redirect_location:
                if len(history) >= self.max_redirects:
                    await response.close()
                    raise TooManyRedirects(
                        f"Exceeded maximum allowed redirects ({self.max_redirects})", request=request
                    )
                next_request = build_redirect_request(request, response, referer=self.referer)

            if next_request is None:
                return response

            # drain the redirect body so an h1 connection returns to the pool
            await response.read()
            history.append(response)
            request = next_request

    def _bind_elapsed(self, response: Response, start: float) -> None:
        backend = self._backend

        def set_elapsed() -> None:
            response.elapsed = datetime.timedelta(seconds=backend.monotonic() - start)

        add_callback = getattr(response.stream, "add_finish_callback", None)
        if add_callback is not None:
            add_callback(set_elapsed)

    # -- verb methods ----------------------------------------------------------

    def request(
        self,
        method: str,
        url: URL | str,
        *,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        files: typing.Any = None,
        json: typing.Any = None,
        params: QueryParamTypes = None,
        headers: HeaderTypes = None,
        auth: AuthTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
        timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        extensions: typing.Mapping[str, typing.Any] | None = None,
    ) -> ResponseHandle:
        return ResponseHandle(
            self._request(
                method,
                url,
                content=content,
                data=data,
                files=files,
                json=json,
                params=params,
                headers=headers,
                auth=auth,
                follow_redirects=follow_redirects,
                timeout=timeout,
                extensions=extensions,
            )
        )

    async def _request(
        self,
        method: str,
        url: URL | str,
        *,
        content: RequestContent | None = None,
        data: RequestData | None = None,
        files: typing.Any = None,
        json: typing.Any = None,
        params: QueryParamTypes = None,
        headers: HeaderTypes = None,
        auth: AuthTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        follow_redirects: bool | UseClientDefault = USE_CLIENT_DEFAULT,
        timeout: TimeoutTypes | UseClientDefault = USE_CLIENT_DEFAULT,
        extensions: typing.Mapping[str, typing.Any] | None = None,
    ) -> Response:
        request = self.build_request(
            method,
            url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=headers,
            timeout=timeout,
            extensions=extensions,
        )
        return await self.send(request, auth=auth, follow_redirects=follow_redirects)

    def get(
        self,
        url,
        *,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "GET",
            url,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    def options(
        self,
        url,
        *,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "OPTIONS",
            url,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    def head(
        self,
        url,
        *,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "HEAD",
            url,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    def delete(
        self,
        url,
        *,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "DELETE",
            url,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    def post(
        self,
        url,
        *,
        content=None,
        data=None,
        files=None,
        json=None,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "POST",
            url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    def put(
        self,
        url,
        *,
        content=None,
        data=None,
        files=None,
        json=None,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "PUT",
            url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    def patch(
        self,
        url,
        *,
        content=None,
        data=None,
        files=None,
        json=None,
        params=None,
        headers=None,
        auth=USE_CLIENT_DEFAULT,
        follow_redirects=USE_CLIENT_DEFAULT,
        timeout=USE_CLIENT_DEFAULT,
        extensions=None,
    ) -> ResponseHandle:
        return self.request(
            "PATCH",
            url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=headers,
            auth=auth,
            follow_redirects=follow_redirects,
            timeout=timeout,
            extensions=extensions,
        )

    async def close(self) -> None:
        await self._pool.close()

    async def __aenter__(self) -> BaseClient:
        return self

    async def __aexit__(self, exc_type: object, exc_value: object, exc_tb: object) -> bool:
        await self.close()
        return False


def _build_module_api(client_cls: type[BaseClient]) -> tuple:
    """The module-level convenience API (`punkreq.asyncio.get(...)` etc.), bound
    to a backend's Client class. Each call uses a throwaway client whose
    lifetime is tied to the response: the client is closed when the response
    body is fully read or the response is closed — so lazy-body semantics are
    identical to client-based usage."""

    def request(
        method,
        url,
        *,
        params=None,
        content=None,
        data=None,
        files=None,
        json=None,
        headers=None,
        cookies=None,
        auth=None,
        timeout=None,
        follow_redirects=True,
        verify=True,
        proxy=None,
        trust_env=True,
    ) -> ResponseHandle:
        async def send() -> Response:
            client = client_cls(
                cookies=cookies,
                auth=auth,
                verify=verify,
                timeout=timeout,
                follow_redirects=follow_redirects,
                proxy=proxy,
                trust_env=trust_env,
            )
            try:
                response = await client.request(
                    method, url, params=params, content=content, data=data, files=files, json=json, headers=headers
                )
            except BaseException:
                await client.close()
                raise
            # tie the throwaway client's lifetime to the response
            add_callback = getattr(response.stream, "add_finish_callback", None)
            if add_callback is not None:
                add_callback(client.close)
            else:  # pragma: no cover - transport responses always support callbacks
                await client.close()
            return response

        return ResponseHandle(send())

    def get(url, **kwargs) -> ResponseHandle:
        return request("GET", url, **kwargs)

    def options(url, **kwargs) -> ResponseHandle:
        return request("OPTIONS", url, **kwargs)

    def head(url, **kwargs) -> ResponseHandle:
        return request("HEAD", url, **kwargs)

    def post(url, **kwargs) -> ResponseHandle:
        return request("POST", url, **kwargs)

    def put(url, **kwargs) -> ResponseHandle:
        return request("PUT", url, **kwargs)

    def patch(url, **kwargs) -> ResponseHandle:
        return request("PATCH", url, **kwargs)

    def delete(url, **kwargs) -> ResponseHandle:
        return request("DELETE", url, **kwargs)

    return request, get, options, head, post, put, patch, delete
