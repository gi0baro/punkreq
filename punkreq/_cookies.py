from __future__ import annotations

import email.message
import typing
import urllib.request
from http.cookiejar import Cookie, CookieJar

from ._exceptions import CookieConflict


if typing.TYPE_CHECKING:
    from ._models import Request, Response


__all__ = ["Cookies"]

CookieTypes = typing.Union[
    "Cookies",
    CookieJar,
    typing.Mapping[str, str],
    typing.Sequence[typing.Tuple[str, str]],
    None,
]


class Cookies(typing.MutableMapping[str, str]):
    def __init__(self, cookies: CookieTypes = None) -> None:
        if cookies is None or isinstance(cookies, dict):
            self.jar = CookieJar()
            if isinstance(cookies, dict):
                for key, value in cookies.items():
                    self.set(key, value)
        elif isinstance(cookies, list):
            self.jar = CookieJar()
            for key, value in cookies:
                self.set(key, value)
        elif isinstance(cookies, Cookies):
            self.jar = CookieJar()
            with cookies.jar._cookies_lock:
                snapshot = list(cookies.jar)
            for cookie in snapshot:
                self.jar.set_cookie(cookie)
        elif isinstance(cookies, CookieJar):
            self.jar = cookies
        else:
            raise TypeError(f"Invalid type for 'cookies': {type(cookies)!r}")

    def extract_cookies(self, response: Response) -> None:
        """Store any Set-Cookie headers from `response` in the jar."""
        urllib_response = _CookieCompatResponse(response)
        urllib_request = _CookieCompatRequest(response.request)
        self.jar.extract_cookies(urllib_response, urllib_request)

    def set_cookie_header(self, request: Request) -> None:
        """Add a Cookie header for the jar's matching cookies. An existing
        Cookie header on the request is left untouched."""
        urllib_request = _CookieCompatRequest(request)
        self.jar.add_cookie_header(urllib_request)

    def set(self, name: str, value: str, domain: str = "", path: str = "/") -> None:
        kwargs = {
            "version": 0,
            "name": name,
            "value": value,
            "port": None,
            "port_specified": False,
            "domain": domain,
            "domain_specified": bool(domain),
            "domain_initial_dot": domain.startswith("."),
            "path": path,
            "path_specified": bool(path),
            "secure": False,
            "expires": None,
            "discard": True,
            "comment": None,
            "comment_url": None,
            "rest": {"HttpOnly": None},
            "rfc2109": False,
        }
        cookie = Cookie(**kwargs)
        self.jar.set_cookie(cookie)

    def get(  # type: ignore[override]
        self,
        name: str,
        default: str | None = None,
        domain: str | None = None,
        path: str | None = None,
    ) -> str | None:
        """The value for a cookie by name, with optional domain/path narrowing.
        Raises `CookieConflict` when multiple cookies match ambiguously."""
        value = None
        with self.jar._cookies_lock:
            for cookie in self.jar:
                if cookie.name != name:
                    continue
                if domain is not None and cookie.domain != domain:
                    continue
                if path is not None and cookie.path != path:
                    continue
                if value is not None:
                    raise CookieConflict(
                        f"Multiple cookies exist with name {name!r}; use domain=/path= to disambiguate"
                    )
                value = cookie.value
        return default if value is None else value

    def delete(self, name: str, domain: str | None = None, path: str | None = None) -> None:
        with self.jar._cookies_lock:
            if domain is not None and path is not None:
                return self.jar.clear(domain, path, name)
            remove = [
                cookie
                for cookie in self.jar
                if cookie.name == name
                and (domain is None or cookie.domain == domain)
                and (path is None or cookie.path == path)
            ]
            for cookie in remove:
                self.jar.clear(cookie.domain, cookie.path, cookie.name)

    def clear(self, domain: str | None = None, path: str | None = None) -> None:  # type: ignore[override]
        args = []
        if domain is not None:
            args.append(domain)
        if path is not None:
            assert domain is not None
            args.append(path)
        with self.jar._cookies_lock:
            self.jar.clear(*args)

    def update(self, cookies: CookieTypes = None) -> None:  # type: ignore[override]
        other = Cookies(cookies)
        with other.jar._cookies_lock:  # `other.jar` is the caller's live jar when a CookieJar was passed
            snapshot = list(other.jar)
        for cookie in snapshot:
            self.jar.set_cookie(cookie)

    def __setitem__(self, name: str, value: str) -> None:
        self.set(name, value)

    def __getitem__(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise KeyError(name)
        return value

    def __delitem__(self, name: str) -> None:
        self.delete(name)

    def __contains__(self, name: typing.Any) -> bool:
        with self.jar._cookies_lock:
            return any(cookie.name == name for cookie in self.jar)

    def __iter__(self) -> typing.Iterator[str]:
        with self.jar._cookies_lock:
            names = [cookie.name for cookie in self.jar]
        return iter(names)

    def __len__(self) -> int:
        with self.jar._cookies_lock:
            return len(self.jar)

    def __bool__(self) -> bool:
        with self.jar._cookies_lock:
            for _ in self.jar:
                return True
            return False

    def __repr__(self) -> str:
        with self.jar._cookies_lock:
            cookies = [f"<Cookie {cookie.name}={cookie.value} for {cookie.domain}{cookie.path}>" for cookie in self.jar]
        return f"<Cookies[{', '.join(cookies)}]>"


class _CookieCompatRequest(urllib.request.Request):
    """Presents a punkreq Request under `urllib.request.Request`'s interface,
    writing header mutations back through to the wrapped request."""

    def __init__(self, request: Request) -> None:
        super().__init__(
            url=str(request.url),
            headers=dict(request.headers),
            method=request.method,
        )
        self.request = request

    def add_unredirected_header(self, key: str, value: str) -> None:
        super().add_unredirected_header(key, value)
        self.request.headers[key] = value


class _CookieCompatResponse:
    """Presents a punkreq Response under the `urllib.response` interface
    `CookieJar.extract_cookies` expects."""

    def __init__(self, response: Response) -> None:
        self.response = response

    def info(self) -> email.message.Message:
        info = email.message.Message()
        for key, value in self.response.headers.multi_items():
            if key.lower() == "set-cookie":
                info[key] = value
        return info
