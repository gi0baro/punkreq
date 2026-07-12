from __future__ import annotations

import base64
import typing

from ._models import Request


__all__ = ["Auth", "BasicAuth", "BearerAuth"]

AuthTypes = typing.Union["Auth", typing.Tuple[str, str], None]


class Auth:
    """Base class: override `apply` to mutate the outgoing request."""

    def apply(self, request: Request) -> None:
        raise NotImplementedError()


class BasicAuth(Auth):
    def __init__(self, username: str, password: str = "") -> None:
        token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
        self._header = f"Basic {token}"

    def apply(self, request: Request) -> None:
        request.headers["authorization"] = self._header


class BearerAuth(Auth):
    def __init__(self, token: str) -> None:
        self._header = f"Bearer {token}"

    def apply(self, request: Request) -> None:
        request.headers["authorization"] = self._header


def coerce_auth(auth: AuthTypes) -> Auth | None:
    if auth is None or isinstance(auth, Auth):
        return auth
    if isinstance(auth, tuple) and len(auth) == 2:
        return BasicAuth(auth[0], auth[1])
    raise TypeError(f"Invalid 'auth' argument: {auth!r}")
