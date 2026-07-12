from httpunk import Backend as _Backend

from ._client import BaseClient as _BaseClient, _build_module_api


__all__ = ["Client", "delete", "get", "head", "options", "patch", "post", "put", "request"]


class Client(_BaseClient):
    _backend_type = _Backend.tonio


request, get, options, head, post, put, patch, delete = _build_module_api(Client)
