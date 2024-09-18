import contextlib
import json
import pprint
import typing
import urllib.parse
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.error import HTTPError


class _SimpleHTTPResponse(typing.Protocol):
    def read(self) -> bytes:
        pass

    @property
    def code(self) -> int:
        pass


class _OpenURL(typing.Protocol):
    @contextlib.contextmanager
    def __call__(
        self, request: urllib.request.Request, /,
    ) -> Iterator[_SimpleHTTPResponse]:
        pass


@dataclass(frozen=True, kw_only=True, slots=True)
class _Response:
    code: int
    data: dict | None


class DigitalOcean:
    def __init__(
            self,
            token,
            open_url: _OpenURL = urllib.request.urlopen,
    ):
        self._open_url = open_url
        self._token = token

    def post(self, *, endpoint, **data):
        return self._make_api_call(endpoint=endpoint, data=data, method="POST")

    def get(self, *, endpoint, params=None):
        return self._make_api_call(
            endpoint=endpoint, method="GET", params=params,
        )

    def delete(self, *, endpoint):
        return self._make_api_call(endpoint=endpoint, method="DELETE")

    def _make_api_call(self, *, endpoint, method, data=None, params=None):
        params = "" if params is None else "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(  # noqa: S310
            url=f"https://api.digitalocean.com/v2/{endpoint}{params}",
            method=method,
            data=json.dumps(data).encode("ascii") if data else data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._token}",
            },
        )
        try:
            with self._open_url(request) as response:
                raw_data = response.read()
                return _Response(
                    data=json.loads(raw_data) if raw_data else None,
                    code=response.code,
                )
        except HTTPError as exc:
            request_data = json.loads(request.data)  # type: ignore[arg-type]
            msg = "\n".join((
                f"URL: {request.full_url}",
                f"Method: {request.method}",
                f"Request data: {pprint.pformat(request_data)}",
                f"Response code: {exc.code!s}",
                f"Response data: {pprint.pformat(json.loads(exc.read()))}",
            ))
            exc.msg = msg  # type: ignore[attr-defined]
            raise exc  # noqa: TRY201
