from collections import defaultdict
from collections.abc import Mapping, Iterable
from dataclasses import dataclass
from functools import reduce
from typing import Callable, Iterator
import contextlib
import json
import operator
import time
import typing
import urllib.parse
import urllib.request


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
    data: dict


class DigitalOcean:

    def __init__(self, open_url, token):
        self._open_url = open_url
        self._token = token

    def post(self, *, endpoint, data):
        return self._make_api_call(endpoint=endpoint, data=data, method='POST')

    def get(self, *, endpoint, params=None):
        return self._make_api_call(
                endpoint=endpoint, 
                method='GET',
                params=params,
        )

    def delete(self, *, endpoint):
        return self._make_api_call(endpoint=endpoint, method='DELETE')

    def _make_api_call(self, *, endpoint, method, data=None, params=None):
        params = '' if params is None else '?' + urllib.parse.urlencode(params)
        request = urllib.request.Request(
                url=f'https://api.digitalocean.com/v2/{endpoint}{params}',
                method=method,
                data=json.dumps(data).encode('ascii') if data else data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {self._token}',
                },
        )
        with self._open_url(request) as response:
            return _Response(
                data=json.loads(response.read()),
                code=response.code,
            )



@typing.runtime_checkable
class Materializable(typing.Protocol):

    def materialize(self):
        pass


def materialize(item):
    if isinstance(item, Materializable):
        return item.materialize()
    else:
        return item


class CreationSpec:

    def __init__(self, spec):
        self._spec = spec

    def materialize(self):
        return self._materialize(self._spec)

    @classmethod
    def _materialize(cls, spec):
        if isinstance(spec, dict):
            return {k: cls._materialize(v) for k, v in spec.items()}
        elif isinstance(spec, (list, tuple)):
            return type(spec)(map(cls._materialize, spec))
        else:
            return materialize(spec)


class Future:

    def __init__(self, infra_maker, *, builder):
        self._actual_spec = None
        self._infra_maker = infra_maker
        self._builder = builder
        self._references = []

    def _is_populated(self):
        return self._actual_spec is not None

    def __getitem__(self, item):
        if self._is_populated():
            return self._actual_spec[item]
        reference = FutureReference(future=self, item=item)
        self._references.append(reference)
        return reference

    def populate(self, data):
        assert not self._is_populated() or self._actual_spec == data
        self._actual_spec = data


@dataclass(frozen=True, kw_only=True, slots=True)
class FutureReference:
    future: Future
    item: str

    def materialize(self):
        return self.future[self.item]

    def __add__(self, other):
        return FutureReferenceSum(self, other)


class FutureReferenceSum:

    def __init__(self, *elements):
        self._elements = elements
    
    def materialize(self):
        return reduce(operator.add, map(materialize, self._elements))

    def __add__(self, other):
        return FutureReferenceSum(self, other)


@dataclass(frozen=True, kw_only=True, slots=True)
class ResourceToBeCreated:
    creation_spec: dict
    remaining_existing_resource_specs: list


@dataclass(frozen=True, kw_only=True, slots=True)
class ExistingResource:
    spec: dict
    remaining_existing_resource_specs: list


@dataclass(frozen=True, kw_only=True, slots=True)
class RequiredResource:
    creation_spec: dict
    future: Future
    builder: type


class InfraMaker:

    def __init__(self):
        self._required_resources = []
        self._builders = []
        self._futures = defaultdict(list)

    def __call__(self, builder, /):
        self._builders.append(builder)
        return self._get_future_creator(builder=builder)

    def _get_future_creator(self, builder):

        def create_future(**creation_spec):
            future = Future(self, builder=builder)
            self._futures[builder].append(future)
            self._register_required_resource(RequiredResource(
                    creation_spec=CreationSpec(creation_spec),
                    builder=builder,
                    future=future,
            ))
            return future

        return create_future

    def _register_required_resource(self, required_resource):
        self._required_resources.append(required_resource)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            remaining_existing_resources = self._get_existing_resources(
                    self._builders,
            )
            for required_resource in self._required_resources:
                builder = required_resource.builder
                creation_spec = required_resource.creation_spec
                existing = remaining_existing_resources[builder]
                categorized = builder.categorize(
                        required_resource_spec=creation_spec,
                        existing_resource_specs=existing, 
                )
                if isinstance(categorized, ResourceToBeCreated):
                    spec = builder.create_resource(
                            categorized.creation_spec.materialize(),
                    )
                else:
                    spec = categorized.spec
                required_resource.future.populate(spec)

                remaining_existing_resources[builder] = (
                        categorized.remaining_existing_resource_specs)

            for (
                   builder,
                   resources_to_delete,
            ) in remaining_existing_resources.items():
                builder.delete_resources(resources_to_delete)

    def _get_existing_resources(self, builders):
        existing = {
                builder: builder.get_existing_resources()
                for builder in builders
        }
        return existing

    def collect(self, resource_spec):
        self._resource_specs.append(resource_spec)
        return resource_spec.future
