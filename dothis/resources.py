import abc
import enum
import time
from dataclasses import dataclass


class HTTPCode(enum.IntEnum):
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204


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
    builder: type


class Resource(abc.ABC):

    def __init__(self, do_api, *, time_=None):
        self._do_api = do_api
        self._time = time_ or time
        self._remaining_existing_resources: list

    def __enter__(self):
        self._remaining_existing_resources = self._get_existing_resources()
        return self

    def __exit__(self, *_):
        self._delete_resources(self._remaining_existing_resources)

    def __call__(self, **required_spec):
        categorized = self._categorize(
                required_resource_spec=required_spec,
                existing_resource_specs=self._remaining_existing_resources,
        )
        self._remaining_existing_resources = (
                categorized.remaining_existing_resource_specs)

        if isinstance(categorized, ExistingResource):
            return categorized.spec

        return self._create_resource(**categorized.creation_spec)

    @abc.abstractmethod
    def _get_existing_resources(self):
        pass

    @abc.abstractmethod
    def _create_resource(self, **creation_spec):
        pass

    @abc.abstractmethod
    def _categorize(self, *, required_resource_spec, existing_resource_specs):
        pass

    @abc.abstractmethod
    def _delete_resources(self, resources, /):
        pass


class Droplets(Resource):
    _ENDPOINT = "droplets"

    def __init__(self, *args, tag_name, **kwargs):
        super().__init__(*args, **kwargs)
        self._tag_name = tag_name

    def _get_existing_resources(self):
        ret = self._do_api.get(
            endpoint=self._ENDPOINT, params=dict(tag_name=self._tag_name),
        ).data["droplets"]
        return ret

    def _create_resource(self, *, name, size, image, **other):
        response = self._do_api.post(
                endpoint=self._ENDPOINT,
                name=name,
                size=size,
                image=image,
                **other,
        )
        assert response.code == HTTPCode.ACCEPTED
        droplet_id = response.data["droplet"]["id"]
        action_id = next(
                action
                for action in response.data["links"]["actions"]
                if action["rel"] == "create"
        )["id"]

        sleep_s = 1.0
        backoff_factor = 1.5
        max_number_of_tries = 10
        for _ in range(max_number_of_tries):
            if response := self._get_created_droplet(
                endpoint=self._ENDPOINT,
                droplet_id=droplet_id,
                action_id=action_id,
                do_api=self._do_api,
            ):
                return response
            self._time.sleep(sleep_s)
            sleep_s *= backoff_factor
        return 1/0

    @staticmethod
    def _get_created_droplet(*, endpoint, droplet_id, action_id, do_api):
        response = do_api.get(
            endpoint=f"{endpoint}/{droplet_id}/actions/{action_id}",
        )
        if (
            response.code == HTTPCode.OK
            and response.data["action"]["status"] == "completed"
        ):
            response = do_api.get(endpoint=f"{endpoint}/{droplet_id}")
            if response.code == HTTPCode.OK:
                return response.data["droplet"]
        return None

    def _categorize(self, *, required_resource_spec, existing_resource_specs):
        return _spec_subset_exists(
                required_resource_spec, existing_specs=existing_resource_specs)

    def _delete_resources(self, specs, /):
        for spec in specs:
            endpoint = f"{self._ENDPOINT}/{spec['id']}"
            response = self._do_api.delete(endpoint=endpoint)
            assert response.code == HTTPCode.NO_CONTENT


class VPCs(Resource):
    _ENDPOINT = "vpcs"

    def _get_existing_resources(self):
        return self._do_api.get(endpoint=self._ENDPOINT).data["vpcs"]

    def _categorize(self, *, required_resource_spec, existing_resource_specs):
        return _spec_subset_exists(
                required_resource_spec, existing_specs=existing_resource_specs)

    def _create_resource(self, *, name, region, **other):
        response = self._do_api.post(
                endpoint=self._ENDPOINT,
                name=name,
                region=region,
                **other,
        )
        assert response.code == HTTPCode.CREATED, response.code
        return response.data

    def _delete_resources(self, resources, /):
        pass


def _spec_subset_exists(required_spec, *, existing_specs):
    for i, existing in enumerate(existing_specs):
        if _is_dict_subset(sub=required_spec, super_=existing):
            return ExistingResource(
                spec=existing,
                remaining_existing_resource_specs=(
                    existing_specs[:i] + existing_specs[i + 1:]
                ),
            )

    return ResourceToBeCreated(
        creation_spec=required_spec,
        remaining_existing_resource_specs=existing_specs,
    )


def _is_dict_subset(*, sub, super_):
    return all(v == super_.get(k) for k, v in sub.items())
