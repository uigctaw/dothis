import enum
from typing import Protocol

from .api import ExistingResource, ResourcePoller, ResourceToBeCreated


class HTTPCode(enum.IntEnum):
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204


class Resource(Protocol):
    def get_existing_resources(self):
        pass

    def create_resource(self, resource, /):
        pass

    def categorize(self, *, required_resource_spec, existing_resource_specs):
        pass

    def delete_resources(self, resources, /):
        pass


class Droplets:
    _ENDPOINT = "droplets"

    def __init__(self, do_api, *, tag_name):
        self._tag_name = tag_name
        self._do_api = do_api

    def get_existing_resources(self):
        ret = self._do_api.get(
            endpoint=self._ENDPOINT, params=dict(tag_name=self._tag_name),
        ).data["droplets"]
        return ret

    def create_resource(self, *, name, size, image, **other):
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

        poller = ResourcePoller(
            self._get_created_droplet,
            endpoint=self._ENDPOINT,
            droplet_id=droplet_id,
            action_id=action_id,
            do_api=self._do_api,
        )
        if response := poller.poll():
            return response
        return poller

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

    def categorize(self, *, required_resource_spec, existing_resource_specs):
        return _spec_subset_exists(
                required_resource_spec, existing_specs=existing_resource_specs)

    def delete_resources(self, specs, /):
        for spec in specs:
            endpoint = f"{self._ENDPOINT}/{spec['id']}"
            response = self._do_api.delete(endpoint=endpoint)
            assert response.code == HTTPCode.NO_CONTENT


class VPCs:
    _ENDPOINT = "vpcs"

    def __init__(self, do_api):
        self._do_api = do_api

    def get_existing_resources(self):
        return self._do_api.get(endpoint=self._ENDPOINT).data["vpcs"]

    def categorize(self, *, required_resource_spec, existing_resource_specs):
        return _spec_subset_exists(
                required_resource_spec, existing_specs=existing_resource_specs)

    def create_resource(self, *, name, region, **other):
        response = self._do_api.post(
                endpoint=self._ENDPOINT,
                name=name,
                region=region,
                **other,
        )
        assert response.code == HTTPCode.CREATED, response.code
        return response.data

    def delete_resources(self, resources, /):
        pass


def _spec_subset_exists(required_spec, *, existing_specs):
    required = required_spec.materialize()
    for i, existing in enumerate(existing_specs):
        if _is_dict_subset(sub=required, super_=existing):
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
