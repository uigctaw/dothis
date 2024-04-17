from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Protocol
import json

from .api import ExistingResource, InfraMaker, ResourceToBeCreated


class Resource(Protocol):

    def get_existing_resources(self):
        pass

    def create_resource(self, resource, /):
        pass

    def categorize(
            self,
            *,
            required_resource_spec,
            existing_resource_specs,
    ):
        pass

    def delete_resources(self, resources, /):
        pass


class Poller:

    def __init__(self, fn, **kwargs):
        self._fn = fn
        self._kwargs = kwargs

    def poll(self):
        return self._fn(**self._kwargs)


class Droplets:

    _ENDPOINT = 'droplets'

    def __init__(self, *, tag_name, do_api):
        self._tag_name = tag_name
        self._do_api = do_api

    def get_existing_resources(self):
        return self._do_api.get(
                endpoint=self._ENDPOINT,
                params=dict(tag_name=self._tag_name),
        ).data['droplets']

    def create_resource(self, spec, /):
        endpoint = self._ENDPOINT
        response = self._do_api.post(
            endpoint=endpoint,
            data=spec,
        )
        assert response.code == 202
        droplet_id = response.data['droplet']['id']
        action_id = response.data['actions']['id']

        poller = Poller(
                self._get_created_droplet,
                endpoint=endpoint,
                droplet_id=droplet_id,
                action_id=action_id,
                do_api=self._do_api,
        )
        if response := poller.poll():
            return response
        return poller


    @staticmethod
    def _get_created_droplet(
            *,
            endpoint,
            droplet_id,
            action_id,
            do_api,
    ):
        response = do_api.get(
            endpoint=f'{endpoint}/{droplet_id}/actions/{action_id}'
        )
        if response.code == 200:
            if response.data['status'] == 'completed':
                response = do_api.get(
                    endpoint=f'{endpoint}/{droplet_id}'
                )
                if response.code == 200:
                    return response.data['droplet']

    def categorize(
            self,
            *,
            required_resource_spec,
            existing_resource_specs,
    ):
        found_index = None
        for i, existing_resource_spec in enumerate(existing_resource_specs):
            if (
                    existing_resource_spec['name']
                    != required_resource_spec['name']
            ):
                break
            else:
                found_index = i
            if found_index is not None:
                break

        if found_index is None:
            return ResourceToBeCreated(
                creation_spec=required_resource_spec,
                remaining_existing_resource_specs=existing_resource_specs,
            )
        return ExistingResource(
            spec=existing_resources_spec[found_index],
            remaining_existing_resource_specs=(
                existing_resource_specs[:found_index]
                + existing_resource_specs[found_index + 1:]
            ),
        )

    def delete_resources(self, specs, /):
        for spec in specs:
            endpoint = f"{self._ENDPOINT}/{spec['id']}"
            response = self._do_api.delete(endpoint=endpoint)
            assert response.code == 204


class VPCs:

    _ENDPOINT = 'vpcs'

    def __init__(self, *, do_api):
        self._do_api = do_api

    def get_existing_resources(self):
        return []

    def categorize(self, *, required_resource_spec, existing_resource_specs):
        assert not existing_resource_specs
        return ResourceToBeCreated(
                creation_spec=required_resource_spec,
                remaining_existing_resource_specs=existing_resource_specs,
        )

    def create_resource(self, spec, /):
        response = self._do_api.post(
            endpoint=self._ENDPOINT,
            data=spec,
        )
        assert response.code == 201
        return response.data

    def delete_resources(self, specs, /):
        1/0
