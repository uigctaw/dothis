import abc
import enum
import logging
import pprint
import re
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
    remaining_existing_resources_specs: list


@dataclass(frozen=True, kw_only=True, slots=True)
class ExistingResource:
    spec: dict
    remaining_existing_resources_specs: list


@dataclass(frozen=True, kw_only=True, slots=True)
class RequiredResource:
    creation_spec: dict
    builder: type


class Resource(abc.ABC):

    def __init__(self, do_api, *, time_=None, logger=None):
        self._do_api = do_api
        self._time = time_ or time
        if logger is None:
            logger = self._get_default_logger()
        self._logger = logger
        self._remaining_existing_resources: list

    def _get_default_logger(self):
        logger = logging.getLogger(type(self).__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def __enter__(self):
        existing_resources = self._get_existing_resources()
        self._logger.info(
                "Existing:\n%s\n",
                self._format_existing_resources(existing_resources),
        )
        self._remaining_existing_resources = existing_resources
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self._logger.error(
                "An error occurred, not deleting resources:\n%s\n",
                exc_value,
            )
            return
        existing_resources = self._remaining_existing_resources
        self._logger.info(
                "Deleting:\n%s\n",
                self._format_existing_resources(existing_resources),
        )
        self._delete_resources(existing_resources)

    def __call__(self, **required_spec):
        self._logger.info(
                "Required resource:\n%s",
                self._format_required_resource(required_spec),
        )
        categorized = self._categorize(
                required_resource_spec=required_spec,
                existing_resources_specs=self._remaining_existing_resources,
        )
        self._remaining_existing_resources = (
                categorized.remaining_existing_resources_specs)

        if isinstance(categorized, ExistingResource):
            self._logger.info("Already exists...\n")
            return categorized.spec

        self._logger.info("Creating...\n")
        created = self._create_resource(**categorized.creation_spec)
        self._logger.info("Created\n")
        return created

    def _categorize(self, *, required_resource_spec, existing_resources_specs):
        for i, existing in enumerate(existing_resources_specs):
            if self._are_specs_equal(
                    required_resource_spec=required_resource_spec,
                    existing_resource_spec=existing,
            ):
                return ExistingResource(
                    spec=existing,
                    remaining_existing_resources_specs=(
                        existing_resources_specs[:i]
                        + existing_resources_specs[i + 1:]
                    ),
                )
        return ResourceToBeCreated(
            creation_spec=required_resource_spec,
            remaining_existing_resources_specs=existing_resources_specs,
        )

    @abc.abstractmethod
    def _get_existing_resources(self):
        pass

    @abc.abstractmethod
    def _format_existing_resources(self, resources):
        pass

    def _format_required_resource(self, spec):
        return pprint.pformat(spec)

    @abc.abstractmethod
    def _create_resource(self, **spec):
        pass

    @abc.abstractmethod
    def _are_specs_equal(
            self,
            *,
            required_resource_spec,
            existing_resource_spec,
    ) -> bool:
        pass

    @abc.abstractmethod
    def _delete_resources(self, resources, /):
        pass


def _delete_by_resources_by_id(self, resources, /):
    for spec in resources:
        endpoint = f"{self._ENDPOINT}/{spec['id']}"
        response = self._do_api.delete(endpoint=endpoint)
        assert response.code == HTTPCode.NO_CONTENT


class Droplets(Resource):
    _ENDPOINT = "droplets"

    def __init__(self, *args, tag, **kwargs):
        super().__init__(*args, **kwargs)
        self._tag = tag

    def _get_existing_resources(self):
        ret = self._do_api.get(
                endpoint=self._ENDPOINT, params=dict(tag_name=self._tag),
        ).data["droplets"]
        return ret

    def _create_resource(self, *, name, size, image, region, **other):
        tags = [*other.pop("tags", []), self._tag]
        response = self._do_api.post(
                endpoint=self._ENDPOINT,
                name=name,
                size=size,
                image=image,
                region=region,
                tags=tags,
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
            self._logger.info("Still creating...")
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

    def _are_specs_equal(
            self, *, required_resource_spec, existing_resource_spec):
        required = required_resource_spec.copy()
        required["image"] = dict(slug=required.pop("image"))
        required["size_slug"] = required.pop("size")
        required["region"] = dict(slug=required.pop("region"))
        required.pop("ssh_keys", None)
        return _is_dict_subset(sub=required, super_=existing_resource_spec)

    _delete_resources = _delete_by_resources_by_id

    def _format_existing_resources(self, specs):
        return pprint.pformat([
            {
                "name": spec["name"],
                "region": spec["region"]["slug"],
                "image": spec["image"]["slug"],
                "size": spec["size_slug"],
                "vpc_uuid": spec["vpc_uuid"],
            }
            for spec in specs
        ])


class Vpcs(Resource):
    _ENDPOINT = "vpcs"

    def _get_existing_resources(self):
        return self._do_api.get(endpoint=self._ENDPOINT).data["vpcs"]

    def _create_resource(self, *, name, region, **other):
        response = self._do_api.post(
                endpoint=self._ENDPOINT,
                name=name,
                region=region,
                **other,
        )
        assert response.code == HTTPCode.CREATED, response.code
        return response.data

    def _are_specs_equal(
            self, *, required_resource_spec, existing_resource_spec):
        return _is_dict_subset(
            sub=required_resource_spec, super_=existing_resource_spec)

    def _delete_resources(self, resources, /):
        non_default_vpcs = (
            spec for spec in resources if not spec["default"]
        )
        _delete_by_resources_by_id(self, non_default_vpcs)

    def _format_existing_resources(self, specs):
        return pprint.pformat([
            {
                "name": spec["name"],
                "region": spec["region"],
                "id": spec["id"],
                "ip_range": spec["ip_range"],
                "default": spec["default"],
            }
            for spec in specs
        ])


def _is_dict_subset(*, sub, super_):
    return all(
        _is_dict_subset(sub=v, super_=super_v)
        if isinstance(v, dict) and isinstance(super_v := super_.get(k), dict)
        else v == super_.get(k)
        for k, v in sub.items()
    )


class SshKeys(Resource):
    _ENDPOINT = "account/keys"

    def __init__(self, *args, name_pattern, **kwargs):
        super().__init__(*args, **kwargs)
        self._name_pattern = re.compile(name_pattern)

    def _get_existing_resources(self):
        keys = self._do_api.get(endpoint=self._ENDPOINT).data["ssh_keys"]
        return [
            key
            for key in keys
            if self._name_pattern.search(key["name"])
        ]

    def _create_resource(self, *, name, public_key, **other):
        response = self._do_api.post(
                endpoint=self._ENDPOINT,
                name=name,
                public_key=public_key,
                **other,
        )
        assert response.code == HTTPCode.CREATED, response.code
        return response.data

    _delete_resources = _delete_by_resources_by_id

    def _are_specs_equal(
            self, *, required_resource_spec, existing_resource_spec):
        return _is_dict_subset(
            sub=required_resource_spec, super_=existing_resource_spec)

    def _format_existing_resources(self, specs):
        return pprint.pformat([
            {
                "name": spec["name"],
                "id": spec["id"],
                "fingerprint": spec["fingerprint"],
            }
            for spec in specs
        ])
