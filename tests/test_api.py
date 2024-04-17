from dataclasses import dataclass
import contextlib
import itertools
import json
import random
import re
import urllib
import uuid

from dothis.api import DigitalOcean, InfraMaker
from dothis.resources import Droplets, VPCs


class DoNotKnowHowToProcess(Exception):
    pass


def _iter_increasing_random_ints():
    return itertools.accumulate(
            (random.randrange(x) for x in itertools.cycle([1_000])),
            initial=random.randrange(1_000_000),
    )


@dataclass(frozen=True, kw_only=True, slots=True)
class DropletUrl:

    droplet_id: int | None
    action_id: int | None

    @classmethod
    def from_string(cls, url_str):
        url = urllib.parse.urlparse(url_str)
        path_parts = url.path.split('/')
        assert path_parts[:3] == ['', 'v2', 'droplets']

        droplet_id = None
        action_id = None

        subpath_parts = path_parts[3:] 
        if subpath_parts:
            droplet_id = int(subpath_parts[0])
            subpath_parts = subpath_parts[1:]

        if subpath_parts:
            actions, action_id = subpath_parts
            assert actions == 'actions'
            action_id = int(action_id)

        return cls(droplet_id=droplet_id, action_id=action_id)


class DropletsProcessor:

    def __init__(self):
        self._action_ids = _iter_increasing_random_ints()
        self._droplet_ids = _iter_increasing_random_ints()
        self._droplets = {}

    def get_number_of_droplets(self):
        return len(self._droplets)

    def process(self, request):
        url = DropletUrl.from_string(request.full_url)
        if request.method == 'POST':
            return self._process_post(request, url=url)
        if request.method == 'GET':
            return self._process_get(request, url=url)
        if request.method == 'DELETE':
            return self._process_delete(request, url=url)
        raise DoNotKnowHowToProcess(request)

    def _process_post(self, request, *, url):
        action_id = next(self._action_ids)
        droplet_id = next(self._droplet_ids)
        request_data = json.loads(request.data)
        self._droplets[droplet_id] = dict(
                name=request_data['name'],
                action_id=action_id,
                vpc_uuid=request_data.get('vpc_uuid', uuid.uuid4().int),
                id=droplet_id,
        )
        return FakeResponse(
            code=202,
            data=dict(
                actions=dict(id=action_id),
                droplet=dict(id=droplet_id),
            ),
        )

    def _process_get(self, request, *, url):
        if url.droplet_id:
            droplet = self._droplets[url.droplet_id]
            if url.action_id:
                assert url.action_id == droplet['action_id']
                return FakeResponse(
                    code=200,
                    data=dict(name=droplet['name'], status='completed'),
                )
            else:
                return FakeResponse(
                    code=200,
                    data=dict(droplet=dict(
                        name=droplet['name'],
                        vpc_uuid=droplet['vpc_uuid'],
                    )),
                )
        return FakeResponse(
                code=200,
                data=dict(
                    droplets=[
                        dict(name=droplet['name'], id=droplet['id'])
                        for droplet in self._droplets.values()
                    ]
                ),
        )

    def _process_delete(self, request, *, url):
        del self._droplets[url.droplet_id]
        return FakeResponse(code=204)

    def _process_action(self, request, *, droplet_id):
        return FakeResponse(
            code=200,
            data=dict(
                action=dict(id=action_id, status='completed'),
            ),
        )


class VPCsProcessor:

    def __init__(self):
        self._vpcs = []
        self._vpc_ids = _iter_increasing_random_ints()

    def process(self, request):
        if request.method == 'POST':
            details = json.loads(request.data)
            self._vpcs.append(details)

            return FakeResponse(
                code=201,
                data=details | dict(id=next(self._vpc_ids)),
            )
        raise DoNotKnowHowToProcess(request)


class FakeDigitalOcean:

    def __init__(self):
        self._droplets_processor = DropletsProcessor()
        self._vpcs_processor = VPCsProcessor()

    def process_request(self, request):
        endpoint, = re.match(
                pattern=r'https://api.digitalocean.com/v2/(\w+)',
                string=request.full_url,
        ).groups()
        return dict(
                droplets=self._droplets_processor,
                vpcs=self._vpcs_processor,
        )[endpoint].process(request)

    def get_number_of_droplets(self):
        return self._droplets_processor.get_number_of_droplets()


class FakeResponse:

    def __init__(self, *, data=None, code):
        self._data = data
        self._code = code

    def read(self) -> bytes:
        return json.dumps(self._data).encode('utf8')

    @property
    def code(self):
        return self._code


def get_fake_open_url():
    do = FakeDigitalOcean()

    @contextlib.contextmanager
    def fake_open_url(request):
        yield do.process_request(request)

    return fake_open_url


class FakeURLOpener:

    def __init__(self):
        self.do = FakeDigitalOcean()

    @contextlib.contextmanager
    def __call__(self, request):
        yield self.do.process_request(request)


def test_noop():
    with InfraMaker() as infra_maker:
        pass


def test_create_one_droplet():
    do_api = DigitalOcean(token=None, open_url=get_fake_open_url())
    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name='my_droplet', do_api=do_api))

        droplet = droplets(name='test_droplet')
    assert droplet['name'] == 'test_droplet'


def test_create_droplet_in_vpc():
    do_api = DigitalOcean(token=None, open_url=get_fake_open_url())
    with InfraMaker() as infra_maker:
        vpcs = infra_maker(VPCs(do_api=do_api))
        droplets = infra_maker(Droplets(tag_name='my_droplet', do_api=do_api))

        vpc = vpcs(name='my_vpc')
        droplet = droplets(name='test_droplet', vpc_uuid=vpc['id'])
    assert vpc['id'] == droplet['vpc_uuid']


def test_complex_dependencies_are_respected():
    do_api = DigitalOcean(token=None, open_url=get_fake_open_url())
    with InfraMaker() as infra_maker:

        vpcs = infra_maker(VPCs(do_api=do_api))
        droplets = infra_maker(Droplets(tag_name='my_droplet', do_api=do_api))

        drop1 = droplets(name='d1')
        vpc1 = vpcs(name=drop1['name'] + '_v1')
        drop2 = droplets(name=vpc1['name'] + '_d2')
        vpc2 = vpcs(name=drop2['name'] + '_v2_' + vpc1['name'])

    assert drop1['name'] == 'd1'
    assert vpc1['name'] == 'd1_v1'
    assert drop2['name'] == 'd1_v1_d2'
    assert vpc2['name'] == 'd1_v1_d2_v2_d1_v1'


def test_creating_the_same_resource_twice():
    do_api = DigitalOcean(token=None, open_url=get_fake_open_url())

    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name='my_droplet', do_api=do_api))

        droplets(name='d1')
        drop = droplets(name='d1')

    assert drop['name'] == 'd1'


def test_deleting_a_resource():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name='my_droplet', do_api=do_api))

        droplets(name='d1')

    assert fake_url_opener.do.get_number_of_droplets() == 1

    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name='my_droplet', do_api=do_api))

    assert fake_url_opener.do.get_number_of_droplets() == 0
