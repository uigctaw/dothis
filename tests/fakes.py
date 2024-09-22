import abc
import contextlib
import io
import itertools
import json
import random
import re
import urllib
import uuid
from collections import defaultdict
from dataclasses import dataclass
from urllib.error import HTTPError

ILLEGAL_SIZE = "__THIS_IS_AN_ILLEGAL_SIZE__"
GARGANTUAN = "__TOO_BIG_TO_MAKE_QUICKLY__"


class DoNotKnowHowToProcessRequestError(Exception):
    pass


def _iter_increasing_random_ints():
    return itertools.accumulate(
        (
            random.randrange(x)  # noqa: S311
            for x in itertools.cycle([1_000])
        ),
        initial=random.randrange(1_000_000),  # noqa: S311
    )


@dataclass(frozen=True, kw_only=True, slots=True)
class DropletUrl:
    droplet_id: int | None
    action_id: int | None

    @classmethod
    def from_string(cls, url_str):
        url = urllib.parse.urlparse(url_str)
        path_parts = url.path.split("/")
        assert path_parts[:3] == ["", "v2", "droplets"]

        droplet_id = None
        action_id = None

        subpath_parts = path_parts[3:]
        if subpath_parts:
            droplet_id = int(subpath_parts[0])
            subpath_parts = subpath_parts[1:]

        if subpath_parts:
            actions, action_id = subpath_parts
            assert actions == "actions"
            action_id = int(action_id)

        return cls(droplet_id=droplet_id, action_id=action_id)


class Processor(abc.ABC):
    @abc.abstractmethod
    def process(self, request):
        pass


class DropletsProcessor(Processor):
    def __init__(self):
        self._action_ids = _iter_increasing_random_ints()
        self._droplet_ids = _iter_increasing_random_ints()
        self._droplets = {}
        self._creation_progress = defaultdict(int)

    def get_number_of_droplets(self):
        return len(self._droplets)

    def process(self, request):
        url = DropletUrl.from_string(request.full_url)
        if request.method == "POST":
            return self._process_post(request)
        if request.method == "GET":
            return self._process_get(url=url)
        if request.method == "DELETE":
            return self._process_delete(url=url)
        raise DoNotKnowHowToProcessRequestError(request)

    def _process_post(self, request):
        action_id = next(self._action_ids)
        droplet_id = next(self._droplet_ids)
        request_data = json.loads(request.data)
        if request_data["size"] == ILLEGAL_SIZE:
            return FakeResponse(code=422)
        self._droplets[droplet_id] = dict(
            name=request_data["name"],
            region=dict(slug=request_data["region"]),
            action_id=action_id,
            vpc_uuid=request_data.get("vpc_uuid", uuid.uuid4().int),
            id=droplet_id,
            size_slug=request_data["size"],
            image=dict(slug=request_data["image"]),
        )
        return FakeResponse(
            code=202,
            data=dict(
                links=dict(actions=[dict(id=action_id, rel="create")]),
                droplet=dict(id=droplet_id),
            ),
        )

    def _process_get(self, url):
        if url.droplet_id:
            droplet = self._droplets[url.droplet_id]
            if url.action_id:
                assert url.action_id == droplet["action_id"]
                if droplet["size_slug"] == GARGANTUAN:
                    self._creation_progress[url.droplet_id] += 1
                    if self._creation_progress[url.droplet_id] <= 1:
                        return FakeResponse(
                            code=200,
                            data=dict(
                                name=droplet["name"],
                                action=dict(status="in-progress"),
                            ),
                        )
                return FakeResponse(
                    code=200,
                    data=dict(
                        name=droplet["name"],
                        action=dict(status="completed"),
                    ),
                )
            else:
                return FakeResponse(
                    code=200,
                    data=dict(
                        droplet=self._without_action(droplet),
                    ),
                )
        return FakeResponse(
            code=200,
            data=dict(
                droplets=[
                    self._without_action(droplet)
                    for droplet in self._droplets.values()
                ],
            ),
        )

    def _without_action(self, droplet):
        without_action = droplet.copy()
        del without_action["action_id"]
        return without_action

    def _process_delete(self, url):
        del self._droplets[url.droplet_id]
        return FakeResponse(code=204)


class VpcsProcessor(Processor):
    def __init__(self):
        self._vpc_ids = _iter_increasing_random_ints()
        self._vpcs = {
            (id_ := next(self._vpc_ids)):
                dict(
                    id=id_,
                    name="__DEFAULT_Vpc__",
                    default=True,
                    region="__EVERYWHERE__",
                    ip_range="0.0.0.0/0",
                ),
        }

    def process(self, request):
        if request.method == "POST":
            details = json.loads(request.data)
            details["id"] = next(self._vpc_ids)
            details["ip_range"] = details.get(
                "ip_range", "1337.1337.1337.1337/32")
            details["default"] = False
            self._vpcs[details["id"]] = details

            return FakeResponse(code=201, data=details)
        if request.method == "GET":
            return FakeResponse(
                    code=200, data=dict(vpcs=list(self._vpcs.values())))
        if request.method == "DELETE":
            id_, = map(int, request.full_url.rsplit("/", maxsplit=1)[-1:])
            assert not self._vpcs[id_]["default"]
            del self._vpcs[id_]
            return FakeResponse(code=204)
        raise DoNotKnowHowToProcessRequestError(request)

    def get_number_of_vpcs(self):
        return len(self._vpcs)


class FakeDigitalOcean:
    def __init__(self):
        self._droplets_processor = DropletsProcessor()
        self._vpcs_processor = VpcsProcessor()

    def process_request(self, request):
        (endpoint,) = re.match(  # type: ignore[union-attr]
            pattern=r"https://api.digitalocean.com/v2/(\w+)",
            string=request.full_url,
        ).groups()
        return dict(
            droplets=self._droplets_processor, vpcs=self._vpcs_processor,
        )[endpoint].process(request)

    def get_number_of_droplets(self):
        return self._droplets_processor.get_number_of_droplets()

    def get_number_of_vpcs(self):
        return self._vpcs_processor.get_number_of_vpcs()


class FakeResponse:
    def __init__(self, *, data="", code):
        self._data = data
        self._code = code

    def read(self) -> bytes:
        return json.dumps(self._data).encode("utf8")

    @property
    def code(self):
        return self._code


class FakeURLOpener:
    def __init__(self):
        self.do = FakeDigitalOcean()

    @contextlib.contextmanager
    def __call__(self, request):
        response = self.do.process_request(request)
        if str(response.code).startswith("2"):
            yield response
        else:
            raise HTTPError(
                    url="n/a",
                    code=response.code,
                    msg="Unprocessable Entity",
                    hdrs=None,  # type: ignore[arg-type]
                    fp=io.BytesIO(json.dumps(dict(no="thanks")).encode()),
            )
