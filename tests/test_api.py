from urllib.error import HTTPError

import pytest

from dothis.api import DigitalOcean
from dothis.resources import Droplets, VPCs
from tests.fakes import GARGANTUAN, ILLEGAL_SIZE, FakeURLOpener


def test_create_one_droplet():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())

    with Droplets(tag="my_droplet", do_api=do_api) as droplets:
        droplet = droplets(
            name="test_droplet", image="foo", size="bar", region="here")

    assert droplet["name"] == "test_droplet"


def test_create_droplet_in_vpc():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())

    with (
            VPCs(do_api=do_api) as vpcs,
            Droplets(tag="my_droplet", do_api=do_api) as droplets,
    ):
        vpc = vpcs(name="my_vpc", region="over yonder")
        droplet = droplets(
                name="test_droplet",
                vpc_uuid=vpc["id"],
                size="foo",
                image="bar",
                region="over yonder",
        )

    assert vpc["id"] == droplet["vpc_uuid"]


def test_complex_dependencies_are_respected():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())

    kws = dict(size="medium", image="bsd", region="Moria")

    with (
        VPCs(do_api=do_api) as vpcs,
        Droplets(tag="my_droplet", do_api=do_api) as droplets,
    ):
        drop1 = droplets(name="d1", **kws)
        vpc1 = vpcs(name=drop1["name"] + "_v1", region="here")
        drop2 = droplets(name=vpc1["name"] + "_d2", **kws)
        vpc2 = vpcs(name=drop2["name"] + "_v2_" + vpc1["name"], region="there")

    assert drop1["name"] == "d1"
    assert vpc1["name"] == "d1_v1"
    assert drop2["name"] == "d1_v1_d2"
    assert vpc2["name"] == "d1_v1_d2_v2_d1_v1"


def test_creating_the_same_resource_twice():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())

    kws = dict(size="foo", image="bar", region="Shintaro")

    with Droplets(tag="my_droplet", do_api=do_api) as droplets:
        droplets(name="d1", **kws)
        drop = droplets(name="d1", **kws)

    assert drop["name"] == "d1"


def test_deleting_a_resource():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    with Droplets(tag="my_droplet", do_api=do_api) as droplets:
        drop1a = droplets(name="d1", size="foo1", image="bar1", region="baz1")
        droplets(name="d2", size="foo2", image="bar2", region="baz2")

    assert fake_url_opener.do.get_number_of_droplets() == 2

    with Droplets(tag="my_droplet", do_api=do_api) as droplets:
        drop1b = droplets(name="d1", size="foo1", image="bar1", region="baz1")
        # 2nd droplet is deleted
        droplets(name="d3", size="foo3", image="bar3", region="baz3")
        droplets(name="d4", size="foo4", image="bar4", region="baz4")

    assert fake_url_opener.do.get_number_of_droplets() == 3
    assert drop1a["id"] == drop1b["id"]


def test_create_1_vpc_and_1_droplet_with_rerun():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    for _ in range(2):
        with (
            VPCs(do_api) as vpcs,
            Droplets(do_api, tag="greetings") as droplets,
        ):
            vpcs(name="acoin", region="hi")
            droplets(name="main", image="bar", size="foo", region="hi")

    assert fake_url_opener.do.get_number_of_vpcs() == 2  # includes default
    assert fake_url_opener.do.get_number_of_droplets() == 1


def test_unprocessable_entity_gives_a_meaninful_error():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    with pytest.raises(HTTPError, match=ILLEGAL_SIZE):  # noqa: SIM117
        with Droplets(do_api, tag="greetings") as droplets:
            droplets(name="hi", image="bar", size=ILLEGAL_SIZE, region="dunno")


def test_long_creation_is_ok():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    class FakeTime:

        def sleep(self, seconds):
            pass

    with Droplets(do_api, tag="greetings", time_=FakeTime()) as droplets:
        drop1 = droplets(
            name="hello", image="bar", size=GARGANTUAN, region="Moon")
        drop2 = droplets(
                name=drop1["name"] + " there",
                image="bar",
                size="nvm",
                region="Moon",
        )

    assert drop2["name"] == "hello there"
