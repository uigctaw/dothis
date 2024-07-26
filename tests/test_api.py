from urllib.error import HTTPError

import pytest

from dothis.api import DigitalOcean, FutureReference, InfraMaker
from dothis.resources import Droplets, VPCs
from tests.fakes import GARGANTUAN, ILLEGAL_SIZE, FakeURLOpener


def test_noop():
    with InfraMaker():
        pass


def test_create_one_droplet():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())
    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name="my_droplet", do_api=do_api))

        droplet = droplets(name="test_droplet", image="foo", size="bar")
    assert droplet["name"] == "test_droplet"


def test_create_droplet_in_vpc():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())
    with InfraMaker() as infra_maker:
        vpcs = infra_maker(VPCs(do_api=do_api))
        droplets = infra_maker(Droplets(tag_name="my_droplet", do_api=do_api))

        vpc = vpcs(name="my_vpc", region="over yonder")
        droplet = droplets(
                name="test_droplet",
                vpc_uuid=vpc["id"],
                size="foo",
                image="bar",
        )
    assert vpc["id"] == droplet["vpc_uuid"]


def test_complex_dependencies_are_respected():
    do_api = DigitalOcean(token=None, open_url=FakeURLOpener())

    kws = dict(size="medium", image="bsd")
    with InfraMaker() as infra_maker:
        vpcs = infra_maker(VPCs(do_api=do_api))
        droplets = infra_maker(Droplets(tag_name="my_droplet", do_api=do_api))

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

    kws = dict(size="foo", image="bar")
    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name="my_droplet", do_api=do_api))

        droplets(name="d1", **kws)
        drop = droplets(name="d1", **kws)

    assert drop["name"] == "d1"


def test_deleting_a_resource():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name="my_droplet", do_api=do_api))

        droplets(name="d1", size="foo", image="bar")

    assert fake_url_opener.do.get_number_of_droplets() == 1

    with InfraMaker() as infra_maker:
        droplets = infra_maker(Droplets(tag_name="my_droplet", do_api=do_api))

    assert fake_url_opener.do.get_number_of_droplets() == 0


def test_create_1_vpc_and_1_droplet_with_rerun():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    for _ in range(2):
        with InfraMaker() as infra_maker:
            vpcs = infra_maker(VPCs(do_api))
            droplets = infra_maker(Droplets(do_api, tag_name="greetings"))

            vpcs(name="acoin", region="hi")
            droplets(name="main", image="bar", size="foo")

    assert fake_url_opener.do.get_number_of_vpcs() == 1
    assert fake_url_opener.do.get_number_of_droplets() == 1


def test_unprocessable_entity_gives_a_meaninful_error():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    with (  # noqa: PT012
            pytest.raises(HTTPError, match=ILLEGAL_SIZE),
            InfraMaker() as infra_maker,
    ):
        droplets = infra_maker(Droplets(do_api, tag_name="greetings"))
        droplets(name="hi", image="bar", size=ILLEGAL_SIZE)


def test_long_creation_is_ok():
    fake_url_opener = FakeURLOpener()
    do_api = DigitalOcean(token=None, open_url=fake_url_opener)

    with (  # noqa: PT012
            pytest.raises(HTTPError, match=ILLEGAL_SIZE),
            InfraMaker() as infra_maker,
    ):
        droplets = infra_maker(Droplets(do_api, tag_name="greetings"))

        drop1 = droplets(name="hello", image="bar", size=GARGANTUAN)
        drop2 = droplets(
                name=drop1["name"] + " there", image="bar", size="nvm")
        assert isinstance(drop1["name"], FutureReference)

    assert drop2["name"] == "hello there"
