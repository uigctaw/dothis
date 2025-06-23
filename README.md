# dothis

dothis is a small helper library for creating and cleaning up
DigitalOcean resources in tests. It exposes context managers that make
HTTP requests to the DigitalOcean API to create droplets and VPCs and
then removes them once the context exits.

## Requirements

- Python 3.11
- [Poetry](https://python-poetry.org/) for dependency management

## Running the tests

Use the provided script which installs the dependencies and runs the
linters and test suite:

```bash
./run_tests.sh
```

## Example

```python
from dothis.api import DigitalOcean
from dothis.resources import Droplets, Vpcs

do_api = DigitalOcean(token="my-token")
with (
    Vpcs(do_api=do_api) as vpcs,
    Droplets(tag="example", do_api=do_api) as droplets,
):
    vpc = vpcs(name="my_vpc", region="nyc3")
    droplet = droplets(
        name="my_droplet",
        image="ubuntu-20-04-x64",
        size="s-1vcpu-1gb",
        region="nyc3",
        vpc_uuid=vpc["id"],
    )
print(droplet["name"])
```
