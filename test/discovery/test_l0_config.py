"""L0 discovery tests: client config and naming, with no server.

Invalid ip_map and use_services_alternate values are covered against a server in
test/new_tests/test_client_config_level_options.py.
"""

import importlib
import pkgutil
import re
from pathlib import Path

import pytest

import aerospike
import aerospike_helpers
from aerospike import exception as e

# Nothing listens here, so the client never connects
UNREACHABLE_SEED = ("127.0.0.1", 1)
SOURCE_ROOT = Path(__file__).resolve().parents[2]
NEUTRAL_NAME = re.compile("nlb", re.IGNORECASE)


def make_config(**options):
    return {"hosts": [UNREACHABLE_SEED], "fail_if_not_connected": False, "validate_keys": True, **options}


@pytest.mark.parametrize(
    "options",
    [
        {"use_services_alternate": True},
        {"use_services_alternate": False},
        {"ip_map": {}},
        {"ip_map": {"10.0.0.1": "203.0.113.1"}},
        {"ip_map": {"10.0.0.1": "node1.public.example.com", "2001:db8::10": "2001:db8:ffff::10"}},
        {"ip_map": {"10.0.0.1": "203.0.113.1"}, "use_services_alternate": True},
    ],
)
def test_discovery_options_accepted(options):
    client = aerospike.client(make_config(**options))
    client.close()


def public_names(obj):
    return [name for name in dir(obj) if not name.startswith("_")]


def helper_modules():
    for info in pkgutil.walk_packages(aerospike_helpers.__path__, "aerospike_helpers."):
        yield importlib.import_module(info.name)


def config_keys():
    """Return every client config and policy key the client accepts, read from the C source."""
    source = (SOURCE_ROOT / "src" / "main" / "aerospike.c").read_text()
    start = source.index("DEFINE_SET_OF_VALID_KEYS(\n")
    end = source.index("struct py_set_name_to_str_list")
    return re.findall(r'"([^"]+)"', source[start:end])


# U-11: no public identifier or config key names a load balancer product
def test_public_names_are_topology_neutral():
    keys = config_keys()
    assert "ip_map" in keys and "use_services_alternate" in keys

    names = {f"config key {key}" for key in keys}
    for obj, prefix in [(aerospike, "aerospike"), (aerospike.Client, "Client"), (e, "exception")]:
        names.update(f"{prefix}.{name}" for name in public_names(obj))
    for module in helper_modules():
        names.add(module.__name__)
        names.update(f"{module.__name__}.{name}" for name in public_names(module))

    assert sorted(name for name in names if NEUTRAL_NAME.search(name)) == []
