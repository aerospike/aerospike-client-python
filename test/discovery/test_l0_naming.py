"""L0 discovery tests that need no server."""

import importlib
import pkgutil
import re
from pathlib import Path
from types import ModuleType
from typing import Iterator

import aerospike
import aerospike_helpers
from aerospike import exception as e

SOURCE_ROOT = Path(__file__).resolve().parents[2]
NEUTRAL_NAME = re.compile("nlb", re.IGNORECASE)


def public_names(obj: object) -> list[str]:
    return [name for name in dir(obj) if not name.startswith("_")]


def helper_modules() -> Iterator[ModuleType]:
    for info in pkgutil.walk_packages(aerospike_helpers.__path__, prefix="aerospike_helpers."):
        yield importlib.import_module(info.name)


def config_keys() -> list[str]:
    """Return every client config and policy key the client accepts, read from the C source."""
    source = (SOURCE_ROOT / "src" / "main" / "aerospike.c").read_text()
    start = source.index("DEFINE_SET_OF_VALID_KEYS(\n")
    end = source.index("struct py_set_name_to_str_list")
    return re.findall(r'"([^"]+)"', source[start:end])


# U-11: no public identifier or config key names a load balancer product
def test_public_names_are_topology_neutral() -> None:
    keys = config_keys()
    assert "ip_map" in keys and "use_services_alternate" in keys

    names = {f"config key {key}" for key in keys}
    for obj, prefix in [(aerospike, "aerospike"), (aerospike.Client, "Client"), (e, "exception")]:
        names.update(f"{prefix}.{name}" for name in public_names(obj))
    for module in helper_modules():
        names.add(module.__name__)
        names.update(f"{module.__name__}.{name}" for name in public_names(module))

    assert sorted(name for name in names if NEUTRAL_NAME.search(name)) == []
