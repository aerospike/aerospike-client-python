from contextlib import contextmanager
from typing import Iterator

import pytest

import aerospike

SERVICE_CONTEXT = "context=service"
TEST_NAMESPACE_CONTEXT = "context=namespace;id=test"


def get_server_config(client: aerospike.Client, context: str, name: str) -> str:
    response = client.info_random_node(f"get-config:{context}")
    prefix = f"{name}="
    items = response.strip().split(";")
    settings = [item[len(prefix):] for item in items if item.startswith(prefix)]
    if not settings:
        pytest.fail(f"{name} not in get-config:{context}")
    return settings[0]


def set_server_config(client: aerospike.Client, context: str, name: str, value: str) -> None:
    responses = client.info_all(f"set-config:{context};{name}={value}")
    for error, response in responses.values():
        assert error is None
        assert response.strip() == "ok"


# Sets a dynamic server setting on every node and restores the original value afterwards.
@contextmanager
def set_server_dynamic_config(client: aerospike.Client, context: str, name: str, value: str) -> Iterator[None]:
    original = get_server_config(client, context=context, name=name)
    try:
        set_server_config(client, context=context, name=name, value=value)
    except aerospike.exception.RoleViolation:
        pytest.skip(f"set-config of {name} needs the sys-admin privilege")
    try:
        yield
    finally:
        set_server_config(client, context=context, name=name, value=original)
