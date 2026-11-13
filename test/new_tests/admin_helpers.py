"""Waits for security changes, which the server applies after the admin command that makes them returns."""

import time
from typing import Any, Callable

import aerospike
from aerospike import exception as e

TIMEOUT_SECS = 10.0
INTERVAL_SECS = 0.1


def assert_eventually_equal(client: aerospike.Client, fetch: Callable[[], Any], expected: Any) -> None:
    """Poll fetch() until it returns expected from every node; fail with its last result after TIMEOUT_SECS.

    fetch() must send one admin command through client. The C client picks the node for each admin command
    round-robin (as_node_get_random()), so as many matches in a row as the cluster has nodes means every node has
    applied the change, as long as no other thread sends commands through client meanwhile.
    A user or role that isn't applied yet raises InvalidUser or InvalidRole, so fetch() raising either is retried.
    """
    deadline = time.monotonic() + TIMEOUT_SECS
    matches = 0
    while True:
        try:
            result = fetch()
        except (e.InvalidUser, e.InvalidRole) as error:
            result = error
        if result != expected:
            matches = 0
        else:
            matches += 1
            node_count = len(client.get_node_names())
            if matches >= node_count:
                return
            continue
        if time.monotonic() >= deadline:
            raise AssertionError(f"after {TIMEOUT_SECS} s: {result!r} != {expected!r}")
        time.sleep(INTERVAL_SECS)


def user_exists(client: aerospike.Client, user: str) -> bool:
    try:
        client.admin_query_user_info(user)
    except e.InvalidUser:
        return False
    return True


def role_exists(client: aerospike.Client, role: str) -> bool:
    try:
        client.admin_get_role(role)
    except e.InvalidRole:
        return False
    return True
