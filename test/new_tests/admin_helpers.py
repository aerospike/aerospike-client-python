"""Waits for security changes, which the server applies after the admin command that makes them returns.

The client sends each admin command, including the reads polled here, to one random node, so on a cluster with more
than one node a poll that passes doesn't mean every node has applied the change.
"""

import time
from typing import Any, Callable

import aerospike
from aerospike import exception as e

TIMEOUT_SECS = 10.0
INTERVAL_SECS = 0.1


def assert_eventually_equal(fetch: Callable[[], Any], expected: Any) -> None:
    """Poll fetch() until it returns expected; fail with its last result after TIMEOUT_SECS.

    A user or role that isn't applied yet raises InvalidUser or InvalidRole, so fetch() raising either is retried.
    """
    deadline = time.monotonic() + TIMEOUT_SECS
    while True:
        try:
            result = fetch()
        except (e.InvalidUser, e.InvalidRole) as error:
            result = error
        if result == expected:
            return
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
