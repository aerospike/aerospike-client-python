"""Waits for security changes, which the server applies after the admin command that makes them returns."""

import time
from typing import Any, Callable, Dict

import aerospike
from aerospike import exception as e

TIMEOUT_SECS = 10.0
INTERVAL_SECS = 0.1


def parse_fields(section: str) -> Dict[str, str]:
    """Parse a comma-separated list of key=value pairs from an info response."""
    return dict(field.split("=", 1) for field in section.split(","))


def is_security_settled(client: aerospike.Client) -> bool:
    """Return whether every node has committed the same security metadata and has no metadata change in flight.

    The info reference lists smd-info's keys without defining them. Per the server's smd.c, committed_key and
    committed_tid identify the content a node last committed, and change under the lock that applies it, so nodes
    with the same committed key and transaction ID hold the same users and roles. settled isn't checked because it
    stays false on mixed-version clusters.
    """
    responses = client.info_all("smd-info")
    committed = set()
    for error, response in responses.values():
        if error is not None:
            return False
        sections = dict(section.split(":", 1) for section in response.strip().split(";") if section)
        smd = parse_fields(sections["smd"])
        if smd["n_pending_sets"] != "0" or smd["n_events"] != "0":
            return False
        security = parse_fields(sections["security"])
        committed.add((security["committed_key"], security["committed_tid"]))
    return len(committed) == 1


def assert_eventually_equal(client: aerospike.Client, fetch: Callable[[], Any], expected: Any) -> None:
    """Poll fetch() until it returns expected and every node has applied it; fail after TIMEOUT_SECS.

    fetch() reads from one node, and is_security_settled() then shows the other nodes hold the same security metadata.
    A user or role that isn't applied yet raises InvalidUser or InvalidRole, so fetch() raising either is retried.
    """
    deadline = time.monotonic() + TIMEOUT_SECS
    while True:
        try:
            result = fetch()
        except (e.InvalidUser, e.InvalidRole) as error:
            result = error
        if result == expected and is_security_settled(client):
            return
        if time.monotonic() >= deadline:
            if result == expected:
                raise AssertionError(f"after {TIMEOUT_SECS} s: nodes still differ in smd-info security metadata")
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
