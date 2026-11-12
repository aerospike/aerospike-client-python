import os
from typing import Iterator

import pytest

import aerospike
import discovery_info as di


@pytest.fixture
def warnings() -> Iterator[list[str]]:
    """Collect the client's warning and error log messages."""
    messages = []

    def log(level: int, function: str, path: str, line: int, message: str) -> None:
        messages.append(message)
        print(f"CLIENT-LOG {level} {message}")

    aerospike.set_log_handler(log)
    aerospike.set_log_level(aerospike.LOG_LEVEL_WARN)
    yield messages
    aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)
    aerospike.set_log_handler()


@pytest.fixture
def added_nodes(nodes: list[di.ToxiproxyNode]) -> Iterator[list[str]]:
    """Collect the containers a test starts through the node server, and remove them and the
    listeners named after them afterwards."""
    containers = []
    yield containers
    if not containers:
        return
    proxies = di.toxiproxy("GET", "/proxies")
    for container in containers:
        if container in proxies:
            di.toxiproxy("DELETE", f"/proxies/{container}")
        di.stop_node(container)
    di.wait_cluster_stable([node.info for node in nodes], len(nodes))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "l2(*profiles, subset='ce'): needs the L2 topology built with one of these profiles and this subset"
    )
    config.addinivalue_line("markers", "l3(tls=False): needs the kind and AKO cluster, serving TLS or not")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    profile = os.environ.get("L2_PROFILE")
    subset = os.environ.get("DISCOVERY_SUBSET")
    has_l3 = bool(os.environ.get("DISCOVERY_KUBECONFIG"))
    l3_tls = bool(os.environ.get("DISCOVERY_TLS_CA_FILE"))
    deselected = []
    selected = []
    for item in items:
        l2 = item.get_closest_marker("l2")
        if l2 and profile and (profile not in l2.args or l2.kwargs.get("subset", "ce") != subset):
            deselected.append(item)
            continue
        if l2 and not profile:
            item.add_marker(pytest.mark.skip(reason=f"needs the L2 topology with profile {' or '.join(l2.args)}"))
        l3 = item.get_closest_marker("l3")
        if l3 and has_l3 and l3.kwargs.get("tls", False) != l3_tls:
            deselected.append(item)
            continue
        if l3 and not has_l3:
            item.add_marker(pytest.mark.skip(reason="needs the kind and AKO cluster (DISCOVERY_KUBECONFIG)"))
        selected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Iterator[None]:
    outcome = yield
    report = outcome.get_result()
    # pytest reports an expected failure as skipped, with wasxfail set
    if report.skipped and not hasattr(report, "wasxfail") and os.environ.get("DISCOVERY_FAIL_ON_SKIP") == "1":
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else str(report.longrepr)
        report.outcome = "failed"
        report.longrepr = f"Skipped with DISCOVERY_FAIL_ON_SKIP=1: {reason}"
