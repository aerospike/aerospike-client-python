import os

import pytest

import aerospike
import discovery_info as di


@pytest.fixture
def warnings():
    """Collect the client's warning and error log messages."""
    messages = []

    def log(level, function, path, line, message):
        messages.append(message)
        print(f"CLIENT-LOG {level} {message}")

    aerospike.set_log_handler(log)
    aerospike.set_log_level(aerospike.LOG_LEVEL_WARN)
    yield messages
    aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)
    aerospike.set_log_handler()


@pytest.fixture
def added_nodes(nodes):
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
        di.control(f"stop {container}")
    di.wait_cluster_stable([node.info for node in nodes], len(nodes))


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "l2(*profiles, subset='ce'): needs the L2 topology built with one of these profiles and this subset"
    )
    config.addinivalue_line("markers", "l3: needs the kind and AKO cluster")


def pytest_collection_modifyitems(config, items):
    profile = os.environ.get("L2_PROFILE")
    subset = os.environ.get("DISCOVERY_SUBSET")
    has_l3 = bool(os.environ.get("DISCOVERY_SERVICE_NODE_PORTS"))
    deselected = []
    selected = []
    for item in items:
        l2 = item.get_closest_marker("l2")
        if l2 and profile and (profile not in l2.args or l2.kwargs.get("subset", "ce") != subset):
            deselected.append(item)
            continue
        if l2 and not profile:
            item.add_marker(pytest.mark.skip(reason=f"needs the L2 topology with profile {' or '.join(l2.args)}"))
        if item.get_closest_marker("l3") and not has_l3:
            item.add_marker(pytest.mark.skip(reason="needs the kind and AKO cluster (DISCOVERY_SERVICE_NODE_PORTS)"))
        selected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    # pytest reports an expected failure as skipped, with wasxfail set
    if report.skipped and not hasattr(report, "wasxfail") and os.environ.get("DISCOVERY_FAIL_ON_SKIP") == "1":
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else str(report.longrepr)
        report.outcome = "failed"
        report.longrepr = f"Skipped with DISCOVERY_FAIL_ON_SKIP=1: {reason}"
