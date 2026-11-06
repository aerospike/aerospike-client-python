import os

import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "l2(*profiles): needs the L2 topology built with one of these profiles")
    config.addinivalue_line("markers", "l3: needs the kind and AKO cluster")


def pytest_collection_modifyitems(config, items):
    profile = os.environ.get("L2_PROFILE")
    has_l3 = bool(os.environ.get("DISCOVERY_SERVICE_NODE_PORTS"))
    deselected = []
    selected = []
    for item in items:
        l2 = item.get_closest_marker("l2")
        if l2 and profile and profile not in l2.args:
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
    if report.skipped and os.environ.get("DISCOVERY_FAIL_ON_SKIP") == "1":
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else str(report.longrepr)
        report.outcome = "failed"
        report.longrepr = f"Skipped with DISCOVERY_FAIL_ON_SKIP=1: {reason}"
