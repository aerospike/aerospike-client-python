"""L0 discovery tests of the seed provider and address translation settings. They need no server.

A client whose seeds are unreachable is created with fail_if_not_connected false, so it starts its
threads without a cluster. Tests are in the order of their IDs in the testing strategy; tests
without an ID are last.
"""

import gc
import os
import subprocess
import sys
import threading
import time

import pytest

import aerospike
from aerospike import exception as e
from aerospike_helpers.discovery import SeedCandidateContext, SeedCandidateProvider

UNREACHABLE = ("127.0.0.1", 1)
TEND_INTERVAL_MS = 250
OFFLINE = {"hosts": [UNREACHABLE], "fail_if_not_connected": False, "tend_interval": TEND_INTERVAL_MS}


class Seeds(SeedCandidateProvider):
    """Returns hosts and periodic, or raises error and periodic_error instead when they are set, and
    records every refresh."""

    def __init__(self, hosts=None, periodic=False, error=None, periodic_error=None):
        if hosts is None:
            hosts = []
        self.hosts = hosts
        self.periodic = periodic
        self.error = error
        self.periodic_error = periodic_error
        self.contexts = []
        self.closed = 0

    def refresh_seed_candidates(self, context):
        self.contexts.append(context)
        if self.error:
            raise self.error
        return self.hosts

    def needs_periodic_refresh(self):
        if self.periodic_error:
            raise self.periodic_error
        return self.periodic

    def close(self):
        self.closed += 1


class BlockedSeeds(SeedCandidateProvider):
    """Blocks every refresh until released."""

    def __init__(self):
        self.released = threading.Event()
        self.closed = threading.Event()

    def refresh_seed_candidates(self, context):
        self.released.wait()
        return [UNREACHABLE]

    def needs_periodic_refresh(self):
        return True

    def close(self):
        self.closed.set()


def thread_count():
    """Return the number of OS threads in this process, including the C client's."""
    if os.path.isdir("/proc/self/task"):
        return len(os.listdir("/proc/self/task"))
    reply = subprocess.run(["ps", "-M", "-p", str(os.getpid())], capture_output=True, text=True, check=True)
    return len(reply.stdout.strip().splitlines()) - 1


def threads_added(config):
    """Return how many threads a client created from config runs."""
    before = thread_count()
    client = aerospike.client(config)
    try:
        time.sleep(2 * TEND_INTERVAL_MS / 1000)
        return thread_count() - before
    finally:
        client.close()
        del client
        gc.collect()


def raises(exception, config):
    with pytest.raises(exception) as error:
        aerospike.client(config)
    return error.value.msg


# U-1: the SeedCandidateProvider contract through the binding

@pytest.mark.parametrize("cluster_name", [None, "c1"])
def test_u1_provider_context(cluster_name):
    seeds = Seeds()
    hosts = [UNREACHABLE, ("127.0.0.2", 2, "tls-name")]
    config = {**OFFLINE, "hosts": hosts, "discovery": {"provider": seeds}}
    if cluster_name:
        config["cluster_name"] = cluster_name
    client = aerospike.client(config)
    client.close()
    expected = SeedCandidateContext(cluster_name=cluster_name, configured_seeds=tuple(hosts), tend_iteration=0)
    assert seeds.contexts == [expected]


# A provider that returns no hosts, raises or blocks past refresh_timeout_ms leaves the client on its
# configured seeds, and no Python exception reaches the caller
@pytest.mark.parametrize("seeds_args", [{"hosts": []}, {"error": RuntimeError("no seeds today")}])
def test_u1_failed_refresh_keeps_configured_seeds(seeds_args):
    seeds = Seeds(**seeds_args)
    client = aerospike.client({**OFFLINE, "discovery": {"provider": seeds}})
    client.close()
    assert len(seeds.contexts) == 1


def test_u1_blocked_refresh_keeps_configured_seeds():
    seeds = BlockedSeeds()
    start = time.monotonic()
    client = aerospike.client({**OFFLINE, "discovery": {"provider": seeds, "refresh_timeout_ms": 100}})
    elapsed = time.monotonic() - start
    seeds.released.set()
    client.close()
    assert elapsed < 1, f"Client construction waited {elapsed:.1f} s for a blocked refresh"


def test_u1_periodic_refresh_continues_after_errors():
    seeds = Seeds(periodic=True, error=RuntimeError("no seeds today"))
    client = aerospike.client({**OFFLINE, "discovery": {"provider": seeds, "refresh_interval_ms": TEND_INTERVAL_MS}})
    try:
        deadline = time.monotonic() + 10 * TEND_INTERVAL_MS / 1000
        while len(seeds.contexts) < 3 and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        client.close()
    assert len(seeds.contexts) >= 3


def test_u1_provider_error_without_seeds():
    seeds = Seeds(error=RuntimeError("no seeds today"))
    message = raises(e.SeedDiscoveryFailed, {"hosts": [], "discovery": {"provider": seeds}})
    assert message.startswith("Seed provider Seeds failed and no seed host is left")
    assert "RuntimeError: no seeds today" in message
    gc.collect()
    assert seeds.closed == 1


@pytest.mark.parametrize("hosts", [[("127.0.0.1", 0)], [("127.0.0.1",)], [["127.0.0.1", 3000]], ("127.0.0.1", 3000)])
def test_u1_provider_invalid_hosts(hosts):
    message = raises(e.SeedDiscoveryFailed, {"hosts": [], "discovery": {"provider": Seeds(hosts)}})
    assert "refresh_seed_candidates()" in message


@pytest.mark.parametrize(
    "seeds_args, message",
    [
        ({"periodic_error": RuntimeError("unsure")},
         "discovery.provider.needs_periodic_refresh() raised: RuntimeError: unsure"),
        ({"periodic": 1}, "discovery.provider.needs_periodic_refresh() must return a bool"),
    ],
)
def test_u1_needs_periodic_refresh_must_return_a_bool(seeds_args, message):
    seeds = Seeds([UNREACHABLE], **seeds_args)
    assert raises(e.ParamError, {**OFFLINE, "discovery": {"provider": seeds}}) == message
    gc.collect()
    assert seeds.contexts == []
    assert seeds.closed == 0


def test_u1_provider_rejected_in_tend_mode():
    seeds = Seeds([UNREACHABLE])
    references = sys.getrefcount(seeds)
    message = raises(e.ParamError, {**OFFLINE, "discovery": {"provider": seeds, "execution_mode": "tend"}})
    gc.collect()
    assert message == "Binding seed provider requires thread execution mode"
    assert seeds.contexts == []
    assert seeds.closed == 0
    assert sys.getrefcount(seeds) == references


# The registration outlives Client.close() so the client can connect again, and close() runs once
# when the client object is destroyed
def test_u1_provider_closed_when_client_destroyed():
    seeds = Seeds([UNREACHABLE])
    client = aerospike.client({**OFFLINE, "hosts": [], "discovery": {"provider": seeds}})
    client.close()
    client.connect()
    client.close()
    assert len(seeds.contexts) == 2
    assert seeds.closed == 0
    del client
    gc.collect()
    assert seeds.closed == 1


# A refresh still running when the client is destroyed closes the provider when it returns, even
# while the main thread waits outside Python code and so runs no pending calls
def test_u1_provider_closed_after_refresh_outlives_client():
    seeds = BlockedSeeds()
    raises(e.SeedDiscoveryFailed, {**OFFLINE, "hosts": [], "discovery": {"provider": seeds, "refresh_timeout_ms": 100}})
    gc.collect()
    assert not seeds.closed.is_set()
    seeds.released.set()
    assert seeds.closed.wait(2), "The provider was not closed when its refresh returned"


# U-3: the default config is the static provider and the static-map translator, which run no
# thread. A periodic provider adds exactly one, which shows the count would see a discovery thread.
def test_u3_default_construction_adds_no_thread():
    default = threads_added(OFFLINE)
    static = threads_added({**OFFLINE, "discovery": {"provider": "static"}, "translation": {"provider": "static-map"}})
    provider = threads_added({**OFFLINE, "discovery": {"provider": Seeds([UNREACHABLE], periodic=True)}})
    print(f"U-3 threads default={default} static={static} provider={provider}")
    assert static == default
    assert provider == default + 1


# U-4: the binding accepts the dns provider's config. The C client tests resolution.
def test_u4_dns_provider_config_accepted():
    dns = {"names": ["seeds.test"], "record_type": "SRV", "port": 3100, "tls_name": "db", "timeout_ms": 50,
           "servers": ["127.0.0.1:1"]}
    client = aerospike.client({**OFFLINE, "validate_keys": True, "discovery": {"provider": "dns", "dns": dns}})
    client.close()


@pytest.mark.parametrize("record_type", ["a", "A", "srv", "SRV"])
def test_u4_dns_record_type_accepts_either_case(record_type):
    dns = {"names": ["seeds.test"], "record_type": record_type, "servers": ["127.0.0.1:1"]}
    client = aerospike.client({**OFFLINE, "discovery": {"provider": "dns", "dns": dns}})
    client.close()


# U-5 and every other option that takes a name: each documented name is accepted
@pytest.mark.parametrize(
    "config",
    [{"discovery": {"merge_policy": name}} for name in ("merge", "replace", "discovery_only")]
    + [{"discovery": {"execution_mode": name}} for name in ("thread", "tend")]
    + [{"discovery": {"provider": "static"}}]
    + [{"translation": {"provider": name}} for name in ("static-map", "port-map", "pass-through")]
    + [{"translation": {"on_miss": name}} for name in ("pass_through", "strict")],
)
def test_u5_named_options_accepted(config):
    client = aerospike.client({**OFFLINE, "validate_keys": True, **config})
    client.close()


LEGACY_IP_MAP = {"10.0.0.1": "203.0.113.1", "2001:db8::1": "2001:db8:ffff::1", "node.local": "203.0.113.3"}


# U-7: ip_map and translation.static_map accept and reject the same dicts. The C client tests the
# static-map translator itself.
@pytest.mark.parametrize("ip_map", [{"10.0.0.1": 1}, {1: "10.0.0.1"}, ["10.0.0.1"], {"\ud800": "10.0.0.1"}])
def test_u7_ip_map_and_static_map_reject_the_same_input(ip_map):
    legacy = raises(e.ParamError, {**OFFLINE, "ip_map": ip_map})
    static_map = raises(e.ParamError, {**OFFLINE, "translation": {"static_map": ip_map}})
    assert static_map == legacy.replace("ip_map", "translation.static_map")


def test_u7_ip_map_and_static_map_accept_the_same_input():
    for config in ({"ip_map": LEGACY_IP_MAP}, {"translation": {"static_map": LEGACY_IP_MAP}}):
        client = aerospike.client({**OFFLINE, **config})
        client.close()


# Tests without an ID: exception classes and config validation

def test_discovery_exception_codes():
    assert issubclass(e.TranslationMiss, e.ClientError)
    assert issubclass(e.TranslationCollision, e.ClientError)
    assert issubclass(e.SeedDiscoveryFailed, e.ClientError)
    assert (e.TranslationMiss.code, e.TranslationCollision.code, e.SeedDiscoveryFailed.code) == (-25, -26, -27)


def test_hosts_needed_without_provider():
    assert raises(e.ParamError, {"hosts": []}) == "Hosts must not be empty"
    assert raises(e.ParamError, {"discovery": {"provider": "static"}}) == "Hosts must be a list"


@pytest.mark.parametrize(
    "config, message",
    [
        ({"discovery": {"bogus": 1}}, '"bogus" is an invalid client config dictionary key'),
        ({"discovery": {"provider": "dns", "dns": {"bogus": 1}}}, '"bogus" is an invalid client config dictionary key'),
        ({"translation": {"bogus": 1}}, '"bogus" is an invalid client config dictionary key'),
        ({"discovery": []}, "discovery must be a dict"),
        ({"discovery": {"provider": "aws-tags"}}, "discovery.provider must be"),
        ({"discovery": {"provider": object()}}, "SeedCandidateProvider"),
        ({"discovery": {"merge_policy": "first"}}, "discovery.merge_policy must be"),
        ({"discovery": {"execution_mode": "process"}}, "discovery.execution_mode must be"),
        ({"discovery": {"refresh_interval_ms": -1}}, "discovery.refresh_interval_ms: OverflowError"),
        ({"discovery": {"refresh_timeout_ms": 2**32}}, "discovery.refresh_timeout_ms: ValueError"),
        ({"discovery": {"tend_refresh_deadline_ms": "100"}}, "discovery.tend_refresh_deadline_ms: TypeError"),
        ({"discovery": {"dns": {"names": ["a.test"]}}}, 'discovery.dns needs discovery.provider "dns"'),
        ({"discovery": {"provider": Seeds(), "dns": {"names": ["a.test"]}}},
         'discovery.dns needs discovery.provider "dns"'),
        ({"discovery": {"provider": "dns", "dns": {"names": "a.test"}}}, "discovery.dns.names must be a list"),
        ({"discovery": {"provider": "dns", "dns": {"names": [5]}}}, "discovery.dns.names: TypeError"),
        ({"discovery": {"provider": "dns", "dns": {"names": ["a.test"], "tls_name": 5}}},
         "discovery.dns.tls_name: TypeError"),
        ({"discovery": {"provider": "dns", "dns": {"names": ["a.test"], "port": 0}}},
         "Invalid DNS seed provider port: 0"),
        ({"discovery": {"provider": "dns", "dns": {"names": ["a.test"], "port": 70000}}},
         "discovery.dns.port: ValueError"),
        ({"discovery": {"provider": "dns", "dns": {"names": ["a.test"], "record_type": "mx"}}}, "record_type"),
        ({"translation": {"provider": "custom"}}, "translation.provider must be"),
        ({"translation": {"on_miss": "fail"}}, "translation.on_miss must be"),
        ({"translation": {"provider": "pass-through", "port_map": {}}}, 'needs translation.provider "port-map"'),
        ({"translation": {"static_map": {"a": "b"}}, "ip_map": {"a": "b"}}, "Set either ip_map or"),
        ({"translation": {"provider": "port-map"}, "ip_map": {"a": "b"}}, "Set either ip_map or"),
        ({"translation": {"provider": "port-map", "port_map": {"10.0.0.1:3000": "nope"}}}, "port-map"),
        ({"use_shared_connection": True, "translation": {"provider": "pass-through"}}, "use_shared_connection"),
        ({"use_shared_connection": True, "discovery": {"provider": "dns", "dns": {"names": ["a.test"],
                                                                                   "servers": ["127.0.0.1"]}}},
         "use_shared_connection"),
    ],
)
def test_invalid_config(config, message):
    actual = raises(e.ParamError, {**OFFLINE, "validate_keys": True, **config})
    assert message in actual
