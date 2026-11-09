"""L0 discovery tests of the seed provider and address translation settings. They need no server.

A client whose seeds are unreachable is created with fail_if_not_connected false, so it starts its
threads without a cluster. The C client's conformance entry points are reached through
c_discovery.py.
"""

import gc
import os
import subprocess
import sys
import time

import pytest

import aerospike
import c_discovery
from aerospike import exception as e
from aerospike_helpers.discovery import SeedCandidateContext, SeedCandidateProvider

UNREACHABLE = ("127.0.0.1", 1)
TEND_INTERVAL_MS = 250
OFFLINE = {"hosts": [UNREACHABLE], "fail_if_not_connected": False, "tend_interval": TEND_INTERVAL_MS}
NODE = "BB9030011AC4202"


class Seeds(SeedCandidateProvider):
    """Returns hosts, or raises it when it is an exception, and records every call."""

    def __init__(self, hosts, periodic=False):
        self.hosts = hosts
        self.periodic = periodic
        self.contexts = []
        self.closed = 0

    def refresh_seed_candidates(self, context):
        self.contexts.append(context)
        if isinstance(self.hosts, Exception):
            raise self.hosts
        return self.hosts

    def needs_periodic_refresh(self):
        return self.periodic

    def close(self):
        self.closed += 1


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


# U-3: the default config is the static provider and the static-map translator, which run no
# thread. A periodic provider adds exactly one, which shows the count would see a discovery thread.
def test_u3_default_construction_adds_no_thread():
    default = threads_added(OFFLINE)
    static = threads_added({**OFFLINE, "discovery": {"provider": "static"}, "translation": {"provider": "static-map"}})
    provider = threads_added({**OFFLINE, "discovery": {"provider": Seeds([UNREACHABLE], periodic=True)}})
    print(f"U-3 threads default={default} static={static} provider={provider}")
    assert static == default
    assert provider == default + 1


def legacy_ip_map(ip_map, endpoint):
    """The ip_map rule before pluggable translation: replace a host that is a key, keep the port and TLS name."""
    host = ip_map.get(endpoint["host"], endpoint["host"])
    return {"host": host, "port": endpoint["port"], "tlsName": endpoint["tlsName"]}


LEGACY_IP_MAP = {"10.0.0.1": "203.0.113.1", "2001:db8::1": "2001:db8:ffff::1", "node.local": "203.0.113.3"}


# U-7: ip_map inputs give the same endpoints through the static-map translator as the legacy rule,
# and ip_map and translation.static_map accept and reject the same dicts
@pytest.mark.parametrize(
    "host, port, tls_name, source",
    [
        ("10.0.0.1", 3000, "db1.example.com", "standard"),
        ("10.0.0.1", 4333, None, "alternate"),
        ("10.0.0.9", 3000, "db2.example.com", "standard"),
        ("2001:db8::1", 3000, None, "standard"),
        ("node.local", 3100, None, "standard"),
        ("10.0.0.1:3000", 3000, None, "standard"),
    ],
)
def test_u7_ip_map_parity(host, port, tls_name, source):
    endpoint = {"nodeName": NODE, "tlsName": tls_name, "host": host, "port": port, "sourceList": source}
    actual = c_discovery.translate("static-map", "pass_through", LEGACY_IP_MAP, endpoint)
    assert actual == legacy_ip_map(LEGACY_IP_MAP, endpoint)


@pytest.mark.parametrize("ip_map", [{"10.0.0.1": 1}, {1: "10.0.0.1"}, ["10.0.0.1"], {"\ud800": "10.0.0.1"}])
def test_u7_ip_map_and_static_map_reject_the_same_input(ip_map):
    legacy = raises(e.ParamError, {**OFFLINE, "ip_map": ip_map})
    static_map = raises(e.ParamError, {**OFFLINE, "translation": {"static_map": ip_map}})
    assert static_map == legacy.replace("ip_map", "translation.static_map")


def test_u7_ip_map_and_static_map_accept_the_same_input():
    for config in ({"ip_map": LEGACY_IP_MAP}, {"translation": {"static_map": LEGACY_IP_MAP}}):
        client = aerospike.client({**OFFLINE, **config})
        client.close()


# U-9: a strict miss names the translator, the node, the advertised endpoint and its list, and the
# setting that avoids it
@pytest.mark.parametrize(
    "kind, entries",
    [("static-map", {"10.0.0.1": "203.0.113.1"}), ("port-map", {"10.0.0.1:3000": "lb.example.com:30001"})],
)
def test_u9_strict_miss_error_content(kind, entries):
    endpoint = {"nodeName": NODE, "tlsName": None, "host": "10.0.0.9", "port": 3000, "sourceList": "alternate"}
    with pytest.raises(c_discovery.CError) as error:
        c_discovery.translate(kind, "strict", entries, endpoint)
    print(f"U-9 {kind}: {error.value.message}")
    assert error.value.code == e.TranslationMiss.code
    for part in (kind, NODE, "10.0.0.9:3000", "alternate", "translation.on_miss", "pass_through"):
        assert part in error.value.message


def test_discovery_exception_codes():
    assert issubclass(e.TranslationMiss, e.ClientError)
    assert issubclass(e.TranslationCollision, e.ClientError)
    assert issubclass(e.SeedDiscoveryFailed, e.ClientError)
    assert (e.TranslationMiss.code, e.TranslationCollision.code, e.SeedDiscoveryFailed.code) == (-25, -26, -27)


def test_provider_supplies_every_seed():
    seeds = Seeds([UNREACHABLE])
    client = aerospike.client({**OFFLINE, "hosts": [], "cluster_name": "c1", "discovery": {"provider": seeds}})
    client.close()
    assert seeds.contexts == [SeedCandidateContext("c1", (), 0)]


def test_provider_context_has_configured_seeds():
    seeds = Seeds([])
    hosts = [UNREACHABLE, ("127.0.0.2", 2, "tls-name")]
    client = aerospike.client({**OFFLINE, "hosts": hosts, "discovery": {"provider": seeds}})
    client.close()
    assert seeds.contexts[0].configured_seeds == tuple(hosts)
    assert seeds.contexts[0].cluster_name is None


# The registration outlives Client.close() so the client can connect again, and close() runs once
# when the client object is destroyed
def test_provider_closed_when_client_destroyed():
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


def test_provider_rejected_in_tend_mode():
    seeds = Seeds([UNREACHABLE])
    references = sys.getrefcount(seeds)
    message = raises(e.ParamError, {**OFFLINE, "discovery": {"provider": seeds, "execution_mode": "tend"}})
    gc.collect()
    assert message == "Binding seed provider requires thread execution mode"
    assert seeds.contexts == []
    assert seeds.closed == 0
    assert sys.getrefcount(seeds) == references


def test_provider_error_without_seeds():
    seeds = Seeds(RuntimeError("no seeds today"))
    message = raises(e.SeedDiscoveryFailed, {"hosts": [], "discovery": {"provider": seeds}})
    assert message.startswith("Seed provider Seeds failed and no seed host is left")
    assert "RuntimeError: no seeds today" in message
    gc.collect()
    assert seeds.closed == 1


@pytest.mark.parametrize("hosts", [[("127.0.0.1", 0)], [("127.0.0.1",)], [["127.0.0.1", 3000]], ("127.0.0.1", 3000)])
def test_provider_invalid_hosts(hosts):
    message = raises(e.SeedDiscoveryFailed, {"hosts": [], "discovery": {"provider": Seeds(hosts)}})
    assert "refresh_seed_candidates()" in message


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
        ({"discovery": {"provider": "dns", "dns": {"names": "a.test"}}}, "discovery.dns.names must be a list"),
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


def test_dns_provider_config_accepted():
    dns = {"names": ["seeds.test"], "record_type": "SRV", "port": 3100, "tls_name": "db", "timeout_ms": 50,
           "servers": ["127.0.0.1:1"]}
    client = aerospike.client({**OFFLINE, "validate_keys": True, "discovery": {"provider": "dns", "dns": dns}})
    client.close()
