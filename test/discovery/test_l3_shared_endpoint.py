"""L3 discovery test against the kind and AKO cluster that the setup-kind-ako action builds.

Every Kubernetes node carries the same endpoint hostname as its configured alternate access
address, so the operator advertises that hostname with each pod's own Service NodePort as the
alternate access port. The client seeds through the ingress port of the first pod, reads the
alternate list with use_services_alternate and no ip_map, and must reach every pod at the address
it advertises.
"""

import os
import socket
import time

import pytest

import aerospike
import discovery_info as di

NAMESPACE = "test"
SET_NAME = "l3-shared-endpoint"
TEND_INTERVAL_MS = 250
JOIN_TIMEOUT_SEC = 120


def ports(name):
    return [int(port) for port in os.environ[name].split(",") if port]


def connect(host, port):
    config = {
        "hosts": [(host, port)],
        "user": "admin",
        "password": os.environ["AEROSPIKE_ADMIN_PASSWORD"],
        "use_services_alternate": True,
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": 5000,
    }
    return aerospike.client(config)


def client_nodes(client):
    return {entry["node_name"]: entry for entry in client.get_node_names()}


def addresses(host):
    resolved = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    return {info[4][0] for info in resolved}


# K-10, T3 variant: one hostname with a port per pod, rendered by the operator
@pytest.mark.l3
def test_k10_shared_endpoint_advertises_each_pod_node_port_and_routes_every_partition():
    endpoint = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    node_ports = ports("DISCOVERY_SERVICE_NODE_PORTS")
    seed_port = ports("DISCOVERY_INGRESS_PORTS")[0]
    endpoint_addresses = addresses(endpoint)
    assert len(node_ports) >= 2, f"Service NodePorts {node_ports}: K-10 needs at least two pods"
    assert len(set(node_ports)) == len(node_ports), f"Service NodePorts {node_ports} are not distinct"

    client = connect(endpoint, seed_port)
    try:

        def joined():
            held = client_nodes(client)
            return len(held) == len(node_ports)

        complete = di.wait_for(joined, JOIN_TIMEOUT_SEC)
        held = client_nodes(client)
        assert complete, f"Client holds {sorted(held)}, expected {len(node_ports)} nodes"
        # The client exposes no partition map, so give it the tends that fetch every node's partitions
        time.sleep(4 * TEND_INTERVAL_MS / 1000)

        infos = {}
        advertised = {}
        seeded = []
        for name, entry in held.items():
            info = di.node_info(client, name)
            infos[name] = info
            alternates = di.endpoints(info("service-clear-alt"))
            print(f"L3-NODE {name} client={entry} alternate={alternates}")
            assert len(alternates) == 1, f"{name} advertises {alternates}"
            host, port = di.split_endpoint(alternates[0])
            assert host == endpoint, f"{name} advertises {alternates[0]}"
            advertised[name] = port

            assert entry["address"] in endpoint_addresses, f"{entry} is not at {endpoint} {endpoint_addresses}"
            if entry["port"] == seed_port:
                seeded.append(name)
            else:
                assert entry["port"] == port, f"{entry} is not at its advertised port {port}"

        assert sorted(advertised.values()) == sorted(node_ports), f"Advertised {advertised}, NodePorts {node_ports}"
        assert len(seeded) <= 1, f"Nodes held at the seed: {seeded}"
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, "L3-K-10", infos)
    finally:
        client.close()

    # The node the client seeded is held at the seed, so reach it again at its advertised address
    for name in seeded:
        port = advertised[name]
        client = connect(endpoint, port)
        try:
            held = client_nodes(client)
            print(f"L3-ADVERTISED node={name} seed={endpoint}:{port} client={held.get(name)}")
            assert name in held, f"{name} is not reachable at {endpoint}:{port}"
            assert held[name]["port"] == port
        finally:
            client.close()
