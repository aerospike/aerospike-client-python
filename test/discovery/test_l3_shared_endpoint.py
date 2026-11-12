"""L3 discovery test against the kind and AKO cluster that the setup-kind-ako action builds.

Every Kubernetes node carries the same endpoint hostname as its configured alternate access
address, so the operator advertises that hostname with each pod's own Service NodePort as the
alternate access port. The client seeds through the ingress port of the first pod, reads the
alternate list with use_services_alternate and no ip_map, and must hold every pod at the address
it advertises. That includes the seeded pod: the seed address is not in its alternate list, so the
client moves to the advertised address.
"""

import pytest

import discovery_info as di
import l3_cluster as l3

SET_NAME = "l3-shared-endpoint"


# K-10, T3 variant: one hostname with a port per pod, rendered by the operator
@pytest.mark.l3
def test_k10_shared_endpoint_advertises_each_pod_node_port_and_routes_every_partition() -> None:
    cluster = l3.Cluster()
    endpoint = l3.endpoint()
    port = cluster.service_port()
    node_ports = [cluster.node_port(pod.name, port) for pod in cluster.pods()]
    assert len(node_ports) >= 2, f"Service NodePorts {node_ports}: K-10 needs at least two pods"
    assert len(set(node_ports)) == len(node_ports), f"Service NodePorts {node_ports} are not distinct"

    client = l3.connect(l3.seed())
    try:
        held = l3.wait_for_size(client, len(node_ports))
        advertised = {}
        for name, entry in held.items():
            alternates = di.endpoints(di.node_info(client, name)("service-clear-alt"))
            print(f"L3-NODE {name} client={entry} alternate={alternates}")
            assert len(alternates) == 1, f"{name} advertises {alternates}"
            host, advertised[name] = di.split_endpoint(alternates[0])
            assert host == endpoint, f"{name} advertises {alternates[0]}"
            di.assert_at(entry, alternates[0])

        assert sorted(advertised.values()) == sorted(node_ports), f"Advertised {advertised}, NodePorts {node_ports}"
        l3.assert_served(client, SET_NAME, len(node_ports), "L3-K-10")
    finally:
        client.close()
