"""L3 discovery tests that scale the AerospikeCluster under a running client, and move the per-pod
Service NodePorts across pod rescheduling and recreation, on the shared endpoint the operator
renders: every pod advertises the endpoint hostname with its own Service NodePort as its alternate
address, and the client reads the alternate list with use_services_alternate and no translator.

The client exposes no partition map, so where the Java suite checks that every partition is owned,
these tests check from the nodes' own statistics that every node serves the keys it is master of.
"""

import time
from typing import Callable, Collection, Iterator, Optional

import pytest

import aerospike
import discovery_info as di
import l3_cluster as l3

NAMESPACE = "test"
SET_NAME = "l3-scale"
MIN_PODS = 2
# K-2's budget is two tend intervals from the new node accepting a login, which includes a tend that
# waited out connect_timeout on a connection made just before, so it runs at the default interval
SCALE_UP_TEND_INTERVAL_MS = 1000
LOAD_THREADS = 4
LOAD_KEYS = 1000
QUIET_TEND_INTERVALS = 5


@pytest.fixture
def cluster() -> Iterator[l3.Cluster]:
    """The AerospikeCluster, scaled back to its size and pod count when the test ends."""
    found = l3.Cluster()
    assert found.get(".spec.podSpec.multiPodPerHost") == "true", "Needs multiPodPerHost, as setup-kind-ako builds"
    assert found.get(".spec.aerospikeNetworkPolicy.alternateAccess") == "configuredIP"
    size = found.size()
    yield found
    if found.size() != size or len(found.pods()) != size:
        found.scale(size)


@pytest.fixture
def services(cluster: l3.Cluster) -> Iterator[list[str]]:
    """Collect the Services a test creates, and delete them afterwards."""
    created = []
    yield created
    for service in created:
        cluster.delete_service(service)
        print(f"L3-SERVICE-DELETED service={service}")


def highest_ordinal(pods: list[l3.Pod]) -> l3.Pod:
    return max(pods, key=lambda pod: l3.ordinal(pod.name))


def node_of(client: aerospike.Client, pod: l3.Pod) -> Optional[str]:
    """Return the name of the node that lists the pod's IP in its service-clear-std reply, or None."""
    for name in di.client_nodes(client):
        if l3.has_address(di.node_info(client, name)("service-clear-std"), pod.ip):
            return name
    return None


def new_peer_accepts_login(observer: Callable[[str], str], names: Collection[str]) -> bool:
    """Return whether a login succeeds at the address the observer node advertises for a peer not in
    names."""
    for name, _, endpoints in di.peers(observer("peers-clear-alt")):
        if name not in names and endpoints:
            return l3.accepts_login(di.split_endpoint(endpoints[0]))
    return False


def client_errors(client: aerospike.Client) -> int:
    return sum(node.error_count + node.timeout_count for node in client.get_stats().nodes)


# K-2: one client seeded once holds the pod the operator adds within two tend intervals of the
# later of the peers-generation bump that announces it and the new node accepting a login at the
# address the cluster advertises for it, which follows the pod turning Ready by seconds while the node
# gets the cluster's users. The client holds it at the endpoint hostname and the new pod's own
# Service NodePort, without being rebuilt or reseeded, and keeps the nodes it held.
@pytest.mark.l3
def test_k2_scale_up_new_node_active_within_two_tend_intervals(cluster: l3.Cluster) -> None:
    size = max(1, cluster.size() - 1)
    cluster.scale(size)
    before = {pod.name for pod in cluster.pods()}

    # A tend whose connection to the new node is dropped before its NodePort routes waits out
    # connect_timeout, so K-2 keeps the 1000 ms default
    client = l3.connect(l3.seed(), tend_interval=SCALE_UP_TEND_INTERVAL_MS, connect_timeout=1000)
    try:
        existing = l3.wait_for_size(client, size, SCALE_UP_TEND_INTERVAL_MS)
        observer = di.node_info(client, sorted(existing)[0])
        generation = observer("peers-generation")
        cluster.resize(size + 1)
        joined, active_ms = di.wait_joined(
            client, observer, generation, existing,
            reachable=lambda: new_peer_accepts_login(observer, existing), timeout=l3.SCALE_TIMEOUT_SEC,
        )
        budget_ms = 2 * SCALE_UP_TEND_INTERVAL_MS
        cluster.scale(size + 1)

        pod = next(pod for pod in cluster.pods() if pod.name not in before)
        node_port = cluster.node_port(pod.name, cluster.service_port())
        access = di.node_info(client, joined["node_name"])("service-clear-std")
        print(f"L3-SCALE-UP node={joined['node_name']} pod={pod.name} access={access} serviceNodePort={node_port} "
              f"client={joined} activeMs={active_ms:.0f} budgetMs={budget_ms}")
        assert l3.has_address(access, pod.ip), f"{joined['node_name']} access {access} is not {pod}"
        di.assert_at(joined, f"{l3.endpoint()}:{node_port}")
        assert active_ms <= budget_ms, (
            f"{joined['node_name']} active {active_ms:.0f} ms after the bump and its first login, "
            f"budget {budget_ms} ms"
        )

        held = l3.wait_for_size(client, size + 1, SCALE_UP_TEND_INTERVAL_MS)
        assert set(existing) <= set(held), f"Client holds {sorted(held)}, held {sorted(existing)} before"
        l3.assert_served(client, SET_NAME, size=size + 1, row="L3-K-2", tend_interval_ms=SCALE_UP_TEND_INTERVAL_MS)
    finally:
        client.close()


# K-3: with reads and writes running, the pod the operator removes leaves the client's nodes, every
# remaining node is a remaining pod, and once the remaining nodes are stable commands succeed while
# the client's error and retry counts and the failed commands stay flat over several tend intervals.
@pytest.mark.l3
def test_k3_scale_down_removes_node_without_retry_storm(cluster: l3.Cluster) -> None:
    size = max(MIN_PODS, cluster.size())
    cluster.scale(size)
    removed = highest_ordinal(cluster.pods())

    client = l3.connect(l3.seed())
    try:
        l3.wait_for_size(client, size)
        victim = node_of(client, removed)
        assert victim, f"No node for {removed}"
        print(f"L3-SCALE-DOWN-VICTIM node={victim} pod={removed}")

        def command(thread: int, iteration: int) -> None:
            key = (NAMESPACE, SET_NAME, f"load-{(thread + iteration * LOAD_THREADS) % LOAD_KEYS}")
            client.put(key, {"v": iteration})
            client.get(key)

        load = di.Load(LOAD_THREADS, command)
        try:
            assert di.wait_for(lambda: load.successes() > 0, l3.ROUTING_TIMEOUT_SEC), "Load did not start"
            failed_before = len(load.errors())
            begin = time.monotonic()
            cluster.scale(size - 1)

            def gone() -> bool:
                held = di.client_nodes(client)
                return victim not in held and len(held) == size - 1

            left = di.wait_for(gone, l3.ROUTING_TIMEOUT_SEC, interval=0.05)
            transition_ms = (time.monotonic() - begin) * 1000
            errors = load.errors()
            print(f"L3-SCALE-DOWN-TRANSITION node={victim} pod={removed.name} transitionMs={transition_ms:.0f} "
                  f"failedCommands={len(errors) - failed_before} succeededCommands={load.successes()} "
                  f"errors={di.kinds(errors)} nodes={sorted(di.client_nodes(client))}")
            assert left, f"{victim} still held: {sorted(di.client_nodes(client))}"

            pods = cluster.pods()
            for name in di.client_nodes(client):
                access = di.node_info(client, name)("service-clear-std")
                assert l3.pod_of(pods, access) is not None, f"{name} is no remaining pod in {pods}"
            info_by_name = l3.node_info_by_name(client)
            di.wait_cluster_stable(list(info_by_name.values()), size - 1)
            time.sleep(4 * l3.TEND_INTERVAL_MS / 1000)

            errors = client_errors(client)
            retries = client.get_stats().retry_count
            failed = len(load.errors())
            succeeded = load.successes()
            window_ms = QUIET_TEND_INTERVALS * l3.TEND_INTERVAL_MS

            def grew() -> bool:
                return (client_errors(client) != errors or client.get_stats().retry_count != retries
                        or len(load.errors()) != failed)

            storm = di.wait_for(grew, window_ms / 1000, interval=0.05)
            print(f"L3-SCALE-DOWN-QUIET windowMs={window_ms} errors={errors}->{client_errors(client)} "
                  f"retries={retries}->{client.get_stats().retry_count} failed={failed}->{len(load.errors())} "
                  f"succeeded={succeeded}->{load.successes()} errors={di.kinds(load.errors())}")
            assert not storm, f"Errors, retries or failed commands grew after {victim} left"
            assert load.successes() > succeeded, f"No command succeeded after {victim} left"
        finally:
            assert load.stop(l3.ROUTING_TIMEOUT_SEC * 1000), "Load threads blocked"
        l3.assert_served(client, SET_NAME, size=size - 1, row="L3-K-3")
    finally:
        client.close()


def assert_pinned_seed(cluster: l3.Cluster, step: str, service: str, port: int, pinned: int, size: int) -> None:
    """Assert that no pod advertises the pinned NodePort, every pod advertises its own Service
    NodePort, and a client seeded only at the endpoint hostname and the pinned NodePort holds one
    node per pod at that address, each serving the keys it is master of."""
    ready = di.wait_for(lambda: cluster.ready_endpoints(service) > 0, l3.ROUTING_TIMEOUT_SEC, interval=0.5)
    assert ready, f"{service} has no ready endpoint within {l3.ROUTING_TIMEOUT_SEC} s"

    client = l3.connect_when_ready(seed=(l3.endpoint(), pinned))
    try:
        held = l3.wait_for_size(client, size)
        pods = cluster.pods()
        for name, entry in held.items():
            info = di.node_info(client, name)
            pod = l3.pod_of(pods, info("service-clear-std"))
            assert pod is not None, f"{name} is no pod in {pods}"
            advertised = info("service-clear-alt")
            node_port = cluster.node_port(pod.name, port)
            print(f"L3-PINNED-NODE step={step} node={name} pod={pod.name} alternate={advertised} "
                  f"serviceNodePort={node_port} pinned={pinned} client={entry}")
            assert advertised == f"{l3.endpoint()}:{node_port}", f"{name} ({pod.name}) advertised alternate address"
            assert node_port != pinned, f"{pod.name} advertises the pinned NodePort of {service}"
            assert entry["port"] == node_port
        l3.assert_served(client, SET_NAME, size, row=f"L3-K-9-{step}")
    finally:
        client.close()


def wait_until(condition: Callable[[], bool], description: str) -> None:
    assert di.wait_for(condition, l3.SCALE_TIMEOUT_SEC, interval=2), (
        f"{description} not within {l3.SCALE_TIMEOUT_SEC} s"
    )


def assert_rescheduled_pod_keeps_service(cluster: l3.Cluster, pod: str, port: int, size: int) -> None:
    """Delete the pod, wait for its StatefulSet to start a new pod with the same name, and assert the
    pod's Service is the same object with the same NodePort."""
    pod_uid = cluster.uid("pod", pod)
    service_uid = cluster.uid("service", pod)
    node_port = cluster.node_port(pod, port)
    cluster.delete_pod(pod)

    def rescheduled() -> bool:
        uid = cluster.uid("pod", pod)
        if uid in ("", pod_uid):
            return False
        return any(found.name == pod and found.phase == "Running" for found in cluster.pods())

    wait_until(rescheduled, f"{pod} Running again")
    cluster.scale(size)
    new_service_uid = cluster.uid("service", pod)
    new_node_port = cluster.node_port(pod, port)
    print(f"L3-POD-RESCHEDULED pod={pod} podUid={pod_uid}->{cluster.uid('pod', pod)} "
          f"serviceUid={service_uid}->{new_service_uid} nodePort={node_port}->{new_node_port}")
    assert new_service_uid == service_uid, f"{pod} Service UID"
    assert new_node_port == node_port, f"{pod} Service NodePort"


def assert_recreated_pod_gets_new_service(
    cluster: l3.Cluster, pod: str, port: int, size: int, services: list[str]
) -> None:
    """Scale the pod away, which deletes its Service, and back, and assert the operator creates a
    new Service for it with a new NodePort. A placeholder Service holds the old NodePort meanwhile,
    since Kubernetes would otherwise be free to hand the same NodePort back."""
    service_uid = cluster.uid("service", pod)
    node_port = cluster.node_port(pod, port)
    cluster.scale(size - 1)
    wait_until(lambda: cluster.uid("service", pod) == "", f"{pod} Service deleted")
    placeholder = f"{cluster.name}-held-node-port"
    services.append(placeholder)
    cluster.create_pod_node_port_service(placeholder, pod=pod, port=port, node_port=node_port)
    cluster.scale(size)

    new_service_uid = cluster.uid("service", pod)
    new_node_port = cluster.node_port(pod, port)
    print(f"L3-POD-RECREATED pod={pod} serviceUid={service_uid}->(deleted)->{new_service_uid} "
          f"oldNodePort={node_port} newNodePort={new_node_port}")
    assert new_service_uid not in ("", service_uid), f"{pod} Service not recreated"
    assert new_node_port not in (0, node_port), f"{pod} Service NodePort {node_port}->{new_node_port}"


# K-9: a pod's Service keeps its UID and NodePort when the pod is rescheduled, and is recreated
# with a new NodePort when the pod is removed and added again. A separate NodePort Service pinned
# to pod 0 keeps its NodePort through both, no pod advertises it, and a client seeded only through
# it reaches every node.
@pytest.mark.l3
def test_k9_node_ports_stable_across_pod_reschedule_and_recreate(
    cluster: l3.Cluster, services: list[str]
) -> None:
    size = max(MIN_PODS, cluster.size())
    cluster.scale(size)
    port = cluster.service_port()
    first = min(cluster.pods(), key=lambda pod: l3.ordinal(pod.name))
    assert l3.ordinal(first.name) == 0, f"{first.name} ordinal"

    low, high = cluster.node_port_range()
    allocated = l3.allocated_node_ports()
    pinned = next((candidate for candidate in range(high, low - 1, -1) if candidate not in allocated), 0)
    assert pinned, f"No free NodePort in {low}-{high}"
    service = f"{cluster.name}-pinned-seed"
    services.append(service)
    cluster.create_pod_node_port_service(service, pod=first.name, port=port, node_port=pinned)
    print(f"L3-PINNED service={service} pod={first.name} nodePort={pinned} range={low}-{high} "
          f"allocated={len(allocated)}")
    assert cluster.node_port(service, port) == pinned
    assert_pinned_seed(cluster, step="created", service=service, port=port, pinned=pinned, size=size)

    assert_rescheduled_pod_keeps_service(cluster, pod=first.name, port=port, size=size)
    assert cluster.node_port(service, port) == pinned
    assert_pinned_seed(cluster, step="rescheduled", service=service, port=port, pinned=pinned, size=size)

    last = highest_ordinal(cluster.pods())
    assert_recreated_pod_gets_new_service(cluster, pod=last.name, port=port, size=size, services=services)
    assert cluster.node_port(service, port) == pinned
    assert_pinned_seed(cluster, step="recreated", service=service, port=port, pinned=pinned, size=size)
