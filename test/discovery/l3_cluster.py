"""Helpers for the L3 discovery tests against the kind and AKO cluster that the setup-kind-ako action
builds, given as environment variables:

- DISCOVERY_KUBECONFIG: the kind cluster's kubeconfig
- DISCOVERY_ENDPOINT_HOSTNAME: the endpoint hostname, which resolves on the runner to the kind node
- DISCOVERY_INGRESS_PORTS: the ingress-nginx tcp-services ports, the first one reaching pod 0
- AEROSPIKE_ADMIN_PASSWORD: the admin password, since the cluster has security enabled
- DISCOVERY_TLS_CA_FILE and DISCOVERY_TLS_NAME: set when setup-kind-ako serves TLS, with enable-tls

The AerospikeCluster, its pods and their Services are read from the API server through kubectl.
"""

import base64
import os
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import aerospike
import discovery_info as di

NAMESPACE = "test"
TEND_INTERVAL_MS = 250
KUBECTL_TIMEOUT_SEC = 120
SCALE_TIMEOUT_SEC = 900
ROUTING_TIMEOUT_SEC = 120


@dataclass
class Pod:
    name: str
    node: str
    ip: str
    phase: str


def run_kubectl(*args: str) -> subprocess.CompletedProcess:
    command = ["kubectl", "--request-timeout=30s", *args]
    env = {**os.environ, "KUBECONFIG": os.environ["DISCOVERY_KUBECONFIG"]}
    return subprocess.run(
        command, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=KUBECTL_TIMEOUT_SEC
    )


def kubectl(*args: str) -> str:
    """Run kubectl and return its standard output, failing the test if it exits non-zero."""
    result = run_kubectl(*args)
    assert result.returncode == 0, f"kubectl {' '.join(args)} exited {result.returncode}: {result.stderr.strip()}"
    return result.stdout


def kubectl_error(*args: str) -> str:
    """Run kubectl, expecting it to fail, and return its standard error."""
    result = run_kubectl(*args)
    assert result.returncode != 0, f"kubectl {' '.join(args)} succeeded"
    return result.stderr.strip()


def lines(output: str) -> list[str]:
    return [line.strip() for line in output.splitlines() if line.strip()]


def ordinal(pod: str) -> int:
    """Return the StatefulSet ordinal at the end of a pod's name."""
    return int(pod.rpartition("-")[2])


class Cluster:
    """The one AerospikeCluster in the kind cluster."""

    def __init__(self) -> None:
        found = lines(kubectl(
            "get", "aerospikecluster", "--all-namespaces", "-o",
            'jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{"\\n"}{end}',
        ))
        assert len(found) == 1, f"AerospikeClusters {found}"
        self.namespace, self.name = found[0].split(" ")

    def get(self, jsonpath: str) -> str:
        reply = kubectl("-n", self.namespace, "get", "aerospikecluster", self.name, "-o", f"jsonpath={{{jsonpath}}}")
        return reply.strip()

    def size(self) -> int:
        return int(self.get(".spec.size"))

    def service_port(self) -> int:
        return int(self.get(".spec.aerospikeConfig.network.service.port"))

    def pods(self) -> list[Pod]:
        found = []
        for line in lines(kubectl(
            "-n", self.namespace, "get", "pods", "-l", f"aerospike.com/cr={self.name}", "-o",
            'jsonpath={range .items[*]}{.metadata.name} {.spec.nodeName} {.status.podIP} {.status.phase}{"\\n"}{end}',
        )):
            name, node, ip, phase = (line.split(" ") + ["", "", ""])[:4]
            found.append(Pod(name, node, ip, phase))
        return sorted(found, key=lambda pod: pod.name)

    def ready_pods(self) -> set[str]:
        """Return the names of the pods whose Ready condition is True."""
        ready = set()
        for line in lines(kubectl(
            "-n", self.namespace, "get", "pods", "-l", f"aerospike.com/cr={self.name}", "-o",
            'jsonpath={range .items[*]}{.metadata.name} {.status.conditions[?(@.type=="Ready")].status}{"\\n"}{end}',
        )):
            name, _, status = line.partition(" ")
            if status == "True":
                ready.add(name)
        return ready

    def node_port(self, service: str, port: int) -> int:
        """Return a Service's NodePort for port, or 0 when it has none. Each pod's Service has the
        pod's name."""
        value = kubectl(
            "-n", self.namespace, "get", "service", service, "--ignore-not-found", "-o",
            f"jsonpath={{.spec.ports[?(@.port=={port})].nodePort}}",
        ).strip()
        if not value:
            return 0
        return int(value)

    def uid(self, kind: str, name: str) -> str:
        """Return an object's UID, or an empty string when it does not exist."""
        return kubectl(
            "-n", self.namespace, "get", kind, name, "--ignore-not-found", "-o", "jsonpath={.metadata.uid}"
        ).strip()

    def ready_endpoints(self, service: str) -> int:
        """Return the number of ready endpoints in a Service's EndpointSlices."""
        ready = kubectl(
            "-n", self.namespace, "get", "endpointslices", "-l", f"kubernetes.io/service-name={service}", "-o",
            "jsonpath={range .items[*].endpoints[*]}{.conditions.ready} {end}",
        )
        return ready.split().count("true")

    def delete_pod(self, pod: str) -> None:
        kubectl("-n", self.namespace, "delete", "pod", pod, "--wait=true")

    def create_pod_node_port_service(self, service: str, pod: str, port: int, node_port: int) -> None:
        """Create a NodePort Service that sends port on node_port to one pod."""
        kubectl("-n", self.namespace, "create", "service", "nodeport", service, f"--tcp={port}:{port}",
                f"--node-port={node_port}")
        kubectl("-n", self.namespace, "set", "selector", "service", service,
                f"statefulset.kubernetes.io/pod-name={pod}")

    def delete_service(self, service: str) -> None:
        kubectl("-n", self.namespace, "delete", "service", service, "--ignore-not-found", "--wait=true")

    def node_port_range(self) -> tuple[int, int]:
        """Return the API server's NodePort range, read from the error it returns for a Service whose
        NodePort is outside it."""
        probe = f"{self.name}-node-port-range"
        assert self.uid("service", probe) == "", f"Service {probe} exists"
        try:
            error = kubectl_error("-n", self.namespace, "create", "service", "nodeport", probe, "--tcp=1:1",
                                  "--node-port=1")
        finally:
            self.delete_service(probe)
        match = re.search(r"range of valid ports is (\d+)-(\d+)", error)
        assert match, f"No NodePort range in: {error}"
        return int(match.group(1)), int(match.group(2))

    def mounted_certificate(self, source: str, key: str) -> bytes:
        """Return the PEM certificate stored under key in the Kubernetes Secret named source."""
        escaped = key.replace(".", "\\.")
        value = kubectl("-n", self.namespace, "get", "secret", source, "-o", f"jsonpath={{.data.{escaped}}}").strip()
        return base64.b64decode(value)

    def resize(self, size: int) -> None:
        """Set the AerospikeCluster size without waiting for the operator."""
        kubectl("-n", self.namespace, "patch", "aerospikecluster", self.name, "--type", "merge",
                "-p", f'{{"spec":{{"size":{size}}}}}')

    def scale(self, size: int) -> None:
        """Set the AerospikeCluster size and wait until the operator reports it Completed at that size
        with that many running pods."""
        self.resize(size)

        def scaled() -> bool:
            if self.get(".status.size") != str(size) or self.get(".status.phase") != "Completed":
                return False
            pods = self.pods()
            return len(pods) == size and all(pod.phase == "Running" for pod in pods)

        done = di.wait_for(scaled, SCALE_TIMEOUT_SEC, interval=2)
        print(f"L3-SCALED cluster={self.namespace}/{self.name} size={size} done={bool(done)} pods={self.pods()}")
        assert done, f"AerospikeCluster {self.name} not Completed at size {size} within {SCALE_TIMEOUT_SEC} s"


def allocated_node_ports() -> set[int]:
    """Return the NodePorts every Service in the kind cluster holds."""
    output = kubectl("get", "service", "--all-namespaces", "-o",
                     "jsonpath={range .items[*].spec.ports[*]}{.nodePort},{end}")
    return {int(port) for port in output.split(",") if port.strip()}


def endpoint() -> str:
    return os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]


def seed() -> tuple[str, int]:
    """Return the endpoint hostname with the first ingress port, which reaches pod 0."""
    ports = [int(port) for port in os.environ["DISCOVERY_INGRESS_PORTS"].split(",") if port]
    return endpoint(), ports[0]


def connect(seed: tuple, **options: Any) -> aerospike.Client:
    """Connect through seed, a (hostname, port) or (hostname, port, tls-name) tuple, reading the
    alternate list with no translator."""
    config = {
        "hosts": [seed],
        "user": "admin",
        "password": os.environ["AEROSPIKE_ADMIN_PASSWORD"],
        "use_services_alternate": True,
        "tend_interval": TEND_INTERVAL_MS,
        "connect_timeout": 5000,
        **options,
    }
    return aerospike.client(config)


def connect_when_ready(seed: tuple, **options: Any) -> aerospike.Client:
    """Connect through seed, retrying while the seed does not accept connections yet."""
    deadline = time.monotonic() + ROUTING_TIMEOUT_SEC
    while True:
        try:
            return connect(seed, **options)
        except aerospike.exception.AerospikeError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def accepts_login(seed: tuple) -> bool:
    """Return whether a client can log in through seed. A node that just joined rejects the
    credentials until it has the cluster's users."""
    try:
        client = connect(seed, connect_timeout=1000)
    except aerospike.exception.AerospikeError:
        return False
    client.close()
    return True


def wait_for_size(client: aerospike.Client, size: int, tend_interval_ms: int = TEND_INTERVAL_MS) -> dict:
    """Wait until the client holds size nodes, give it the tends that fetch their partitions, and
    return its get_node_names() entries by node name."""
    held = di.wait_for(lambda: len(client.get_node_names()) == size, ROUTING_TIMEOUT_SEC, interval=0.05)
    assert held, f"Client holds {sorted(di.client_nodes(client))}, expected {size} nodes"
    time.sleep(4 * tend_interval_ms / 1000)
    return di.client_nodes(client)


def node_info_by_name(client: aerospike.Client) -> dict[str, Callable[[str], str]]:
    """Return a di.node_info function for each node the client holds, by node name."""
    return {name: di.node_info(client, name) for name in di.client_nodes(client)}


def assert_served(client: aerospike.Client, set_name: str, size: int, row: str,
                  tend_interval_ms: int = TEND_INTERVAL_MS) -> None:
    """Wait until the nodes the client holds report a stable cluster of size nodes, give the client
    the tends that fetch their partitions, and assert every node serves the keys it is master of."""
    info_by_name = node_info_by_name(client)
    di.wait_cluster_stable(list(info_by_name.values()), size)
    time.sleep(4 * tend_interval_ms / 1000)
    di.assert_served_by_owners(client, NAMESPACE, set_name, row, info_by_name)


def has_address(access: str, ip: str) -> bool:
    """Return whether a service-*-std reply lists ip."""
    return any(di.split_endpoint(endpoint)[0] == ip for endpoint in di.endpoints(access))


def pod_of(pods: list[Pod], access: str) -> Optional[Pod]:
    """Return the pod whose IP a node lists in its service-*-std reply, or None."""
    for pod in pods:
        if has_address(access, pod.ip):
            return pod
    return None
