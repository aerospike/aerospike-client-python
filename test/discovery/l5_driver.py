"""Driver for the l5-aws harness in aerospike-client-conformance (l5-aws/DRIVER.md there, contract version 1).

Builds a client from the flags, runs a retrying load, and writes what the client sees to stdout as JSON
lines while it answers the commands it reads from stdin. It measures and reports; the harness asserts.

The client exposes neither its partition map nor a node's active flag, so snapshots leave out
unowned, owners and longestUnownedMs, a node is usable once the client holds it, and every node the
client holds is reported active: the C client deactivates a node in the same tend that drops it.
"""

import argparse
import importlib.metadata
import json
import os
import socket
import sys
import threading
import time
from typing import Any, Callable, Optional, TextIO, Union

import aerospike
from aerospike import exception as e

CONTRACT = 1
PARTITIONS = 4096
POLL_MS = 10
HEARTBEAT_MS = 100
REACH_INTERVAL_MS = 500
REACH_TIMEOUT_MS = 1000
PROBE_INTERVAL_MS = 10
PROBE_CONNECT_MS = 50
WATCH_BOUND_MS = 60000
QUIT_MS = 30000
KEY_SEARCH_LIMIT = 1000000
LOG_LEVELS = {aerospike.LOG_LEVEL_ERROR: "ERROR", aerospike.LOG_LEVEL_WARN: "WARN"}
ON_MISS = {"strict": "strict", "pass-through": "pass_through"}

START_NS = time.monotonic_ns()


def now() -> int:
    """Milliseconds on the monotonic clock since the driver started."""
    return (time.monotonic_ns() - START_NS) // 1000000


def error_message(error: BaseException) -> str:
    message = getattr(error, "msg", None) or str(error)
    return f"{type(error).__name__}: {message}"


def every(interval_ms: int, stop: threading.Event, step: Callable[[], None]) -> None:
    """Call step every interval_ms, at a fixed rate, until stop is set."""
    interval = interval_ms / 1000
    deadline = time.monotonic()
    while not stop.is_set():
        step()
        deadline += interval
        delay = deadline - time.monotonic()
        if delay < 0:
            deadline = time.monotonic()
            delay = 0
        stop.wait(delay)


class Output:
    """Stdout, one JSON object per line, each flushed. Lines written before ready are held and follow it."""

    def __init__(self, stream: TextIO) -> None:
        self.stream = stream
        self.lock = threading.Lock()
        self.held = []

    def write(self, message: dict[str, Any]) -> None:
        line = json.dumps(message, separators=(",", ":"))
        with self.lock:
            if self.held is None:
                self.print(line)
            else:
                self.held.append(line)

    def write_now(self, message: dict[str, Any]) -> None:
        line = json.dumps(message, separators=(",", ":"))
        with self.lock:
            self.print(line)

    def open(self, ready: dict[str, Any]) -> None:
        line = json.dumps(ready, separators=(",", ":"))
        with self.lock:
            self.print(line)
            for held in self.held:
                self.print(held)
            self.held = None

    def print(self, line: str) -> None:
        self.stream.write(line + "\n")
        self.stream.flush()


def host_port(value: str) -> tuple[str, int]:
    """Parse HOST:PORT, the host optionally in brackets."""
    host, colon, port = value.rpartition(":")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    if not colon or not host or not port.isdigit() or not 0 < int(port) <= 65535:
        raise argparse.ArgumentTypeError(f"must be HOST:PORT: {value}")
    return host, int(port)


def positive(value: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer: {value}")
    return int(value)


def port_map(value: str) -> dict[str, tuple[str, int]]:
    mapping = {}
    for entry in value.split(","):
        node, equals, address = entry.partition("=")
        if not node or not equals:
            raise argparse.ArgumentTypeError(f"entry must be NODE=HOST:PORT: {entry}")
        if node in mapping:
            raise argparse.ArgumentTypeError(f"names {node} twice")
        mapping[node] = host_port(address)
    return mapping


def parse_options(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="l5_driver.py", allow_abbrev=False)
    parser.add_argument("--seed", required=True, type=host_port)
    parser.add_argument("--shape", required=True, choices=("pass-through", "port-map"))
    parser.add_argument("--port-map", type=port_map)
    parser.add_argument("--on-miss", choices=("strict", "pass-through"), default="strict")
    parser.add_argument("--user")
    parser.add_argument("--tend-interval", required=True, type=positive)
    parser.add_argument("--timeout", required=True, type=positive)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--set", required=True)
    parser.add_argument("--workers", type=positive, default=4)
    parser.add_argument("--keys", type=positive, default=1000)
    options = parser.parse_args(argv)

    if options.shape == "port-map" and options.port_map is None:
        parser.error("--shape port-map needs --port-map")
    if options.shape != "port-map" and options.port_map is not None:
        parser.error("--port-map only with --shape port-map")
    options.password = os.environ.get("AEROSPIKE_ADMIN_PASSWORD")
    if options.user is not None and options.password is None:
        parser.error("--user needs AEROSPIKE_ADMIN_PASSWORD in the environment")
    return options


def command_policy(options: argparse.Namespace) -> dict[str, int]:
    return {"socket_timeout": options.timeout, "total_timeout": options.timeout, "max_retries": 0}


def endpoint(host: str, port: int) -> str:
    if ":" in host:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def translation_config(options: argparse.Namespace) -> dict[str, Any]:
    translation = {"provider": options.shape, "on_miss": ON_MISS[options.on_miss]}
    if options.port_map is not None:
        translation["port_map"] = {node: endpoint(host, port) for node, (host, port) in options.port_map.items()}
    return translation


def client_config(options: argparse.Namespace) -> dict[str, Any]:
    config = {
        "hosts": [options.seed],
        "use_services_alternate": options.shape == "pass-through",
        "translation": translation_config(options),
        "tend_interval": options.tend_interval,
        "connect_timeout": options.timeout,
        "policies": {"read": command_policy(options), "write": command_policy(options)},
    }
    if options.user is not None:
        config["user"] = options.user
        config["password"] = options.password
    return config


class Load:
    """Each worker runs one command at a time, and issues a failed command again after one tend interval
    until it succeeds."""

    def __init__(self, client: aerospike.Client, options: argparse.Namespace,
                 on_first_success: Callable[[], None]) -> None:
        self.client = client
        self.options = options
        self.on_first_success = on_first_success
        self.policy = command_policy(options)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.workers = []
        self.started = False
        self.succeeded = 0
        self.failed = 0
        self.last_error = None
        self.reset_at_ns = START_NS
        self.max_latency_ns = 0

    def start(self) -> None:
        for worker in range(self.options.workers):
            thread = threading.Thread(target=self.run, args=(worker,), name=f"l5-load-{worker}", daemon=True)
            self.workers.append(thread)
            thread.start()

    def run(self, worker: int) -> None:
        i = 0
        while not self.stop_event.is_set():
            k = (worker + i * self.options.workers) % self.options.keys
            key = (self.options.namespace, self.options.set, f"load-{k}")
            first_ns = time.monotonic_ns()
            while True:
                try:
                    if i % 2 == 0:
                        self.client.put(key, {"v": i}, policy=self.policy)
                    else:
                        self.client.get(key, policy=self.policy)
                    break
                except e.RecordNotFound:
                    break
                except e.AerospikeError as error:
                    self.record_failure(error)
                if self.stop_event.wait(self.options.tend_interval / 1000):
                    return
            end_ns = time.monotonic_ns()
            self.record_success(first_ns, end_ns)
            i += 1

    def record_failure(self, error: e.AerospikeError) -> None:
        message = error_message(error)
        with self.lock:
            self.failed += 1
            self.last_error = message

    def record_success(self, first_ns: int, end_ns: int) -> None:
        with self.lock:
            self.succeeded += 1
            if first_ns >= self.reset_at_ns:
                self.max_latency_ns = max(self.max_latency_ns, end_ns - first_ns)
            first = not self.started
            self.started = True
        if first:
            self.on_first_success()

    def reset(self) -> None:
        with self.lock:
            self.reset_at_ns = time.monotonic_ns()
            self.max_latency_ns = 0

    def counters(self) -> dict[str, Optional[Union[int, str]]]:
        with self.lock:
            return {
                "succeeded": self.succeeded,
                "failed": self.failed,
                "maxLatencyMs": self.max_latency_ns // 1000000,
                "lastError": self.last_error,
            }

    def stop(self, deadline: float) -> None:
        """Let each worker finish its current attempt, giving up at deadline."""
        self.stop_event.set()
        for thread in self.workers:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                thread.join(remaining)


class Watch:
    """Probe HOST:PORT every PROBE_INTERVAL_MS until it accepts a connection, then check every
    PROBE_INTERVAL_MS whether the node is usable, and reply."""

    def __init__(self, driver: "Driver", address: str, host: str, port: int, node: str) -> None:
        self.driver = driver
        self.address = address
        self.host = host
        self.port = port
        self.node = node
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=every, args=(PROBE_INTERVAL_MS, self.stop_event, self.tick),
                                       name="l5-watch", daemon=True)
        self.serving_at = None
        self.last_closed_at = None

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread.is_alive() and self.thread is not threading.current_thread():
            self.thread.join(1)

    def tick(self) -> None:
        try:
            if self.serving_at is None:
                t = now()
                if not self.connects():
                    self.last_closed_at = t
                    return
                self.serving_at = t
            t = now()
            if self.driver.usable(self.node):
                self.reply(t)
            elif t - self.serving_at >= WATCH_BOUND_MS:
                self.reply(None)
        except Exception as error:
            self.driver.error(f"watch {self.address} {self.node}: {error_message(error)}")
            self.finish()

    def connects(self) -> bool:
        """Connect to the first address HOST resolves to, as a new listener serves on one endpoint address
        before the others."""
        try:
            addresses = socket.getaddrinfo(self.host, self.port, type=socket.SOCK_STREAM)
        except OSError:
            return False
        family, kind, proto, _, address = addresses[0]
        with socket.socket(family, kind, proto) as probe:
            probe.settimeout(PROBE_CONNECT_MS / 1000)
            try:
                probe.connect(address)
            except OSError:
                return False
        return True

    def reply(self, usable_at: Optional[int]) -> None:
        usable_ms = None
        if usable_at is not None:
            usable_ms = usable_at - self.serving_at
        self.driver.out.write({
            "type": "watch",
            "t": now(),
            "address": self.address,
            "node": self.node,
            "servingAt": self.serving_at,
            "lastClosedAt": self.last_closed_at,
            "usableAt": usable_at,
            "usableMs": usable_ms,
            "ownershipChecked": False,
        })
        self.finish()

    def finish(self) -> None:
        self.stop_event.set()
        self.driver.end_watch(self)


class Driver:
    def __init__(self, options: argparse.Namespace, out: Output, client: aerospike.Client) -> None:
        self.options = options
        self.out = out
        self.client = client
        self.load = Load(client, options, self.ready)
        self.stop_event = threading.Event()
        self.routing = threading.Thread(target=every, args=(POLL_MS, self.stop_event, self.poll), name="l5-routing",
                                        daemon=True)
        self.reach = threading.Thread(target=every, args=(REACH_INTERVAL_MS, self.stop_event, self.check_reachable),
                                      name="l5-reach", daemon=True)
        self.lock = threading.Lock()
        self.reachable = {}
        self.checks = {}
        self.watch = None
        self.last_signature = None
        self.next_heartbeat = 0

    def run(self) -> None:
        self.reach.start()
        self.load.start()
        for line in sys.stdin:
            if not self.command(line):
                break
        self.quit()

    def ready(self) -> None:
        self.out.open({
            "type": "ready",
            "t": now(),
            "contract": CONTRACT,
            "epochMs": time.time_ns() // 1000000,
            "client": "python",
            "clientVersion": importlib.metadata.version("aerospike"),
            "shape": self.options.shape,
        })
        self.routing.start()

    def command(self, line: str) -> bool:
        """Run one command line and return False on quit."""
        words = line.split()
        name = ""
        if words:
            name = words[0]

        if name == "serve" and len(words) == 2:
            self.serve(words[1])
        elif name == "reset" and len(words) == 1:
            self.load.reset()
            self.out.write({"type": "reset", "t": now()})
        elif name == "watch" and len(words) == 3:
            self.start_watch(words[1], words[2])
        elif name == "quit" and len(words) == 1:
            return False
        elif name in ("serve", "reset", "watch", "quit"):
            self.error(f"malformed command: {line.strip()}")
        else:
            self.error(f"unknown command: {line.strip()}")
        return True

    def error(self, message: str) -> None:
        self.out.write({"type": "error", "t": now(), "message": message})

    def quit(self) -> None:
        """Stop the load within QUIT_MS, then close the client once nothing else uses it."""
        deadline = time.monotonic() + QUIT_MS / 1000
        self.load.stop(deadline)
        with self.lock:
            watch = self.watch
        if watch is not None:
            watch.stop()
        self.stop_event.set()
        for thread in (self.routing, self.reach):
            if thread.is_alive():
                thread.join(1)
        with self.lock:
            checks = list(self.checks.values())
        for thread in checks:
            thread.join(2 * REACH_TIMEOUT_MS / 1000)
        self.client.close()

    def start_watch(self, address: str, node: str) -> None:
        try:
            host, port = host_port(address)
        except argparse.ArgumentTypeError:
            self.error(f"malformed command: watch {address} {node}")
            return
        watch = Watch(self, address, host, port, node)
        with self.lock:
            running = self.watch
            if running is None:
                self.watch = watch
        if running is not None:
            self.error(f"a watch is already running: watch {address} {node}")
            return
        watch.start()

    def end_watch(self, watch: Watch) -> None:
        with self.lock:
            if self.watch is watch:
                self.watch = None

    def nodes(self) -> list[tuple[str, str, int]]:
        """Every node the client holds as (name, address, port), sorted by name."""
        try:
            nodes = self.client.get_node_names()
        except e.ClusterError:
            return []
        held = [(node["node_name"], node["address"], node["port"]) for node in nodes]
        return sorted(held)

    def usable(self, name: str) -> bool:
        held = self.nodes()
        return any(node[0] == name for node in held)

    def poll(self) -> None:
        """Write a change snapshot when the routing differs from the last poll, and a heartbeat every
        HEARTBEAT_MS."""
        try:
            t = now()
            nodes = self.nodes()
            if self.last_signature is None:
                self.next_heartbeat = t
            elif nodes != self.last_signature:
                self.snapshot("change", t, nodes)
            self.last_signature = nodes

            if t >= self.next_heartbeat:
                self.snapshot("heartbeat", t, nodes)
                self.next_heartbeat = max(self.next_heartbeat + HEARTBEAT_MS, t + 1)
        except Exception as error:
            print(f"routing poll: {error_message(error)}", file=sys.stderr)

    def snapshot(self, reason: str, t: int, nodes: list[tuple[str, str, int]]) -> None:
        with self.lock:
            reachable = dict(self.reachable)
        listed = []
        for name, address, port in nodes:
            listed.append({
                "name": name,
                "host": address,
                "port": port,
                "address": address,
                "active": True,
                "reachable": reachable.get((name, address, port)),
            })
        snapshot = {"type": "snapshot", "t": t, "reason": reason, "nodes": listed}
        snapshot.update(self.client_counts(nodes))
        snapshot.update(self.load.counters())
        self.out.write(snapshot)

    def client_counts(self, nodes: list[tuple[str, str, int]]) -> dict[str, int]:
        """The client's error and timeout counts, summed over the nodes it holds."""
        names = {node[0] for node in nodes}
        stats = self.client.get_stats()
        errors = 0
        timeouts = 0
        for node in stats.nodes:
            if node.name in names:
                errors += node.error_count
                timeouts += node.timeout_count
        return {"clientErrors": errors, "clientTimeouts": timeouts}

    def check_reachable(self) -> None:
        """Start a reachability check for each node that has none running, each on its own thread."""
        nodes = self.nodes()
        with self.lock:
            for key in list(self.reachable):
                if key not in nodes:
                    del self.reachable[key]
            for node in nodes:
                if node in self.checks:
                    continue
                thread = threading.Thread(target=self.check_one, args=(node,), name="l5-reach-check", daemon=True)
                self.checks[node] = thread
                thread.start()

    def check_one(self, node: tuple[str, str, int]) -> None:
        """Record whether the node answers an info request for its name within REACH_TIMEOUT_MS."""
        name = node[0]
        try:
            reply = self.client.info_single_node("node", name, {"timeout": REACH_TIMEOUT_MS})
            value = reply.partition("\t")[2].strip()
            answered = value == name
        except e.AerospikeError:
            answered = False
        with self.lock:
            self.reachable[node] = answered
            del self.checks[node]

    def serve(self, step: str) -> None:
        """Write and read one key in every partition: the first of STEP-0, STEP-1, ... in each."""
        keys = [None] * PARTITIONS
        found = 0
        for i in range(KEY_SEARCH_LIMIT):
            if found == PARTITIONS:
                break
            key = f"{step}-{i}"
            partition = self.client.get_key_partition_id(self.options.namespace, self.options.set, key)
            if keys[partition] is None:
                keys[partition] = (self.options.namespace, self.options.set, key)
                found += 1

        start_ns = time.monotonic_ns()
        policy = command_policy(self.options)
        written = 0
        read = 0
        mismatched = 0
        failures = 0
        first_failure = None
        for partition, key in enumerate(keys):
            failure = None
            if key is None:
                failure = f"partition {partition}: no key in {KEY_SEARCH_LIMIT} candidates"
            value = f"{step}-{partition}"
            if failure is None:
                try:
                    self.client.put(key, {"v": value}, policy=policy)
                    written += 1
                except e.AerospikeError as error:
                    failure = f"partition {partition} put: {error_message(error)}"
            if failure is None:
                try:
                    _, _, bins = self.client.get(key, policy=policy)
                    if bins.get("v") == value:
                        read += 1
                    else:
                        mismatched += 1
                except e.RecordNotFound:
                    mismatched += 1
                except e.AerospikeError as error:
                    failure = f"partition {partition} get: {error_message(error)}"
            if failure is not None:
                failures += 1
                if first_failure is None:
                    first_failure = failure

        elapsed_ms = (time.monotonic_ns() - start_ns) // 1000000
        self.out.write({
            "type": "served",
            "t": now(),
            "step": step,
            "partitions": PARTITIONS,
            "written": written,
            "read": read,
            "mismatched": mismatched,
            "failures": failures,
            "firstFailure": first_failure,
            "elapsedMs": elapsed_ms,
        })


def main(argv: list[str]) -> int:
    options = parse_options(argv)
    out = Output(sys.stdout)
    sys.stdout = sys.stderr

    def log(level: int, function: str, path: str, line: int, message: str) -> None:
        level_name = LOG_LEVELS.get(level, str(level))
        out.write({"type": "log", "t": now(), "level": level_name, "message": message})

    aerospike.set_log_handler(log)
    aerospike.set_log_level(aerospike.LOG_LEVEL_WARN)

    config = client_config(options)
    try:
        client = aerospike.client(config)
    except e.AerospikeError as error:
        message = error_message(error)
        return client_not_created(out, message)

    Driver(options, out, client).run()
    return 0


def client_not_created(out: Output, message: str) -> int:
    out.write_now({"type": "error", "t": now(), "message": f"client not created: {message}"})
    print(f"ERROR: client not created: {message}", file=sys.stderr)
    return 3


if __name__ == "__main__":
    status = main(sys.argv[1:])
    sys.exit(status)
