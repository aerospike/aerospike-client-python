"""L2c fault-injection tests against the default, idle-reset, low-mtu and port-range profiles of the
discovery-l2-topology action.

Every profile here is the default topology: each node advertises its private address in the
standard list, the client has no route to it, and the port-map translator maps it to the node's
own Toxiproxy listener. Each profile adds one fault on that path. In idle-reset the test makes the
listeners reset idle flows with RST, as an NLB does. In low-mtu the client network has a small MTU.
In port-range Toxiproxy has few source ports for its connections to the nodes. In the default
profile the test makes one listener hold data with no reply and no close. The client is the sync
client, so these are the sync variants of the rows.
"""

import concurrent.futures
import os
import random
import socket
import time
from pathlib import Path
from typing import Any, Callable, Iterator

import pytest

import aerospike
import discovery_info as di

NAMESPACE = "test"
SET_NAME = "l2-faults"
TEND_INTERVAL_MS = 250
JOIN_TIMEOUT_SEC = 30
WAIT_SEC = 30
POLL_SEC = 0.01
KEY_SEARCH_LIMIT = 100000

PROBE_TIMEOUT_SEC = 5
F1_WARM_THREADS = 4
F1_WARM_ITERATIONS = 50
F1_KEY_COUNT = 100

F2_RECORD_SIZES = (64 * 1024, 1024 * 1024)
F2_POLICY = {"socket_timeout": 5000, "total_timeout": 10000}

F3_KEY_COUNT = 1000
F3_RECOVERY_KEY_COUNT = 100
F3_POLICY = {"socket_timeout": 2000, "total_timeout": 4000}

F4_TOXIC = "f4-hold"
F4_THREADS = 4
F4_KEY_COUNT = 20
# A timeout counts toward max_error_rate only when the client closes the connection, and a nonzero
# timeout_delay hands the connection to recovery instead
F4_POLICY = {"socket_timeout": 500, "total_timeout": 1500, "max_retries": 2, "timeout_delay": 0}
F4_BOUND_MS = F4_POLICY["total_timeout"] + F4_POLICY["socket_timeout"]
# The client resets both to their defaults when max_error_rate / error_rate_window is below 1
F4_MAX_ERROR_RATE = 4
F4_ERROR_RATE_WINDOW = 4


@pytest.fixture(scope="module")
def nodes() -> list[di.Node]:
    return di.listener_nodes("service-clear-std")


@pytest.fixture
def toxics() -> Iterator[list[tuple[str, str]]]:
    """Collect the (listener, toxic) pairs a test adds, and remove them afterwards."""
    added = []
    yield added
    for proxy, name in added:
        di.remove_toxic(proxy, name)


def add_toxic(
    toxics: list[tuple[str, str]], proxy: str, name: str, toxic_type: str, toxicity: float, attributes: dict
) -> None:
    di.add_toxic(proxy, name, toxic_type, toxicity, attributes)
    toxics.append((proxy, name))


def remove_toxic(toxics: list[tuple[str, str]], proxy: str, name: str) -> None:
    toxics.remove((proxy, name))
    di.remove_toxic(proxy, name)


def connect(nodes: list[di.Node], **options: Any) -> aerospike.Client:
    """Seed through the first node's listener with port-map by endpoint and wait for every node."""
    config = {
        "hosts": [(os.environ["DISCOVERY_ENDPOINT_HOSTNAME"], nodes[0].port)],
        "translation": {"provider": "port-map", "port_map": di.port_map_keyed_by_advertised(nodes)},
        "tend_interval": TEND_INTERVAL_MS,
        **options,
    }
    client = aerospike.client(config)
    try:
        di.wait_for_nodes(client, [node.name for node in nodes], JOIN_TIMEOUT_SEC, TEND_INTERVAL_MS)
    except BaseException:
        client.close()
        raise
    return client


def owners(nodes: list[di.Node]) -> dict[int, str]:
    """Return the name of each partition's master, read from the nodes."""
    found = {}
    for node in nodes:
        for pid in di.master_partitions(node.info, NAMESPACE):
            found[pid] = node.name
    return found


def keys_on(client: aerospike.Client, masters: dict[int, str], name: str, prefix: str, count: int) -> list[tuple]:
    """Return count keys whose partition's master is the named node."""
    keys = []
    for i in range(KEY_SEARCH_LIMIT):
        key = (NAMESPACE, SET_NAME, f"{prefix}-{i}")
        if masters.get(client.get_key_partition_id(*key)) == name:
            keys.append(key)
            if len(keys) == count:
                return keys
    raise AssertionError(f"{name} is master of fewer than {count} of {KEY_SEARCH_LIMIT} keys")


def format_stats(stats: dict) -> str:
    entries = []
    for name, node in sorted(stats.items()):
        conns = node.conns
        entries.append(
            f"{name}{{inUse={conns.in_use},inPool={conns.in_pool},opened={conns.opened},closed={conns.closed},"
            f"errors={node.error_count},timeouts={node.timeout_count}}}"
        )
    return " ".join(entries)


def count(outcomes: list[di.Outcome], error_type: type) -> int:
    return sum(1 for outcome in outcomes if isinstance(outcome.error, error_type))


def probe(node: di.Node) -> socket.socket:
    """Open a connection through a node's listener and complete one info request on it, so
    Toxiproxy has linked it to the node."""
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    sock = socket.create_connection((host, node.port), timeout=PROBE_TIMEOUT_SEC)
    di.send_info(sock, "node")
    return sock


def probe_result(sock: socket.socket) -> str:
    """Read the next byte of a probe connection: RST when the peer reset it, FIN when it closed it."""
    try:
        data = sock.recv(1)
    except ConnectionResetError as error:
        return f"RST {error}"
    except OSError as error:
        return f"ERROR {error!r}"
    if data:
        return f"DATA {data!r}"
    return "FIN"


# F-1: idle flows torn down with RST, as an NLB tears down an idle flow. Every listener is made to
# close its connections with RST rather than FIN, and is then cycled while the client's connections
# sit idle in the pool. With the default max_socket_idle the next commands succeed, because the pool
# drops each reset socket before use, so no command fails to the caller. A raw probe connection on
# every listener shows the resets happened, and each node's closed connection count shows the
# client found them.
@pytest.mark.l2("idle-reset")
def test_f1_idle_reset_absorbed_by_pool(nodes: list[di.Node], toxics: list[tuple[str, str]]) -> None:
    assert os.environ["DISCOVERY_IDLE_RESET"] == "1"
    for node in nodes:
        add_toxic(toxics, node.proxy, name="f1-rst", toxic_type="reset_peer", toxicity=0.0, attributes={"timeout": 0})

    client = connect(nodes)
    try:

        def warm(thread: int, iteration: int) -> None:
            key = (NAMESPACE, SET_NAME, f"f1-{(thread * F1_WARM_ITERATIONS + iteration) % F1_KEY_COUNT}")
            client.put(key, {"v": iteration})
            client.get(key)

        load = di.Load(F1_WARM_THREADS, warm)
        warmed = di.wait_for(lambda: len(load.snapshot()) >= F1_WARM_THREADS * F1_WARM_ITERATIONS, WAIT_SEC)
        assert load.stop(WAIT_SEC * 1000), "Warm-up threads blocked"
        assert warmed, f"Warm-up ran {len(load.snapshot())} commands"
        assert load.errors() == [], f"Warm-up errors: {di.kinds(load.errors())}"

        probes = [probe(node) for node in nodes]
        try:
            idle = di.node_stats(client)
            pooled = sum(stats.conns.in_pool for stats in idle.values())
            print(f"L2-F1-IDLE pooled={pooled} {format_stats(idle)}")
            assert pooled >= len(nodes), "No idle pooled connection to reset"

            for node in nodes:
                di.set_toxiproxy_listener(node.proxy, enabled=False)
                di.set_toxiproxy_listener(node.proxy, enabled=True)

            for node, sock in zip(nodes, probes):
                result = probe_result(sock)
                print(f"L2-F1-PROBE listener={node.port} result={result}")
                assert result.startswith("RST"), f"F-1: listener {node.port} did not reset an idle flow: {result}"
        finally:
            for sock in probes:
                sock.close()

        retries_before = client.get_stats().retry_count
        start = di.node_stats(client)
        failures = []
        commands = 0
        for i in range(F1_KEY_COUNT):
            key = (NAMESPACE, SET_NAME, f"f1-{i}")
            commands += 1
            try:
                client.put(key, {"v": i})
            except aerospike.exception.AerospikeError as error:
                failures.append(f"put {i}: {error!r}")
            commands += 1
            try:
                client.get(key)
            except aerospike.exception.AerospikeError as error:
                failures.append(f"get {i}: {error!r}")

        end = di.node_stats(client)
        closed = {name: end[name].conns.closed - start[name].conns.closed for name in end if name in start}
        retries = client.get_stats().retry_count - retries_before
        print(f"L2-F1-RESULT commands={commands} failedToCaller={len(failures)} pooledReset={pooled} "
              f"closedDelta={closed} retryDelta={retries}")
        print(f"L2-F1-STATS {format_stats(end)}")
        for failure in failures:
            print(f"L2-F1-FAILED {failure}")
        undetected = sorted(name for name in start if closed.get(name, 0) == 0)
        assert undetected == [], f"No reset pooled connection was found on {undetected}"
        assert failures == [], f"F-1: commands failed to the caller after idle RST: {failures}"
    finally:
        client.close()


def bounded(executor: concurrent.futures.Executor, bound_ms: int, command: Callable[[], None]) -> str:
    """Run command with a bound and return "ok <ms>", or "error <ms> <error>" for a diagnosable
    error. A command still running past the bound, or a client timeout, fails the test."""
    begin = time.monotonic()
    future = executor.submit(command)
    try:
        future.result(timeout=bound_ms / 1000)
    except concurrent.futures.TimeoutError:
        pytest.fail(f"F-2: command still running after {bound_ms} ms")
    except aerospike.exception.AerospikeError as error:
        elapsed_ms = (time.monotonic() - begin) * 1000
        print(f"L2-F2-ERROR {elapsed_ms:.0f}ms {error!r}")
        assert not isinstance(error, aerospike.exception.TimeoutError), (
            f"F-2: client timeout instead of a diagnosable error after {elapsed_ms:.0f} ms: {error!r}"
        )
        return f"error {elapsed_ms:.0f}ms {error!r}"
    elapsed_ms = (time.monotonic() - begin) * 1000
    return f"ok {elapsed_ms:.0f}ms"


# F-2: with the client network's MTU well below the record size, as on PrivateLink with no path MTU
# discovery, a write and a read of a 64 KiB and a 1 MiB value through every node's listener succeed,
# or fail with a diagnosable error, within a bound from socket_timeout and total_timeout. A hang or a
# client timeout fails the row.
@pytest.mark.l2("low-mtu")
def test_f2_large_record_over_low_mtu_never_hangs(nodes: list[di.Node]) -> None:
    mtu = int(Path("/sys/class/net/eth0/mtu").read_text().strip())
    print(f"L2-F2-MTU eth0={mtu} network={os.environ['DISCOVERY_CLIENT_NETWORK_MTU']}")
    assert mtu == int(os.environ["DISCOVERY_CLIENT_NETWORK_MTU"])
    bound_ms = F2_POLICY["total_timeout"] + F2_POLICY["socket_timeout"]
    masters = owners(nodes)
    generator = random.Random(mtu)

    client = connect(nodes)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        for node in nodes:
            key = keys_on(client, masters, node.name, prefix="f2", count=1)[0]
            for size in F2_RECORD_SIZES:
                value = generator.randbytes(size)

                def write() -> None:
                    client.put(key, {"v": value}, policy=F2_POLICY)

                def read() -> None:
                    _, _, bins = client.get(key, policy=F2_POLICY)
                    assert bins["v"] == value, f"{node.name} returned a different {size} byte value"

                wrote = bounded(executor, bound_ms, write)
                got = bounded(executor, bound_ms, read)
                print(f"L2-F2-RECORD node={node.name} listener={node.port} mtu={mtu} bytes={size} "
                      f"write={wrote} read={got} boundMs={bound_ms}")
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
        client.close()


def round_succeeds(client: aerospike.Client, count: int) -> bool:
    try:
        for i in range(count):
            key = (NAMESPACE, SET_NAME, f"f3-round-{i}")
            client.put(key, {"v": i}, policy=F3_POLICY)
            client.get(key)
    except aerospike.exception.AerospikeError:
        return False
    return True


# F-3: with more concurrent commands than the proxy has source ports for, as at NLB source NAT
# exhaustion, and max_conns_per_node raised above that, the client surfaces a clear error within a
# bound from socket_timeout and total_timeout rather than hanging or timing out, the client counts
# the errors, and commands succeed again once the load drops.
@pytest.mark.l2("port-range")
def test_f3_source_port_exhaustion_surfaces_error_and_recovers(nodes: list[di.Node]) -> None:
    first, last = (int(port) for port in os.environ["DISCOVERY_PROXY_PORT_RANGE"].split())
    ports = last - first + 1
    threads = 2 * ports * len(nodes)
    max_conns_per_node = 4 * threads
    bound_ms = F3_POLICY["total_timeout"] + F3_POLICY["socket_timeout"]
    print(f"L2-F3-RANGE proxyLocalPortRange={first}-{last} ports={ports} nodes={len(nodes)} threads={threads} "
          f"max_conns_per_node={max_conns_per_node}")

    client = connect(nodes, max_conns_per_node=max_conns_per_node)
    try:
        start = di.node_stats(client)
        begin = time.monotonic()

        def put(thread: int, iteration: int) -> None:
            key = (NAMESPACE, SET_NAME, f"f3-{(thread * 31 + iteration) % F3_KEY_COUNT}")
            client.put(key, {"v": iteration}, policy=F3_POLICY)

        load = di.Load(threads, put)
        try:
            di.wait_for(lambda: len(load.errors()) >= threads, WAIT_SEC, interval=POLL_SEC)
        finally:
            finished = load.stop(bound_ms)

        errors = load.errors()
        end = di.node_stats(client)
        first_error_ms = min(((error.start - begin) * 1000 + error.elapsed_ms for error in errors), default=-1)
        max_elapsed_ms = max((error.elapsed_ms for error in errors), default=0)
        error_delta = sum(end[name].error_count - start[name].error_count for name in end if name in start)
        timeouts = count(errors, aerospike.exception.TimeoutError)
        not_client_errors = [
            error.kind for error in errors if not isinstance(error.error, aerospike.exception.AerospikeError)
        ]
        print(f"L2-F3-LOAD commands={len(load.snapshot())} successes={load.successes()} errors={di.kinds(errors)} "
              f"firstErrorMs={first_error_ms:.0f} maxErrorElapsedMs={max_elapsed_ms:.0f} boundMs={bound_ms} "
              f"threadsReturned={finished} clientErrorDelta={error_delta}")
        print(f"L2-F3-STATS {format_stats(end)}")
        if errors:
            print(f"L2-F3-ERROR {errors[0].error!r}")
        assert finished, f"F-3: command threads still blocked {bound_ms} ms after stop"
        assert errors, f"F-3: {threads} threads never exceeded the proxy's {ports} source ports per node"
        assert max_elapsed_ms <= bound_ms, f"F-3: an error took {max_elapsed_ms:.0f} ms, bound {bound_ms} ms"
        assert error_delta > 0, f"F-3: client counted no error: {format_stats(end)}"
        assert not_client_errors == [], f"F-3: not client errors: {not_client_errors}"
        assert timeouts == 0, f"F-3: source port exhaustion surfaced as {timeouts} client timeouts"

        names = sorted(node.name for node in nodes)
        dropped = time.monotonic()

        def recovered() -> bool:
            return sorted(di.client_nodes(client)) == names and round_succeeds(client, 4 * len(nodes))

        back = di.wait_for(recovered, WAIT_SEC, interval=POLL_SEC)
        recovered_ms = (time.monotonic() - dropped) * 1000
        failures = []
        for i in range(F3_RECOVERY_KEY_COUNT):
            key = (NAMESPACE, SET_NAME, f"f3-{i}")
            try:
                client.put(key, {"v": i}, policy=F3_POLICY)
                _, _, bins = client.get(key)
            except aerospike.exception.AerospikeError as error:
                failures.append(f"{i}: {error!r}")
                continue
            assert bins == {"v": i}
        print(f"L2-F3-RECOVERY recovered={bool(back)} ms={recovered_ms:.0f} keys={F3_RECOVERY_KEY_COUNT} "
              f"failed={len(failures)} nodes={sorted(di.client_nodes(client))}")
        assert back, "F-3: no successful round of commands after the load dropped"
        assert failures == [], f"F-3: commands failed after the load dropped: {failures}"
    finally:
        client.close()


def drop_victim(nodes: list[di.Node], toxics: list[tuple[str, str]], check: Callable, **options: Any) -> None:
    """Load the last node with writes to keys it is master of, drop its listener's traffic silently,
    run check(client, victim, load, injected), then remove the toxic and assert the node serves
    again and every node serves the keys it is master of."""
    victim = nodes[-1]
    masters = owners(nodes)
    client = connect(nodes, **options)
    try:
        keys = keys_on(client, masters, victim.name, prefix="f4", count=F4_KEY_COUNT)

        def put(thread: int, iteration: int) -> None:
            key = keys[(thread + iteration * F4_THREADS) % len(keys)]
            client.put(key, {"v": iteration}, policy=F4_POLICY)

        load = di.Load(F4_THREADS, put)
        try:
            started = di.wait_for(lambda: load.successes() >= F4_THREADS, WAIT_SEC, interval=POLL_SEC)
            assert started, f"No command succeeded before the drop: {di.kinds(load.errors())}"
            injected = time.monotonic()
            add_toxic(toxics, victim.proxy, F4_TOXIC, toxic_type="timeout", toxicity=1.0, attributes={"timeout": 0})
            check(client, victim, load, injected)
        finally:
            load.stop(F4_BOUND_MS)

        remove_toxic(toxics, victim.proxy, F4_TOXIC)
        removed = time.monotonic()

        def serving() -> bool:
            return victim.name in di.client_nodes(client) and di.writes(client, keys[0], F4_POLICY)

        back = di.wait_for(serving, WAIT_SEC, interval=POLL_SEC)
        back_ms = (time.monotonic() - removed) * 1000
        print(f"L2-F4-RECOVERED back={bool(back)} ms={back_ms:.0f} nodes={sorted(di.client_nodes(client))}")
        assert back, f"F-4: {victim.name} did not serve again after the toxic was removed"
        infos = {node.name: node.info for node in nodes}
        di.assert_served_by_owners(client, NAMESPACE, SET_NAME, row="L2-F4", infos=infos)
    finally:
        client.close()


def check_timeouts(client: aerospike.Client, victim: di.Node, load: di.Load, injected: float) -> None:
    timed_out = di.wait_for(
        lambda: count(load.errors(), aerospike.exception.TimeoutError) >= F4_THREADS, WAIT_SEC, interval=POLL_SEC
    )
    finished = load.stop(F4_BOUND_MS)
    errors = load.errors()
    first_timeout_ms = min(((error.start - injected) * 1000 + error.elapsed_ms for error in errors), default=-1)
    max_elapsed_ms = max((error.elapsed_ms for error in errors), default=0)
    not_timeouts = [error.kind for error in errors if not isinstance(error.error, aerospike.exception.TimeoutError)]
    print(f"L2-F4-TIMEOUT victim={victim.name} listener={victim.port} firstTimeoutMs={first_timeout_ms:.0f} "
          f"maxErrorElapsedMs={max_elapsed_ms:.0f} timeoutBoundMs={F4_BOUND_MS} errors={di.kinds(errors)} "
          f"successes={load.successes()} threadsReturned={finished}")
    print(f"L2-F4-NODE held={victim.name in di.client_nodes(client)} nodes={sorted(di.client_nodes(client))}")
    assert timed_out, f"F-4: fewer than {F4_THREADS} timeouts after the drop: {di.kinds(errors)}"
    assert finished, f"F-4: command threads still blocked {F4_BOUND_MS} ms after stop"
    assert not_timeouts == [], f"F-4: errors that are not timeouts: {not_timeouts}"
    assert first_timeout_ms <= F4_BOUND_MS, f"F-4: first timeout {first_timeout_ms:.0f} ms after the drop"
    assert max_elapsed_ms <= F4_BOUND_MS, f"F-4: an error took {max_elapsed_ms:.0f} ms, bound {F4_BOUND_MS} ms"


# F-4: a node's listener holds data with no reply and no close, as a conntrack mismatch does.
# Commands to the node fail with a timeout within a bound from socket_timeout and total_timeout,
# every error is a timeout, no command thread stays blocked past the bound, and the node serves
# again once the toxic is removed. Whether the client keeps the node is recorded, not asserted:
# no client marks down a node the cluster still lists.
@pytest.mark.l2("default")
def test_f4_silent_drop_times_out_without_blocking(nodes: list[di.Node], toxics: list[tuple[str, str]]) -> None:
    drop_victim(nodes, toxics, check_timeouts)


def check_breaker(client: aerospike.Client, victim: di.Node, load: di.Load, injected: float) -> None:
    trip_bound_ms = (F4_ERROR_RATE_WINDOW + 1) * TEND_INTERVAL_MS + F4_BOUND_MS
    tripped = di.wait_for(
        lambda: count(load.errors(), aerospike.exception.MaxErrorRateExceeded) > 0, WAIT_SEC, interval=POLL_SEC
    )
    finished = load.stop(F4_BOUND_MS)
    errors = load.errors()
    assert tripped, f"F-4: circuit breaker never tripped: {di.kinds(errors)}"

    backoffs = [error for error in errors if isinstance(error.error, aerospike.exception.MaxErrorRateExceeded)]
    tripped_at = min(error.start + error.elapsed_ms / 1000 for error in backoffs)
    tripped_ms = (tripped_at - injected) * 1000
    after_trip = [error for error in backoffs if error.start >= tripped_at]
    max_backoff_ms = max((error.elapsed_ms for error in after_trip), default=0)
    print(f"L2-F4-BREAKER victim={victim.name} max_error_rate={F4_MAX_ERROR_RATE} "
          f"error_rate_window={F4_ERROR_RATE_WINDOW} tend_interval={TEND_INTERVAL_MS} trippedMs={tripped_ms:.0f} "
          f"tripBoundMs={trip_bound_ms} backoffsAfterTrip={len(after_trip)} maxBackoffElapsedMs={max_backoff_ms:.0f} "
          f"errors={di.kinds(errors)} threadsReturned={finished}")
    assert finished, f"F-4: command threads still blocked {F4_BOUND_MS} ms after stop"
    assert tripped_ms <= trip_bound_ms, f"F-4: breaker tripped {tripped_ms:.0f} ms after the drop"
    assert after_trip, "F-4: no command started after the breaker tripped"
    assert max_backoff_ms < F4_POLICY["socket_timeout"], (
        f"F-4: a command started after the trip took {max_backoff_ms:.0f} ms to fail"
    )


# F-4 with a low max_error_rate: the timeouts trip the node's error-rate circuit breaker within a
# bound from error_rate_window and tend_interval, and commands started after the trip fail fast with
# MaxErrorRateExceeded instead of waiting out socket_timeout.
@pytest.mark.l2("default")
def test_f4_silent_drop_trips_circuit_breaker(nodes: list[di.Node], toxics: list[tuple[str, str]]) -> None:
    drop_victim(
        nodes, toxics, check_breaker, max_error_rate=F4_MAX_ERROR_RATE, error_rate_window=F4_ERROR_RATE_WINDOW
    )
