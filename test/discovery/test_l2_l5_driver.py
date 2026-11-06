"""Smoke test of the L5 driver (l5_driver.py) against the pass-through-ports profile of the
discovery-l2-topology action, so it stays runnable between the hand-run L5 rows on AWS.

Every node advertises one hostname with its own listener port, the shape the L5 rows seed through.
"""

import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import discovery_info as di

DRIVER = Path(__file__).resolve().parent / "l5_driver.py"
REPLY_TIMEOUT_SEC = 60
QUIT_TIMEOUT_SEC = 40
PARTITIONS = 4096


class DriverProcess:
    """The driver with its stdout read on a thread, one JSON object per line."""

    def __init__(self, seed):
        command = [sys.executable, str(DRIVER), "--seed", seed, "--shape", "pass-through", "--tend-interval", "250",
                   "--timeout", "1000", "--namespace", "test", "--set", "l5-driver"]
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.lines = queue.Queue()
        self.messages = []
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        for line in self.process.stdout:
            self.lines.put(line)
        self.lines.put(None)

    def send(self, command):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def wait_for(self, matches):
        """Return the first message from now on that matches, failing after REPLY_TIMEOUT_SEC."""
        deadline = time.monotonic() + REPLY_TIMEOUT_SEC
        while True:
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"No matching message within {REPLY_TIMEOUT_SEC} s; last {self.messages[-3:]}"
            line = self.lines.get(timeout=remaining)
            assert line is not None, f"Driver exited with {self.process.wait()}; last {self.messages[-3:]}"
            message = json.loads(line)
            assert isinstance(message.get("t"), int), f"Message without an integer t: {message}"
            self.messages.append(message)
            if matches(message):
                return message

    def quit(self):
        self.send("quit")
        self.process.stdin.close()
        return self.process.wait(QUIT_TIMEOUT_SEC)


@pytest.fixture
def advertised():
    """Each node's name and the one alternate (host, port) it advertises."""
    host = os.environ["DISCOVERY_ENDPOINT_HOSTNAME"]
    found = {}
    for port in os.environ["DISCOVERY_ENDPOINT_PORTS"].split(","):
        info = di.request(host, int(port), "node", "service-clear-alt")
        alternates = di.endpoints(info["service-clear-alt"])
        assert len(alternates) == 1, f"{info['node']} on listener {port} advertises {alternates}"
        found[info["node"]] = di.split_endpoint(alternates[0])
    return found


@pytest.fixture
def driver(advertised):
    name = sorted(advertised)[0]
    host, port = advertised[name]
    process = DriverProcess(f"{host}:{port}")
    yield process
    if process.process.poll() is None:
        process.process.kill()


@pytest.mark.l2("pass-through-ports")
def test_l5_driver_routes_serves_and_watches(advertised, driver):
    ready = driver.wait_for(lambda message: message["type"] == "ready")
    assert (ready["contract"], ready["client"], ready["shape"]) == (1, "python", "pass-through")

    def routed(message):
        if message["type"] != "snapshot":
            return False
        held = {node["name"]: node for node in message["nodes"]}
        return sorted(held) == sorted(advertised) and all(node["reachable"] is True for node in held.values())

    snapshot = driver.wait_for(routed)
    for node in snapshot["nodes"]:
        assert node["port"] == advertised[node["name"]][1], f"{node} is not at {advertised[node['name']]}"
        assert node["active"] is True

    driver.send("serve l2")
    served = driver.wait_for(lambda message: message["type"] == "served")
    counts = (served["written"], served["read"], served["mismatched"], served["failures"])
    assert counts == (PARTITIONS, PARTITIONS, 0, 0), served

    name = sorted(advertised)[0]
    host, port = advertised[name]
    driver.send(f"watch {host}:{port} {name}")
    watch = driver.wait_for(lambda message: message["type"] == "watch")
    assert watch["usableAt"] is not None, watch

    assert driver.quit() == 0
