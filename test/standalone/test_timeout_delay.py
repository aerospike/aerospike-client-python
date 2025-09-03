import unittest
import subprocess
import time
import os
from collections import namedtuple

import docker
import aerospike
from aerospike import exception as e

TestCase = namedtuple("TestCase", ["timeout_delay_ms", "expected_aborted_count", "expected_recovered_count"])
CONTAINER_NAME = "aerospike"
PORT = 3000


# Using unittest to check that exception was raised
class TestTimeoutDelay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docker_client = docker.from_env()
        print("Running server container...")
        cls.container = cls.docker_client.containers.run(
            image="aerospike/aerospike-server",
            detach=True,
            ports={f"{PORT}/tcp": PORT},
            name=CONTAINER_NAME,
            remove=True
        )
        # TODO: reuse script from .github/workflows instead
        print("Waiting for server to initialize...")
        time.sleep(5)

        config = {
            "hosts": [
                ("127.0.0.1", PORT)
            ]
        }
        cls.client = aerospike.client(config)

        cls.key = ("test", "demo", 1)
        cls.client.put(cls.key, bins={"a": 1})

    def tearDownClass(cls):
        cls.client.close()

        cls.container.stop()
        cls.container.remove()
        cls.docker_client.close()

    E2E_LATENCY_MS = 2000

    def test_case(self):
        env = os.environ.copy()
        inject_latency_command = [
            "sudo",
            "-E",
            "tcset",
            CONTAINER_NAME,
            "--docker",
            "--delay",
            # e2e latency should be less than the client's timeout delay window
            # We want to test that the server does return a response after the timeout delay
            f"{self.E2E_LATENCY_MS}ms"
        ]
        subprocess.run(args=inject_latency_command, check=True, env=env)

        test_cases = [
            TestCase(timeout_delay_ms=self.E2E_LATENCY_MS * 0.5, expected_aborted_count=0, expected_recovered_count=1),
            TestCase(timeout_delay_ms=self.E2E_LATENCY_MS * 2, expected_aborted_count=1, expected_recovered_count=0),
        ]

        for timeout_delay_ms, expected_abort_count, expected_recovered_count in test_cases:
            with self.subTest(
                input=timeout_delay_ms,
                timeout_delay_ms=timeout_delay_ms,
                expected_abort_count=expected_abort_count,
                expected_recovered_count=expected_recovered_count
            ):
                # Should cause first command to timeout
                policy = {
                    "total_timeout": 1,
                    "timeout_delay": timeout_delay_ms
                }
                with self.assertRaises(e.TimeoutError):
                    self.client.get(key=self.key, policy=policy)

                time.sleep(timeout_delay_ms)

                # By now, we have passed the timeout delay window
                cluster_stats = self.client.get_stats()
                assert cluster_stats.nodes[0].conns.aborted == expected_abort_count
                assert cluster_stats.nodes[0].conns.recovered == expected_recovered_count


if __name__ == "__main__":
    unittest.main()
