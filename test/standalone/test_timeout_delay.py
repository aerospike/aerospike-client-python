import unittest
import subprocess
import time
import os

import docker
import aerospike
from aerospike import exception as e


CONTAINER_NAME = "aerospike"
PORT = 3000


# Using unittest to check that exception was raised
class TestTimeoutDelay(unittest.TestCase):
    def setUpClass(self):
        self.docker_client = docker.from_env()
        print("Running server container...")
        self.container = self.docker_client.containers.run(
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
        self.client = aerospike.client(config)

        self.key = ("test", "demo", 1)
        self.client.put(self.key, bins={"a": 1})

    def tearDownClass(self):
        self.client.close()

        self.container.stop()
        self.container.remove()
        self.docker_client.close()

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
            # timeout delay window, (expected number of aborted conns, expected number of recovered conns)
            # Test case: connection should've been returned to pool (recovered)
            (self.E2E_LATENCY_MS * 0.5, (0, 1)),
            # Test case: connection should've been destroyed (aborted)
            (self.E2E_LATENCY_MS * 2, (1, 0)),
        ]

        for timeout_delay_ms, expected_results in test_cases:
            with self.subTest(input=timeout_delay_ms, expected=expected_results):
                # Should cause first command to timeout
                policy = {
                    "total_timeout": 1,
                    "timeout_delay": timeout_delay_ms
                }
                with self.assertRaises(e.TimeoutError):
                    self.client.get(key=self.key, policy=policy)

                time.sleep(timeout_delay_ms)

                # By now, we have passed the timeout delay window
                aborted_count, recovered_count = expected_results
                cluster_stats = self.client.get_stats()
                assert cluster_stats.nodes[0].conns.aborted == aborted_count
                assert cluster_stats.nodes[0].conns.recovered == recovered_count


if __name__ == "__main__":
    unittest.main()
