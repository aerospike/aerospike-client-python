import unittest
import subprocess
import time
from collections import namedtuple

import docker
import aerospike
from aerospike import exception as e

TestCase = namedtuple("TestCase", ["timeout_delay_ms", "expected_aborted_count", "expected_recovered_count"])
CONTAINER_NAME = "aerospike"
PORT = 3000

aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)


# Using unittest to check that exception was raised
class TestTimeoutDelay(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docker_client = docker.from_env()
        CUSTOM_AEROSPIKE_CONF_FOLDER="/opt/aerospike/etc"
        print("Running server container...")
        cls.container = cls.docker_client.containers.run(
            image="aerospike/aerospike-server",
            detach=True,
            ports={f"{PORT}/tcp": PORT},
            volumes={
                "./server-config/": {
                    "bind": CUSTOM_AEROSPIKE_CONF_FOLDER
                }
            },
            remove=True,
            name=CONTAINER_NAME,
            command=["--config-file", CUSTOM_AEROSPIKE_CONF_FOLDER]
        )

        # TODO: reuse script from .github/workflows instead
        print("Waiting for server to initialize...")
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        cls.container.stop()
        cls.docker_client.close()

    # latency is this high because timeout_delay must be >= 3000ms
    INJECTED_LATENCY_MS = 6000

    def setUp(self):
        config = {
            "hosts": [
                # TODO: get ip addr from docker client
                ("127.0.0.1", PORT)
            ],
            # The default 1000ms tend interval runs frequently enough for this test
            #
            # We want connect_timeout > injected latency so the node refresh doesn't fail
            # Otherwise the node will be dropped after 5 tend iterations
            "connect_timeout": self.INJECTED_LATENCY_MS * 2
        }
        self.client = aerospike.client(config)

        self.key = ("test", "demo", 1)
        self.client.put(self.key, bins={"a": 1})

    def tearDown(self):
        DELETE_LATENCY_COMMAND = [
            "sudo",
            "tcdel",
            CONTAINER_NAME,
            "--docker",
            "--all"
        ]
        subprocess.run(args=DELETE_LATENCY_COMMAND, check=True)

        self.client.close()

    @staticmethod
    def inject_e2e_latency(latency_ms: int):
        inject_latency_command = [
            "sudo",
            "tcset",
            CONTAINER_NAME,
            "--docker",
            "--delay",
            # e2e latency should be less than the client's timeout delay window
            # We want to test that the server does return a response after the timeout delay
            f"{latency_ms}ms"
        ]
        print("Injecting latency")
        subprocess.run(args=inject_latency_command, check=True)
        print("Done")

    def test_case(self):
        test_cases = [
            # latency window < timeout delay window
            # The connection will receive a response before the timeout delay window ends
            # So the connection will be returned to the pool.
            TestCase(timeout_delay_ms=self.INJECTED_LATENCY_MS * 2, expected_aborted_count=0, expected_recovered_count=1),
            # latency window > timeout delay window
            # The connection will not receive a response during the timeout delay window
            # So the connection will be destroyed.
            TestCase(timeout_delay_ms=self.INJECTED_LATENCY_MS // 2, expected_aborted_count=1, expected_recovered_count=0),
        ]

        self.inject_e2e_latency(self.INJECTED_LATENCY_MS)

        for timeout_delay_ms, expected_abort_count, expected_recovered_count in test_cases:
            with self.subTest(
                timeout_delay_ms=timeout_delay_ms,
                expected_abort_count=expected_abort_count,
                expected_recovered_count=expected_recovered_count
            ):
                policy = {
                    # total_timeout should cause get() to timeout and trigger timeout delay window
                    "total_timeout": 1,
                    "timeout_delay": timeout_delay_ms
                }
                with self.assertRaises(e.TimeoutError):
                    self.client.get(key=self.key, policy=policy)

                time.sleep(timeout_delay_ms / 1000)

                # By now, we have passed the timeout delay window
                # And we assume the tend thread has reaped or drained the connection
                cluster_stats = self.client.get_stats()
                self.assertEqual(cluster_stats.nodes[0].conns.aborted, expected_abort_count)
                self.assertEqual(cluster_stats.nodes[0].conns.recovered, expected_recovered_count)


if __name__ == "__main__":
    unittest.main()
