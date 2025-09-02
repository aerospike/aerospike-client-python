import unittest
import subprocess
import time
import os

import docker
import aerospike
from aerospike import exception as e


CONTAINER_NAME = "aerospike"
PORT = 3000


class TestTimeoutDelay(unittest.TestCase):
    def setUp(self):
        self.docker_client = docker.from_env()
        print("Running server container...")
        self.container = self.docker_client.containers.run(
            image="aerospike/aerospike-server",
            detach=True,
            ports={f"{PORT}/tcp": PORT},
            name=CONTAINER_NAME,
            remove=True
        )
        print("Waiting for server to initialize...")
        time.sleep(5)

    def tearDown(self):
        """Clean up after test methods."""
        self.container.stop()
        self.container.remove()
        self.docker_client.close()

    def test_case(self):
        # Using unittest to check that exception was raised
        config = {
            "hosts": [
                ("127.0.0.1", PORT)
            ]
        }
        client = aerospike.client(config)

        inject_latency_command = [
            "sudo",
            "-E",
            "tcset",
            CONTAINER_NAME,
            "--docker",
            "--delay",
            # e2e latency should be less than the client's timeout delay window
            # We want to test that the server does return a response after the timeout delay
            "1000ms"
        ]
        env = os.environ.copy()
        subprocess.run(args=inject_latency_command, check=True, env=env)

        key = ("test", "demo", 1)
        TIMEOUT_DELAY_MS = 2000
        policy = {
            # Should cause first command to timeout
            "total_timeout": 1,
            # All subsequent commands should timeout because of timeout_delay
            "timeout_delay": TIMEOUT_DELAY_MS
        }
        with self.assertRaises(e.TimeoutError):
            client.get(key=key, policy=policy)

        with self.assertRaises(e.TimeoutError):
            client.get(key=key, policy=policy)

        time.sleep(TIMEOUT_DELAY_MS)

        # By now, we have passed the timeout delay window
        # The server should've returned a response to the client connection
        # and the client connection should not have been destroyed
        # TODO: how to verify this?
        client.get(key=key)


if __name__ == "__main__":
    unittest.main()
