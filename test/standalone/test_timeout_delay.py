import unittest
import subprocess
import time
import os

import docker
import aerospike
from aerospike import exception as e


CONTAINER_NAME = "aerospike"
PORT = 3000

docker_client = docker.from_env()
print("Running server container...")
container = docker_client.containers.run(
    image="aerospike/aerospike-server",
    detach=True,
    ports={f"{PORT}/tcp": PORT},
    name=CONTAINER_NAME,
    remove=True
)
print("Waiting for server to initialize...")
time.sleep(5)

class TestTimeoutDelay(unittest.TestCase):
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
            "100ms"
        ]
        env = os.environ.copy()
        subprocess.run(args=inject_latency_command, check=True, env=env)

        key = ("test", "demo", 1)
        policy = {
            # Should cause first command to timeout
            "total_timeout": 1,
            # All subsequent commands should timeout because of timeout_delay
            "timeout_delay": 2000
        }
        with self.assertRaises(e.ClientError):
            client.get(key=key, policy=policy)

        with self.assertRaises(e.ClientError):
            client.get(key=key, policy=policy)


if __name__ == "__main__":
    unittest.main()
