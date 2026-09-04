import aerospike
import pytest
import subprocess
import time
from collections import namedtuple
# import os

# import docker
from aerospike import exception as e

CONTAINER_NAME = "aerospike"
PORT = 3000

# We want to see if the client closes connections on it's end
# This is useful for debugging the client when the test cases fail
aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)


class TestTimeoutDelay:
    # Ensure that the server doesn't reap client connections
    '''
        @classmethod
        def setup_class(cls):
            cls.docker_client = docker.from_env()
            CUSTOM_AEROSPIKE_CONF_FOLDER = "/opt/aerospike/etc/"
            print("Running server container...")

            script_dir = os.path.dirname(os.path.abspath(__file__))

            cls.container = cls.docker_client.containers.run(
                image="aerospike/aerospike-server",
                detach=True,
                ports={f"{PORT}/tcp": PORT},
                volumes={
                    f"{script_dir}/server-config/": {
                        "bind": CUSTOM_AEROSPIKE_CONF_FOLDER,
                        "mode": "ro",
                    }
                },
                remove=True,
                name=CONTAINER_NAME,
                command=["--config-file", f"{CUSTOM_AEROSPIKE_CONF_FOLDER}/aerospike.conf"],
            )

            # TODO: reuse script from .github/workflows instead
            print("Waiting for server to initialize...")
            time.sleep(5)

        @classmethod
        def teardown_class(cls):
            subprocess.run(args=["docker", "logs", CONTAINER_NAME])
            cls.container.stop()
            cls.docker_client.close()
        '''

    # latency is this high because timeout_delay must be >= 3000ms
    INJECTED_LATENCY_MS = 6000

    @pytest.fixture(autouse=True)
    def as_client(self):
        config = {
            "hosts": [
                ("127.0.0.1", PORT)
            ],
            # The default 1000ms tend interval runs frequently enough for this test
            #
            # We want connect_timeout > tend interval so the node refresh doesn't fail
            # Otherwise the node will eventually be dropped after 5 tend iterations
            # because x node refresh attempt fails (x = 5 tend iterations)
            # "connect_timeout": self.INJECTED_LATENCY_MS * 2,
        }
        client = aerospike.client(config)

        self.key = ("test", "demo", 1)
        client.put(self.key, bins={"a": 1})

        # inject_latency_command = [
        #     "sudo",
        #     "tcset",
        #     CONTAINER_NAME,
        #     "--docker",
        #     "--delay",
        #     f"{self.INJECTED_LATENCY_MS}ms",
        # ]
        # print(f"Injecting latency of {self.INJECTED_LATENCY_MS} ms for outgoing packets from client to server")
#        subprocess.run(args=inject_latency_command, check=True)

        yield client

        # DELETE_LATENCY_COMMAND = ["sudo", "tcdel", CONTAINER_NAME, "--docker", "--all"]
#        subprocess.run(args=DELETE_LATENCY_COMMAND, check=True)

        client.close()

    TestCase = namedtuple(
        "TestCase",
        ["timeout_delay_ms", "expected_aborted_count", "expected_recovered_count"],
    )

    @pytest.mark.parametrize("timeout_delay_ms, expected_aborted_count, expected_recovered_count", [
        # latency window < timeout delay window
        # The connection will receive a response before the timeout delay window ends
        # So the connection will be returned to the pool.
        TestCase(
            timeout_delay_ms=12000,
            expected_aborted_count=0,
            expected_recovered_count=1,
        ),
        # latency window > timeout delay window
        # The connection will not receive a response during the timeout delay window
        # So the connection will be destroyed.
        TestCase(
           timeout_delay_ms=INJECTED_LATENCY_MS // 2,
           expected_aborted_count=1,
           expected_recovered_count=0,
        ),
    ])
    def test_timeout_delay(self, as_client, timeout_delay_ms, expected_aborted_count, expected_recovered_count):
        # Inject latency in server
        subprocess.run(["asinfo", "-v", "sets"], check=True)

        policy = {
            # This socket_timeout and max_retries should cause get() to timeout immediately
            # and trigger the timeout delay window
            "socket_timeout": 100,
            "max_retries": 0,
            # total_timeout will override timeout_delay, so make sure it doesn't interfere with it.
            "total_timeout": 99999,
            "timeout_delay": timeout_delay_ms,
        }
        print(policy)
        print(f"Trigger socket timeout and start timeout delay window of {timeout_delay_ms} ms...")
        with pytest.raises(e.TimeoutError):
            as_client.get(key=self.key, policy=policy)

        # When a connection is aborted, it will not be updated precisely when the timeout delay window
        # ends. We need to wait for the tend thread to update the cluster's stats properly.
        # So we wait a few seconds after the timeout delay window.
        #
        # This is also ok for the other test case (where the connection gets recovered)
        time.sleep(timeout_delay_ms / 1000)

        print("Timeout delay window has ended.")
        WAIT_FOR_TEND_INTERVAL_SECS = 5
        print(f"Sleeping another {WAIT_FOR_TEND_INTERVAL_SECS} seconds to wait for tend thread to update cluster stats")
        time.sleep(WAIT_FOR_TEND_INTERVAL_SECS)

        # By now, we have passed the timeout delay window
        # And we assume the tend thread has attempted to drain the connection
        cluster_stats = as_client.get_stats()
        print("Using standard metrics to get synchronous connection statistics...")
        # If the first assert fails, we won't know the number of recovered connections
        print("Num of aborted connections:", cluster_stats.nodes[0].conns.aborted)
        print("Num of recovered connections:", cluster_stats.nodes[0].conns.recovered)

        # DEBUG: check if server reaped a client connection
#        _, stdout = self.container.exec_run(cmd='sh -c "asinfo -v \'statistics\' -l | grep reaped_fds"')
#        print("Number of client connections reaped by server:", stdout)

        assert cluster_stats.nodes[0].conns.aborted == expected_aborted_count
        assert cluster_stats.nodes[0].conns.recovered == expected_recovered_count
