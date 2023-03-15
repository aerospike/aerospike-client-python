import subprocess
import time
import sys

import docker
import aerospike

if __name__ == "__main__":
    try:
        print("Waiting for cluster to start...")
        time.sleep(5)

        client = docker.from_env()
        firstNode = client.containers.get("aerolab-mydc_1")
        serverIp = firstNode.attrs["NetworkSettings"]["IPAddress"]
        config = {
            "hosts": [(serverIp, 3000)],
            "policies": {
                "read": {
                    "socket_timeout": 5000,
                    "total_timeout": 10000,
                }
            }
        }

        print(f"Connecting to host {serverIp}...")
        client = aerospike.client(config)

        print("Writing record to all nodes...")
        # Default write policy writes to all nodes
        key = ("test", "demo", 1)
        bin = {"bin": 1}
        client.put(key, bin)

        print("Stopping node 1...")
        subprocess.run(["aerolab", "cluster", "stop", "--nodes=1"])

        print("Getting record...")
        record = client.get(key)
        print(record)
        exitCode = 0
    except Exception as e:
        print(e)
        exitCode = 1
    finally:
        print("Cleaning up test...")
        client.close()
        subprocess.run(["aerolab", "cluster", "stop"])
        subprocess.run(["aerolab", "cluster", "destroy"])
        sys.exit(exitCode)
