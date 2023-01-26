import subprocess
import time
import docker

import aerospike

if __name__ == "__main__":
    print("Creating 2 node cluster...")
    subprocess.run(["aerolab", "cluster", "create", "--count=2"])

    try:
        print("Waiting for cluster to start...")
        time.sleep(5)

        # Get IP address of cluster
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
    except Exception as e:
        print(e)
    finally:
        print("Cleaning up test...")
        client.close()
        subprocess.run(["aerolab", "cluster", "stop"])
        subprocess.run(["aerolab", "cluster", "destroy"])
