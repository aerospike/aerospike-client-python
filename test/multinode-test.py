import subprocess
import time
import docker

import aerospike

if __name__ == "__main__":
    subprocess.run(["aerolab", "cluster", "create", "--count=2"])

    try:
        time.sleep(5)

        # Get IP address of cluster
        client = docker.from_env()
        firstNode = client.containers.get("aerolab-mydc_1")
        serverIp = firstNode.attrs["NetworkSettings"]["IPAddress"]
        config = {"hosts": [(serverIp, 3000)]}
        client = aerospike.client(config)

        print("Connected to host", serverIp)

        key = ("test", "demo", 1)
        bin = {"bin": 1}
        client.put(key, bin)

        subprocess.run(["aerolab", "cluster", "stop", "--nodes=1"])

        record = client.get(key)
        print(record)
    finally:
        # Cleanup
        client.close()
        subprocess.run(["aerolab", "cluster", "stop"])
        subprocess.run(["aerolab", "cluster", "destroy"])
