"""Print the cluster-wide statistics deprecated_requests total.

Reads test/config.conf the same way the integration tests do. Prints one integer
on stdout.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "new_tests"))

import aerospike
from test_base_class import TestBaseClass


def deprecated_requests(statistics):
    for item in statistics.split(";"):
        name, separator, value = item.partition("=")
        if separator and name == "deprecated_requests":
            return int(value)
    raise SystemExit("deprecated_requests was not in the statistics response")


def main():
    config = TestBaseClass.get_connection_config()
    client = aerospike.client(config)
    if config["user"] is None and config["password"] is None:
        client.connect()
    else:
        client.connect(config["user"], config["password"])

    total = 0
    try:
        for error, result in client.info_all("statistics").values():
            if error is not None:
                raise SystemExit(f"statistics info failed: {error}")
            total += deprecated_requests(result)
    finally:
        client.close()

    print(total)


if __name__ == "__main__":
    main()
