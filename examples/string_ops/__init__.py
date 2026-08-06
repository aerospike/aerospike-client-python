import aerospike


class Example:
    def __init__(self):
        config = {
            "hosts": [("127.0.0.1", 3000)]
        }
        client = aerospike.client(config)

        self.client = client

    def __del__(self):
        self.client.close()
