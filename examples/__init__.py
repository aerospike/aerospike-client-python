import aerospike


class Example:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 3000,
        user: str = None,
        password: str = None,
        namespace: str = "test",
        set_name: str = "demo"
    ):
        config = {
            "hosts": [(host, port)],
            "user": user,
            "password": password
        }
        client = aerospike.client(config)

        self.client = client
        self.namespace = namespace
        self.set_name = set_name

    def __del__(self):
        self.client.close()
