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

# TODO: I'm wondering if pytest can be used since
# it has fixtures as a built-in feature
class ExampleWithRecord(Example):
    def __init__(self):
        super().__init__()

        self.key = (self.namespace, self.set_name, "docreadkey")
        self.client.put(self.key, bins={"a": 1})

    def __del__(self):
        self.client.remove(self.key)

        super().__del__()
