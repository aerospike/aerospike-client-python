import aerospike
import os


class Example:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 3000,
        user: str = None,
        password: str = None,
        namespace: str = "test",
        set_name: str = "demo",
        extra_config: dict = {}
    ):
        self.config = {
            "hosts": [(host, port)],
            "user": user,
            "password": password
        }
        self.config |= extra_config
        client = aerospike.client(self.config)

        self.client = client
        self.namespace = namespace
        self.set_name = set_name
        self.key = (self.namespace, self.set_name, "docreadkey")

    def __del__(self):
        self.client.close()


class UDFExample(Example):
    def __init__(self):
        extra_config = {
            'lua': {
                'user_path': os.path.dirname(__file__) + "/client/"
            }
        }
        super().__init__(extra_config)

    def __del__(self):
        pass

class ExampleWithIndex(Example):
    INDEX_NAME = "index_name"
    def __init__(self):
        self.client.index_single_value_create(self.namespace, self.set_name, aerospike.INDEX_INTEGER, self.INDEX_NAME)

    def __del__(self):
        self.client.index_remove(self.namespace, self.INDEX_NAME)

# TODO: I'm wondering if pytest can be used since
# it has fixtures as a built-in feature
class ExampleWithRecord(Example):
    def __init__(self):
        super().__init__()

        self.client.put(self.key, bins={"a": 1})

    def __del__(self):
        self.client.remove(self.key)

        super().__del__()
