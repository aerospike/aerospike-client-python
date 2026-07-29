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
        self.user_key = "docreadkey"
        self.key = (self.namespace, self.set_name, self.user_key)
        self.non_existent_key = (self.namespace, self.set_name, "nonexistent")
        self.BIN_NAME = "a"

    def cleanup(self):
        self.client.close()

class AdminExample(Example):
    def __init__(self):
        # TODO: admin user doesn't have enough permissions
        super().__init__(user="admin", password="admin")

class ExampleWithUser(AdminExample):
    def __init__(self):
        super().__init__()
        self.user = "foo-example"
        self.password = "foobar"
        self.client.admin_create_user(self.user, self.password, roles=[])

    def cleanup(self):
        self.client.admin_drop_user(self.user)
        super().cleanup()

class UDFExample(Example):
    def __init__(self):
        extra_config = {
            'lua': {
                'user_path': os.path.dirname(__file__) + "/client/"
            }
        }
        super().__init__(extra_config=extra_config)

class ExampleWithIndex(Example):
    INDEX_NAME = "index_name"
    def __init__(self):
        super().__init__()

        self.client.index_single_value_create(self.namespace, self.set_name, self.BIN_NAME, aerospike.INDEX_INTEGER, self.INDEX_NAME)

    def cleanup(self):
        self.client.index_remove(self.namespace, self.INDEX_NAME)

        super().cleanup()

class ExampleWithRecord(Example):
    def __init__(self):
        super().__init__()

        self.client.put(self.key, bins={self.BIN_NAME: 1})

    def cleanup(self):
        self.client.remove(self.key)

        super().cleanup()
