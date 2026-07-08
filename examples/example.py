import aerospike
from aerospike_helpers.operations import string_operations


class Example:
    def __init__(self):
        config = {
            "hosts": [("127.0.0.1", 3000)]
        }
        client = aerospike.client(config)
        self.client = client

    def __del__(self):
        self.client.close()

class StringOperations(Example):
    def run(self):
        client = self.client

        key = ("test", "demo", "opstr_read")
        BIN_NAME = "text"
        client.put(key, {BIN_NAME: "hello world"})

        ops = [
            string_operations.strlen(BIN_NAME)
        ]
        _, _, bins = client.operate(key, ops)
        print(f"strlen(\"hello world\") = {bins[BIN_NAME]}")
