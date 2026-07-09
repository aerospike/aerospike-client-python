from .. import Example

class StringOps(Example):
    def run(self):
        client = self.client

        from aerospike_helpers.operations import string_operations as so

        ops = [
            so.strlen("mybin"),
            so.upper("mybin"),
            so.replace("mybin", "old", "new"),
        ]

        key = ("test", "demo", 1)
        client.put(key, bins={"mybin": "old"})

        _, _, bins = client.operate(key, ops)
