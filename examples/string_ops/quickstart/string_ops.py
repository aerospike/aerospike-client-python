from .. import Example

class StringOps(Example):
    def run(self):
        from aerospike_helpers.operations import string_operations as so

        ops = [
            so.strlen("mybin"),
            so.upper("mybin"),
            so.replace("mybin", "old", "new"),
        ]

        key = ("test", "demo", 1)
        self.client.put(key, bins={"mybin": "old"})

        _, _, bins = self.client.operate(key, ops)
