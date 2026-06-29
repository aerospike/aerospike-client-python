import aerospike
from aerospike_helpers.operations import string_operations as so

ops = [
    so.strlen("mybin"),
    so.upper("mybin"),
    so.replace("mybin", "old", "new"),
]

config = {
    "hosts": [
        ("127.0.0.1", 3000)
    ]
}
client = aerospike.client(config)

key = ("test", "demo", 1)
client.put(key, bins={"mybin": "old"})

_, _, bins = client.operate(key, ops)
