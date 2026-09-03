import aerospike

config = {
    "hosts": [
        ("127.0.0.1", 3000)
    ],
    "send_bool_as": aerospike.PY_BYTES
}
client = aerospike.client(config)

bins = {"a": True}
key = ("test", "demo", 1)
client.put(key, bins=bins)

client.close()
