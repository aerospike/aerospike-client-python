import aerospike

config = {
    "hosts": [
        ("127.0.0.1", 3000)
    ],
}
client = aerospike.client(config)

key = ("test", "demo", 1)
_, _, bins = client.get(key)
print(bins["a"])
assert isinstance(bins["a"], bytearray)

client.close()
