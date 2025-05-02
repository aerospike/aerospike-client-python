import pyperf
import aerospike

config = {"hosts": [("127.0.0.1", 3000)]}
client = aerospike.client(config)

keys = []
try:
    for i in range(1000):
        key = ("test", "demo", i)
        client.put(key, {"a": 1})
        keys.append(key)

    policy = {"concurrent": True}
    runner = pyperf.Runner()
    runner.bench_func('batch_read', client.batch_read, keys, ["a"], policy)
finally:
    client.close()
