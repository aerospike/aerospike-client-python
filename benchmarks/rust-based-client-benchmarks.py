#!/usr/bin/env python3
import pyperf

import aerospike

client: aerospike.Client = None
key = ("test", "test", 0)

def setup():
    global client, key
    client = aerospike.client({"hosts": [("127.0.0.1", 3000)]})
    client.put(key, {
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

setup()

runner = pyperf.Runner()
runner.bench_func('put', client.put,
    key,
    {
        "id": 0,
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    }
)
runner.bench_func('get', client.get, key)
runner.bench_func('touch', client.touch, key)
runner.bench_func('append', client.append, key, "brand", "+")
runner.bench_func('prepend', client.prepend, key, "brand", "-")
runner.bench_func('exists', client.exists, key)
