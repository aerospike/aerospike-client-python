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


def put():
    global client, key
    client.put(key, {
        "id": 0,
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

def get():
    global client, key
    client.get(key)

def touch():
    global client, key
    client.touch(key)

def append():
    global client, key
    client.append(key, {"brand": "+"})

def prepend():
    global client, key
    client.prepend(key, {"brand": "-"})

def exists():
    global client, key
    client.exists(key)

runner = pyperf.Runner()
runner.bench_func('put', put)
runner.bench_func('get', get)
runner.bench_func('touch', touch)
runner.bench_func('append', append)
runner.bench_func('prepend', prepend)
runner.bench_func('exists', exists)
