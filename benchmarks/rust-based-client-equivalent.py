#!/usr/bin/env python3
import asyncio
import pyperf
import random

import aerospike

client: aerospike.Client = None

async def setup():
    global client
    client = await aerospike.client({"hosts": [("127.0.0.1", 3000)]})

    for i in range(100):
        key = ("test", "test", i)
        client.put(key, {
            "brand": "Ford",
            "model": "Mustang",
            "year": 1964,
            "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
        })


async def put(i):
    global client

    key = ("test", "test", i)
    client.put(key, {
        "id": i,
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

async def get(i):
    global client

    key = ("test", "test", i)
    client.get(key)

async def touch(i):
    global client

    key = ("test", "test", i)
    await client.touch(key)

async def append(i):
    global client

    key = ("test", "test", i)
    await client.append(key, {"brand": "+"})

async def prepend(i):
    global client

    key = ("test", "test", i)
    await client.prepend(key, {"brand": "-"})

async def exists(i):
    global client

    key = ("test", "test", i)
    await client.exists(key)

asyncio.run(setup())

runner = pyperf.Runner()
runner.bench_async_func('put', put, random.randint(0, 99))
runner.bench_async_func('get', get, random.randint(0, 99))
runner.bench_async_func('touch', touch, random.randint(0, 99))
runner.bench_async_func('append', append, random.randint(0, 99))
runner.bench_async_func('prepend', prepend, random.randint(0, 99))
runner.bench_async_func('exists', exists, random.randint(0, 99))
