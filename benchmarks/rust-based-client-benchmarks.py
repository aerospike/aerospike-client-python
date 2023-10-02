#!/usr/bin/env python3
import asyncio
import pyperf
import random

import aerospike

client: aerospike.Client = None
key = ("test", "test", 0)

async def setup():
    global client, key
    client = await aerospike.client({"hosts": [("127.0.0.1", 3000)]})
    client.put(key, {
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })


async def put():
    global client, key
    client.put(key, {
        "id": 0,
        "brand": "Ford",
        "model": "Mustang",
        "year": 1964,
        "fa/ir": "بر آن مردم دیده روشنایی سلامی چو بوی خوش آشنایی",
    })

async def get():
    global client, key
    client.get(key)

async def touch():
    global client, key
    await client.touch(key)

async def append():
    global client, key
    await client.append(key, {"brand": "+"})

async def prepend():
    global client, key
    await client.prepend(key, {"brand": "-"})

async def exists():
    global client, key
    await client.exists(key)

asyncio.run(setup())

runner = pyperf.Runner()
runner.bench_async_func('put', put)
runner.bench_async_func('get', get)
runner.bench_async_func('touch', touch)
runner.bench_async_func('append', append)
runner.bench_async_func('prepend', prepend)
runner.bench_async_func('exists', exists)
