##########################################################################
# Copyright 2013-2021 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################
'''
Module with helper functions to do async get/put by
the :mod:`aerospike.Client.awaitable` methods for the aerospike.client class.
'''
import warnings
import asyncio
import aerospike
from aerospike import exception as e

get_results = {}
get_cqd = 0
put_results = {}
put_cqd = 0

async def _io_get_put(client, op=0, key=None, record=None, meta=None, policy=None, serialize=None):
    global get_cqd, put_cqd
    index = None

    if key is not None:
        if len(key) >= 3:
            index = key[2]
        if index is None:
            if len(key) >= 4:
                index = key[3]

    if index is not None and isinstance(index, bytearray):
        index = bytes(index)

    print(f"io:_io_get_put op:{op} key:{key} index:{index}")

    def put_async_callback(key_tuple, err, exce):
        global put_cqd

        put_cqd -= 1

        result = [key_tuple, err, exce]
        put_results[index]['result'] = result

        print(f"io:put_cb {put_results[index]}")

        loop = put_results[index]['loop']
        fut = put_results[index]['fut']

        loop.call_soon_threadsafe(fut.set_result, result[1][0])

    def get_async_callback(key_tuple, record_tuple, err, exce):
        global get_cqd

        get_cqd -= 1

        result = [key_tuple, record_tuple, err, exce]
        get_results[index]['result'] = result

        print(f"io:get_cb {get_results[index]}")

        loop = get_results[index]['loop']
        fut = get_results[index]['fut']

        loop.call_soon_threadsafe(fut.set_result, result[1])

    loop = asyncio.get_event_loop()
    future = loop.create_future()
    
    context = {'loop': None, 'fut': None, 'result': None}

    if index is not None:
        if op == 0:
            put_results[index] = context
            put_results[index]['loop'] = loop
            put_results[index]['fut'] = future
            put_cqd += 1
        elif op == 1:
            get_results[index] = context
            get_results[index]['loop'] = loop
            get_results[index]['fut'] = future
            get_cqd += 1

    try:
        if op == 0:
            client.put_async(put_async_callback, key, record, meta, policy, serialize)
        if op == 1:
            client.get_async(get_async_callback, key, policy)
    except Exception as e:
        print(f"io:post_get key:{key} except:{e}")
        # if index or isinstance(index, int):
        #     if op == 0:
        #         print(f"io:post_put put_cqd:{put_cqd}")
        #     if op == 1:
        #         print(f"io:post_get get_cqd:{get_cqd}")
        raise e

    await future
    
    if op == 0:
        _,err,exec = put_results[index]['result']
        del put_results[index]
    if op == 1:
        _,_,err,exec = get_results[index]['result']
        del get_results[index]
    
    if err[0] != 0:
        raise exec
    else:
        return future.result()

async def get(client, key=None, policy=None):
    return await _io_get_put(client, 1, key, None, None, policy)

async def put(client, key=None, record=None, meta=None, policy=None, serialize=None):
    return await _io_get_put(client, 0, key, record, meta, policy, serialize)
