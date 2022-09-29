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
import asyncio
import aerospike
from aerospike import exception as e

_get_results = {}
_get_cqd = 0
_put_results = {}
_put_cqd = 0

async def _io_get_put(client, op=0, key=None, record=None, meta=None, policy=None, serialize=None):
    global _get_cqd, _put_cqd
    index = None

    if key is not None:
        if len(key) >= 3:
            index = key[2]
        if index is None:
            if len(key) >= 4:
                index = key[3]

    if index is not None and isinstance(index, bytearray):
        index = bytes(index)

    #print(f"io:_io_get_put op:{op} key:{key} index:{index}")

    def put_async_callback(key_tuple, err, exce):
        global _put_cqd

        _put_cqd -= 1

        result = [key_tuple, err, exce]
        _put_results[index]['result'] = result

        #print(f"io:put_cb {_put_results[index]}")

        loop = _put_results[index]['loop']
        fut = _put_results[index]['fut']

        if result[1][0] != 0:
            #print(f"io:put_cb except:{result[2]}")
            loop.call_soon_threadsafe(fut.set_exception, result[2])
        else:
            loop.call_soon_threadsafe(fut.set_result, result[1][0])

    def get_async_callback(key_tuple, record_tuple, err, exce):
        global _get_cqd

        _get_cqd -= 1

        result = [key_tuple, record_tuple, err, exce]
        _get_results[index]['result'] = result

        #print(f"io:get_cb {_get_results[index]}")

        loop = _get_results[index]['loop']
        fut = _get_results[index]['fut']

        if result[2][0] != 0:
            #print(f"io:get_cb except:{result[3]}")
            loop.call_soon_threadsafe(fut.set_exception, result[3])
        else:
            loop.call_soon_threadsafe(fut.set_result, result[1])

    loop = asyncio.get_event_loop()
    future = loop.create_future()
    
    context = {'loop': None, 'fut': None, 'result': None}

    if index is not None:
        if op == 0:
            _put_results[index] = context
            _put_results[index]['loop'] = loop
            _put_results[index]['fut'] = future
            _put_cqd += 1
        elif op == 1:
            _get_results[index] = context
            _get_results[index]['loop'] = loop
            _get_results[index]['fut'] = future
            _get_cqd += 1

    try:
        if op == 0:
            client.put_async(put_async_callback, key, record, meta, policy, serialize)
        if op == 1:
            client.get_async(get_async_callback, key, policy)
    except Exception as e:
        #print(f"io:post_get key:{key} except:{e} cqd:{[_get_cqd if op else _put_cqd]}")
        raise e

    await future
    
    if index is not None:
        if op == 0:
            del _put_results[index]
        if op == 1:
            del _get_results[index]
    
    return future.result()

async def get(client, key=None, policy=None):
    return await _io_get_put(client, 1, key, None, None, policy)

async def put(client, key=None, record=None, meta=None, policy=None, serialize=None):
    return await _io_get_put(client, 0, key, record, meta, policy, serialize)
