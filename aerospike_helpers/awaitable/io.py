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

get_results = {}
get_cqd = 0
put_results = {}
put_cqd = 0

async def get(client, namespace, set, key, policy):
    global get_cqd

    def get_async_callback(key_tuple, record_tuple, err):
        global get_cqd
        (key) = key_tuple

        get_cqd -= 1
        result = [key_tuple, record_tuple, err]
        #print(result)
        loop = get_results[key[2]]['loop']
        fut = get_results[key[2]]['fut']
        
        loop.call_soon_threadsafe(fut.set_result, result)
        
        del get_results[key[2]]

    context = {'loop': None, 'fut': None}
    get_results[key["key"]] = context
    
    loop = asyncio.get_event_loop()
    get_results[key["key"]]['loop'] = loop
    
    future = loop.create_future()
    get_results[key["key"]]['fut'] = future
    
    get_cqd += 1
    
    client.get_async(get_async_callback, key, policy)
    
    print(f"get_cqd: {get_cqd}")
    
    await future
    return future.result()
    #return future

async def put(client, namespace, set, key, record, meta, policy):
    global put_cqd

    def put_async_callback(key_tuple, err):
        global put_cqd
        (key) = key_tuple

        put_cqd -= 1
        result = [key_tuple, err]
        #print(result)
        loop = put_results[key[2]]['loop']
        fut = put_results[key[2]]['fut']
        
        loop.call_soon_threadsafe(fut.set_result, result)
        
        del put_results[key[2]]

    context = {'loop': None, 'fut': None}
    put_results[key["key"]] = context
    
    loop = asyncio.get_event_loop()
    put_results[key["key"]]['loop'] = loop
    
    future = loop.create_future()
    put_results[key["key"]]['fut'] = future
    
    put_cqd += 1
    
    client.put_async(put_async_callback, key, record, meta, policy)

    print(f"put_cqd: {put_cqd}")

    await future
    
    return future.result()
