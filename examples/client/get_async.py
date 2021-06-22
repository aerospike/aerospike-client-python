# -*- coding: utf-8 -*-
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

import asyncio
import sys
import aerospike
import array
#import pdb; pdb.set_trace()

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

#breakpoint()

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-U", "--username", dest="username", type="string",  default="ram", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", default="ram", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="as-s1.as-network.com", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "--timeout", dest="timeout", type="int", default=1000, metavar="<MS>",
    help="Client timeout")

optparser.add_option(
    "--read-timeout", dest="read_timeout", type="int", default=1000, metavar="<MS>",
    help="Client read timeout")

optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-c", "--test_count", dest="test_count", type="int", default=128, metavar="<TEST_COUNT>",
    help="Port of the Aerospike server.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)],
    'policies': {
        'total_timeout': options.timeout
    }
}

##########################################################################
# Application
##########################################################################

exitCode = 0

try:

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    client = aerospike.client(config).connect(
        options.username, options.password)

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:
        get_results = {}
        count = 0
        test_count = options.test_count
        result_array = array.array('i',(0 for i in range(0,test_count)))

        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        policy = {
            'total_timeout': options.read_timeout
        }

        def sample_puts(namespace, set, test_count):
            for i in range(0, test_count):
                key = {'ns': namespace, \
                        'set':set, \
                        'key': str(i), \
                        'digest': client.get_key_digest(namespace, set, str(i))}
                record = {
                    'i': i,
                    'f': 3.1415,
                    's': 'abc',
                    'u': '안녕하세요',
                    #  'b': bytearray(['d','e','f']),
                    #  'l': [i, 'abc', bytearray(['d','e','f']), ['x', 'y', 'z'], {'x': 1, 'y': 2, 'z': 3}],
                    #  'm': {'i': i, 's': 'abc', 'u': '안녕하세요', 'b': bytearray(['d','e','f']), 'l': ['x', 'y', 'z'], 'd': {'x': 1, 'y': 2, 'z': 3}},
                    'l': [i, 'abc', 'வணக்கம்', ['x', 'y', 'z'], {'x': 1, 'y': 2, 'z': 3}],
                    'm': {'i': i, 's': 'abc', 'u': 'ஊத்தாப்பம்', 'l': ['x', 'y', 'z'], 'd': {'x': 1, 'y': 2, 'z': 3}}
                }
                meta = None
                policy = None
                client.put(key, record)

        def get_async_callback(input_tuple):
            global count, result_array
            (key, _, record) = input_tuple
            print(key)
            print(record)
            count += 1
            # print("cb " + result_array[int(key['key'])])
            # result_array[int(key['key'])] = 1
            # print("cb done " + result_array[int(key['key'])])

        async def get_async(namespace, set, key, policy):
            client.get_async(get_async_callback, key, policy)
 
        async def many_gets(namespace, set, test_count):        
            for i in range(0, test_count):
                key = {'ns': namespace, \
                        'set':set, \
                        'key': str(i), \
                        'digest': client.get_key_digest(namespace, set, str(i))}
                client.get_async(get_async_callback, key, policy)
                #await get_async(namespace, set, key, policy)
            while count != test_count:
                print(count)
                await asyncio.sleep(1)
                print(count)

        sample_puts(namespace, set, test_count)
        asyncio.run(many_gets(namespace, set, test_count))
    
    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        rc = 1

    # ----------------------------------------------------------------------------
    # Close Connection to Cluster
    # ----------------------------------------------------------------------------

    client.close()

except Exception as eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    exitCode = 3

##########################################################################
# Exit
##########################################################################

sys.exit(exitCode)
