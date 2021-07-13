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
import time
from aerospike_helpers.awaitable import io

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

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
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Namespace of database.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Set to use within namespace of database.")

optparser.add_option(
    "-c", "--test_count", dest="test_count", type="int", default=128, metavar="<TEST_COUNT>",
    help="Number of async IO to spawn.")

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
    print(f"Connecting to {options.host}:{options.port} with {options.username}:{options.password}")
    client = aerospike.client(config).connect(
        options.username, options.password)

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:
        io_results = {}
        test_count = options.test_count
        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        policy = {
            'total_timeout': options.timeout
        }
        meta = None

        print(f"IO async test count:{test_count}")
        
        async def async_io(namespace, set, i):
            key = (namespace, \
                    set, \
                    str(i), \
                    client.get_key_digest(namespace, set, str(i)))
            context = {'result': {}}
            io_results[key[2]] = context
            result = None
            try:
                result = await io.get(client, key, policy)
            except Exception as eargs:
                print(f"error: {eargs.code}, {eargs.msg}, {eargs.file}, {eargs.line}")
            print(result)
            io_results[key[2]]['result'] = result
        async def main():
            func_list = []
            for i in range(test_count):
                func_list.append(async_io(namespace, set, i))
            await asyncio.gather(*func_list)
        asyncio.get_event_loop().run_until_complete(main())
        print(io_results)
        print(f"get_async completed with returning {len(io_results)} records")
    except Exception as e:
        print(f"error: {0} ".format(e), file=sys.stderr)
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
