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
    help="Number of test cases to run.")

optparser.add_option(
    "-q", "--qd", dest="qd", type="int", default=128, metavar="<QUEUE_DEPTH>",
    help="Async IO queue depth.")

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
        qd = options.qd
        cqd = 0
        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        policy = {
            'total_timeout': options.timeout
        }
        meta = None
        print(f"IO test count:{test_count} IO-QueueDepth {qd}")

        def get_async_callback(key_tuple, record_tuple, err):
            global count, cqd
            (key) = key_tuple
            (record) = record_tuple
            bins = []
            if record != None:
                (_,_,bins) = record
            count += 1
            cqd -= 1
            get_results[key[2]]['state'] = 1
            get_results[key[2]]['bins'] = bins
            get_results[key[2]]['err'] = err
            

        async def get_async(namespace, set, key, policy):
            client.get_async(get_async_callback, key, policy)
 
        async def async_io(namespace, set, test_count):
            global cqd, count
            assert (cqd == 0)
            count = 0        
            for i in range(0, test_count):
                key = {'ns': namespace, \
                        'set':set, \
                        'key': str(i), \
                        'digest': client.get_key_digest(namespace, set, str(i))}
                context = {'state': 0, 'bins': {}} # 0->i/o_issued, 1->i/o_success, 2->i/o failure
                get_results[key["key"]] = context
                client.get_async(get_async_callback, key, policy)
                    
                #await get_async(namespace, set, key, policy)
                cqd += 1
                # maintain and/or dont overload IO queue
                while cqd > qd:
                    print(f"{test_count} I/O are issued so far")
                    print(f"Outstanding I/Os ({cqd}) are greater than QD {qd}, wait for it to drop before issuing additional I/Os")
                    await asyncio.sleep(1)
                    print(f"cqd dropped to {cqd}")
            # make sure all IO drained before verifying data with next OP
            while cqd:
                print(f"{test_count} I/O are issued, waiting for callback")
                await asyncio.sleep(1)
            print(f"{test_count} I/O are completed successfully")

        asyncio.run(async_io(namespace, set, test_count))
        print(f"get_async completed with returning {len(get_results)} records")
        print(get_results)
    
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
