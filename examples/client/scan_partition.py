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

from __future__ import print_function

import aerospike
import sys

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-i", "--partition_id", dest="partition", type="int", default=0,
    help="Partition id from where to scan.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)]
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

        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None

        s = client.scan(namespace, set)

        partition_policy = None

        if options.partition > 0:
            # project specified bins
            partition_policy = {'partition_filter': {'begin': options.partition, 'count': 1}}
            print(f'partition_id: {options.partition}')
    
        records = []

        # callback to be called for each record read
        def callback(input_tuple):
            (_, _, record) = input_tuple
            records.append(record)
            print(record)
        
        client.truncate('test', None, 0)

        # invoke the operations, and for each record invoke the callback
        s.foreach(callback, partition_policy)
        existing_count = len(records)
        if existing_count > 0:
            print(f"{existing_count} records are exist already in partition:{options.partition}.")

        count = 0
        for i in range(1, 80000):
            rec_partition = client.get_key_partition_id('test', 'demo', str(i))

            if rec_partition == options.partition: # and not client.exists(('test', 'demo', str(i))):
                
                count = count + 1
                rec = {
                    'i': i,
                    's': 'xyz',
                    'l': [2, 4, 8, 16, 32, None, 128, 256],
                    'm': {'partition': rec_partition, 'b': 4, 'c': 8, 'd': 16}
                }
                client.put(('test', 'demo', str(i)), rec)
        
        records.clear()
        # invoke the operations, and for each record invoke the callback
        s.foreach(callback, partition_policy)

        print("---")
        print(f"{count} records are put into partition:{options.partition}.")
        print(f"{len(records)} records are found in partition:{options.partition}.")
 
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
