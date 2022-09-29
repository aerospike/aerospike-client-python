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
from aerospike import exception as e

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
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

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Application
##########################################################################

try:

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    config = {
        'hosts': [(options.host, options.port)],
        'policies': {
            'total_timeout': options.timeout
        }
    }
    client = aerospike.client(config).connect(
        options.username, options.password)

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:
        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        smile = u"smilé"

        key = (namespace, set, smile)
        bins = {'smiley': smile, 'smile_count': 1, 'mood': 'happy'}
        print("Storing ", bins, "at a record identified by the tuple", key)
        # overwrite the record if it exists, otherwise create it
        client.put(key, bins,
                   policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
                           'key': aerospike.POLICY_KEY_SEND})
        print("Retrieving the record from the server for comparison")
        (key, meta, record) = client.get(
            key, policy={'total_timeout': options.read_timeout})
        print("The value of the 'smiley' bin is", record['smiley'], "\n")
        print("By the way, this record has been written", meta['gen'], "times")
        future_gen = str(int(meta['gen']) + 2)
        print("Expect the record generation to be",
              future_gen, "with two more write operations\n")

        # add a dictionary under a bin named 'data'
        bins = {'data': {'smiley_key': smile, smile: 'this is a smiley '}}
        print("Storing ", bins, "at the record", key)
        client.put(key,  bins)
        (key, metadata, bins) = client.get(key)
        print("The value of the 'smiley_key' of the 'data' bin is",
              bins['data']['smiley_key'], "\n")
        # print("The value of the", smile, " key is:",
        #      bins['data'][smile], "\n")

        # append to the value of the smile key
        print("Before appending, the value of the 'mood' key is:",
              bins['mood'])
        client.append(key, 'mood', smile)
        (key, metadata, bins) = client.get(key)
        print("After appending, the value of the 'mood' key is:",
              bins['mood'], "\n")

        # prepend to the value of the smile key
        print("Before prepending, the value of the 'mood' key is:",
              bins['mood'])
        client.prepend(key, 'mood', smile)
        (key, metadata, bins) = client.get(key)
        print("After prepending, the value of the 'mood' key is:",
              bins['mood'], "\n")

        # multiple operations on the record using the operate() method
        ops = [{'bin': 'smiley', 'op': aerospike.OPERATOR_APPEND, 'val': smile},
               {'bin': 'smile_count', 'op': aerospike.OPERATOR_INCR, 'val': 5},
               {'bin': 'smiley', 'op': aerospike.OPERATOR_READ}]
        print("Setting the following multiops on the same record\n", ops)
        (key, meta, bins) = client.operate(key, ops)
        print("The value of the 'smiley' bin is", bins['smiley'], "\n")

        print("Displaying the key, metadata, and bins of the record")
        (key, meta, bins) = client.get(key)
        print(key)
        print(meta)
        print(bins, "\n")
        client.remove(key)

        # example of a bytearray primary key
        print("Save a new record with a bytearray primary key")
        smiley_pk = smile.encode("utf-8")
        client.put((namespace, set, smiley_pk), {'smiley': smile, 'smiley_pk':
                                                 smiley_pk})
        print("Display the bins of a record with a bytearray key")
        (key, meta, bins) = client.get((namespace, set, smiley_pk))
        print(bins)
        print("The value of the 'smiley_pk' bin is", bins['smiley_pk'], "\n")
        client.remove(key)
        exitCode = 0
    except Exception as exception:
        print("error: {0}".format(exception), file=sys.stderr)
        exitCode = 2

    # ----------------------------------------------------------------------------
    # Close Connection to Cluster
    # ----------------------------------------------------------------------------

    client.close()

except e.ClientError as exception:
    print("Error: {0} [{1}]".format(exception.msg, exception.code))
    exitCode = 3

##########################################################################
# Exit
##########################################################################

sys.exit(exitCode)

#
