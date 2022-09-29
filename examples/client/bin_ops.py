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
import pprint
import sys

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

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
    "--help", dest="help", action="store_true",
    help="Displays this message.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Application
##########################################################################

exitCode = 0

try:
    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    config = {'hosts': [(options.host, options.port)]}
    client = aerospike.client(config).connect(
        options.username, options.password)
    # ----------------------------------------------------------------------------
    # Perform Operations
    # ----------------------------------------------------------------------------

    try:
        pp = pprint.PrettyPrinter(indent=2)
        client.put(('test', 'cats', 'mr. peppy'), {'breed': 'persian'},
                   policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
                           'key': aerospike.POLICY_KEY_DIGEST},
                   meta={'ttl': 120})
        (key, meta, bins) = client.get(('test', 'cats', 'mr. peppy'))
        print("Before:", bins)
        client.increment(
            key, 'lives', -1, {'gen': 2, 'ttl': 1000}, policy={'total_timeout': 1500})
        (key, meta, bins) = client.get(key)
        print("After:", bins)
        # the key we got back when we fetched the record with get() is useable
        # as-is because it contains the record's digest
        client.increment(key, 'lives', -1)
        (key, meta, bins) = client.get(key)
        # kitty lost a life, unfortunately
        print("Poor Kitty:", bins)
        client.put(key, {'owner': 'Fry'})
        client.prepend(key, 'owner', 'Philip J. ')
        client.append(key, 'owner', ' Esq.')
        # kitty loses another life, gains a color, all as part of a record
        # multi-op
        ops = [{'bin': 'color', 'op': aerospike.OPERATOR_WRITE, 'val': 'smoke'},
               {'bin': 'lives', 'op': aerospike.OPERATOR_INCR, 'val': -1},
               {'bin': 'ailments', 'op': aerospike.OPERATOR_READ},
               {'bin': 'lives', 'op': aerospike.OPERATOR_READ}]
        (key, meta, bins) = client.operate(key, ops)
        print("After calling operate(), kitty is down to",
              bins['lives'], "lives")
        pp.pprint(bins)

        # display the record as it is after all the operations
        (key, meta, bins) = client.get(key)
        print("\nRecord\n======\nKey\n---")
        pp.pprint(key)
        print("Meta\n----")
        pp.pprint(meta)
        print("Bins\n----")
        pp.pprint(bins)

    except Exception as eargs:
        print("error: {0}".format(eargs), file=sys.stderr)
        exitCode = 2

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
