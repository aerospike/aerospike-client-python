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
import re
import sys
import os.path

from optparse import OptionParser
from aerospike import predicates as p

##########################################################################
# Option Parsing
##########################################################################

usage = "usage: %prog [options] [where]"

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

optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-m", "--module", dest="module", type="string",
    help="UDF Module.")

optparser.add_option(
    "-f", "--function", dest="function", type="string",
    help="UDF Function.")

optparser.add_option(
    "-a", "--arg", dest="arguments", action="append", type="string",
    help="UDF Arguments.")

optparser.add_option(
    "-b", "--bins", dest="bins", type="string", action="append",
    help="Bins to select from each record.")

optparser.add_option(
    "--show-key", dest="show_key", action="store_true",
    help="If set, displays the key/digest.")

optparser.add_option(
    "--show-meta", dest="show_meta", action="store_true",
    help="If set, displays the metadata.")


(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

if len(args) > 1:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)],
    'lua': {
        'user_path': os.path.dirname(__file__)
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

        re_bin = "(.{1,14})"
        re_str_eq = "\s+=\s*(?:(?:\"(.*)\")|(?:\'(.*)\'))"
        re_int_eq = "\s+=\s*(\d+)"
        re_int_rg = "\s+between\s+\(\s*(\d+)\s*,\s*(\d+)\s*\)"
        re_w = re.compile("%s(?:%s|%s|%s)" %
                          (re_bin, re_str_eq, re_int_eq, re_int_rg))

        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None

        q = None

        if len(args) == 1:

            w = re_w.match(args[0])
            if w is not None:

                # If predicate is provided, then perform a query
                q = client.query(namespace, set)

                if w.group(2):
                    b = w.group(1)
                    v = w.group(2)
                    q.where(p.equals(b, v))
                elif w.group(3):
                    b = w.group(1)
                    v = w.group(3)
                    q.where(p.equals(b, v))
                elif w.group(4):
                    b = w.group(1)
                    v = int(w.group(4))
                    q.where(p.equals(b, v))
                elif w.group(5) and w.group(6):
                    b = w.group(1)
                    l = int(w.group(5))
                    u = int(w.group(6))
                    q.where(p.between(b, l, u))

        if q is None:
            # If predicate not provided, then perform a scan
            q = client.scan(namespace, set)

        if options.bins and len(options.bins) > 0:
            # project specified bins
            q.select(*options.bins)

        if options.module and options.function:
            if options.arguments:
                q.apply(options.module, options.function, *options.arguments)
            else:
                q.apply(options.module, options.function)

        results = []

        # callback to be called for each record read
        def callback(input_tuple):
            (key, meta, rec) = input_tuple
            results.append((key, meta, rec))
            if options.show_key and options.show_meta:
                print(key, meta, rec)
            elif options.show_key:
                print(key, rec)
            elif options.show_meta:
                print(meta, rec)
            else:
                print(rec)

        # invoke the operations, and for each record invoke the callback
        q.foreach(callback)

        print("---")
        if len(results) == 1:
            print("OK, 1 result found.")
        else:
            print("OK, %d results found." % len(results))

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
