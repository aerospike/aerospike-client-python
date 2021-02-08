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
import json
import re
import sys
import os.path

from optparse import OptionParser
from aerospike import predicates as p

##########################################################################
# Option Parsing
##########################################################################

usage = "usage: %prog [options] where module function [args...]"

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
    "-b", "--bins", dest="bins", type="string", action="append",
    help="Bins to select from each record.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

if len(args) < 3:
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


def parse_arg(s):
    try:
        return json.loads(s)
    except ValueError:
        return s

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

        args.reverse()
        where = args.pop()
        module = args.pop()
        function = args.pop()

        # If predicate is provided, then perform a query
        q = client.query(namespace, set)
        w = re_w.match(where)
        if w is not None:
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

        if options.bins and len(options.bins) > 0:
            # project specified bins
            q.select(*options.bins)

        args.reverse()
        argl = list(map(parse_arg, args))
        print("argl == ", argl)
        q.apply(module, function, *argl)

        results = []

        # callback to be called for each record read
        def callback(result):
            results.append(result)
            print(result)

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
