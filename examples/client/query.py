# -*- coding: utf-8 -*-
from __future__ import print_function

import aerospike
import re
import sys

from optparse import OptionParser
from aerospike import predicates as p

################################################################################
# Option Parsing
################################################################################

usage = "usage: %prog [options] [where]"

optparser = OptionParser(usage=usage, add_help_option=False)

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
    "-m", "--mode", dest="mode", type="choice", choices=["foreach","results"], default="foreach",
    help="Scan result processing mode.")

optparser.add_option(
    "-b", "--bins", dest="bins", type="string", action="append", 
    help="Bins to select from each record.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

if len(args) > 1:
    optparser.print_help()
    print()
    sys.exit(1)

################################################################################
# Client Configuration
################################################################################

config = {
    'hosts': [ (options.host, options.port) ]
}

################################################################################
# Application
################################################################################

exitCode = 0

try:

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    client = aerospike.client(config).connect()

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:

        re_bin = "(.{1,14})"
        re_str_eq = "\s+=\s*(?:(?:\"(.*)\")|(?:\'(.*)\'))"
        re_int_eq = "\s+=\s*(\d+)"
        re_int_rg = "\s+between\s+\(\s*(\d+)\s*,\s*(\d+)\s*\)"
        re_w = re.compile("%s(?:%s|%s|%s)" % (re_bin, re_str_eq, re_int_eq, re_int_rg))

        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None

        q = None

        if len(args) == 1:

            w = re_w.match(args[0])
            if w != None:

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

        if q == None:
            # If predicate not provided, then perform a scan
            q = client.scan(namespace, set)

        if options.bins and len(options.bins) > 0:
            # project specified bins
            q.select(*options.bins)

        records = []

        # callback to be called for each record read
        def callback((key, meta, record)):
            records.append(record)
            print(record)
        
        # invoke the operations, and for each record invoke the callback
        q.foreach(callback)
        
        print("---")
        if len(records) == 1:
            print("OK, 1 record found.")
        else:
            print("OK, %d records found." % len(records))

    except Exception, eargs:
        print("error: {0}".format(eargs), file=sys.stderr)
        exitCode = 2

    # ----------------------------------------------------------------------------
    # Close Connection to Cluster
    # ----------------------------------------------------------------------------

    client.close()

except Exception, eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    exitCode = 3


################################################################################
# Exit
################################################################################

sys.exit(rc)
