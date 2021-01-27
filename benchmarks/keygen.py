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
import signal
import sys
import time

# from guppy import hpy
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
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Namespace that records will be stored and retrieved from.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Set that records will be stored and retrieved from.")

optparser.add_option(
    "-v", "--verbose", dest="verbose", action="store_true", metavar="<PORT>",
    help="Verbose output.")


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

count = 0
start = 0


def total_summary():

    # stop time
    stop = time.time()

    # elapse time
    elapse = (stop - start)

    print()
    print("Summary:")
    print("     {0} keys generated".format(count))
    print("     {0} seconds for {1} operations".format(elapse, count))
    print("     {0} operations per second".format(count / elapse))
    print()
    # print("Heap: ")
    # print(heapy.heap())

    sys.exit(0)

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

        signal.signal(signal.SIGTERM, total_summary)

        print()
        print("Press CTRL+C to quit.")
        print()

        start = time.time()

        # run the operatons
        while True:
            count += 1
            keyt = (options.namespace, options.set, count)
            client.put(keyt, {'key': count})

    except KeyboardInterrupt:
        total_summary()
    except Exception as eargs:
        print("error: {0}".format(eargs), file=sys.stderr)
        sys.exit(2)

except Exception as eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    sys.exit(3)

##########################################################################
# Exit
##########################################################################

sys.exit(0)
