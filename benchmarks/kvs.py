# -*- coding: utf-8 -*-
################################################################################
# Copyright 2013-2014 Aerospike, Inc.
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
################################################################################

from __future__ import print_function

import aerospike
import random
import signal
import sys
import time

from threading import Timer
from optparse import OptionParser

################################################################################
# Options Parsing
################################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-v", "--verbose", dest="verbose", action="store_true", metavar="<PORT>",
    help="Verbose output.")

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

(options, args) = optparser.parse_args()

if options.help:
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
# Generators
################################################################################

READ_OP = 0
WRITE_OP = 1
CHOICE_OP = [READ_OP, WRITE_OP]

# Generator for operations
def operation(r, w):
    rn = r
    wn = w
    while rn + wn > 0:
        if rn > 0 and wn > 0:
            op = random.randint(0,100)
            if op <= r:
                op = READ_OP
                rn -= 1
            else:
                op = WRITE_OP
                wn -= 1
            if wn == 0 and rn == 0:
                rn = r
                wn = w
            yield op
        elif rn > 0:
            op = READ_OP
            rn -= 1
            if wn == 0 and rn == 0:
                rn = r
                wn = w
            yield op
        elif wn > 0:
            op = WRITE_OP
            wn -= 1
            if wn == 0 and rn == 0:
                rn = r
                wn = w
            yield op


################################################################################
# Application
################################################################################

exitCode = 0

k = 1000000
r = 80
w = 20

count = 0
start = 0
intervals = []

def total_summary():

    # stop time
    stop = time.time()

    # elapse time
    elapse = (stop - start)

    print()
    print("Summary:")
    print("     {0} seconds for {1} operations".format(elapse, count))
    print("     {0} operations per second".format(count / elapse))
    print()

    sys.exit(0)

try:

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    client = aerospike.client(config).connect()

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
        for op in operation(r, w):

            if op == READ_OP:

                key = random.randrange(1, k, 1)
                
                print('[READ] ', key) if options.verbose else 0
                
                (key, metadata, rec) = client.get(('test','demo',key))
                count += 1

            elif op == WRITE_OP:

                key = random.randrange(1, k, 1)

                print('[WRITE]', key) if options.verbose else 0

                rec = {
                    'key': key
                }

                client.put(('test','demo',key), rec)
                count += 1


    except KeyboardInterrupt:
        total_summary()
    except Exception, eargs:
        print("error: {0}".format(eargs), file=sys.stderr)
        sys.exit(2)

except Exception, eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    sys.exit(3)

################################################################################
# Exit
################################################################################

sys.exit(0)
