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
import string
import time

from guppy import hpy
from threading import Timer
from optparse import OptionParser

################################################################################
# Options Parsing
################################################################################

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
    help="Port of the Aerospike server.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-v", "--verbose", dest="verbose", action="store_true", metavar="<PORT>",
    help="Verbose output.")

optparser.add_option(
    "--gen", dest="gen", type="string", default="int",
    help="Key and value generator: int | str")

optparser.add_option(
    "--str-min", dest="str_min", type="int", default=10,
    help="Minimum length for generated strings.")

optparser.add_option(
    "--str-max", dest="str_max", type="int", default=30,
    help="Maximum length for generated strings.")

optparser.add_option(
    "--str-chars", dest="str_chars", type="string", default=string.ascii_uppercase + string.digits,
    help="Charset for generated strings.")

optparser.add_option(
    "--int-min", dest="int_min", type="int", default=0,
    help="Minimum value for generated integers.")

optparser.add_option(
    "--int-max", dest="int_max", type="int", default=sys.maxint,
    help="Maximum value for generated integere.")

optparser.add_option(
    "--heap", dest="heap", action="store_true",
    help="Check the heap. Produce a heap report at the end of the run.")

optparser.add_option(
    "--heap-interval", dest="heap_interval", type="int", default=0,
    help="Display heap report after every n operations.")

optparser.add_option(
    "--reads", dest="reads", type="int", default=80,
    help="Read ratio, as an integer between 0 and 100")

optparser.add_option(
    "--writes", dest="writes", type="int", default=20,
    help="Write ratio, as an integer between 0 and 100")

optparser.add_option(
    "--keys", dest="keys", type="int", default=1000000,
    help="Number of unique keys")

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

def genstr():
    return ''.join(random.choice(options.str_chars) for _ in range(random.randrange(options.str_min, options.str_max, 1)))

def genint():
    return random.randrange(options.int_min, options.int_max, 1)


################################################################################
# Application
################################################################################

exitCode = 0

k = options.keys
r = options.reads
w = options.writes

count = 0
start = 0
intervals = []
heapy = hpy()

def total_summary():

    # stop time
    stop = time.time()

    # elapse time
    elapse = (stop - start)

    print()
    print("Summary:")
    print("     {0} keys generated".format(k))
    print("     {0} seconds for {1} operations".format(elapse, count))
    print("     {0} operations per second".format(count / elapse))
    print()
    print("Heap: ")
    print(heapy.heap())

    sys.exit(0)

try:

    # ----------------------------------------------------------------------------
    # Generate Keys
    # ----------------------------------------------------------------------------

    if options.gen == 'str':
        gen = genstr
    else:
        gen = genint

    print("Generating keys: ", k)
    keys = [gen() for _ in range(k)]
    print("done.")
    print()

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    client = aerospike.client(config).connect(options.username, options.password)

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:

        signal.signal(signal.SIGTERM, total_summary)

        print()
        print("Press CTRL+C to quit.")
        print()

        start = time.time()

        print("HEAP@{0}: {1}".format(0, heapy.heap()))

        # run the operatons
        for op in operation(r, w):

            key = (options.namespace, options.set, keys[random.randint(0,k)])

            if op == READ_OP:
                print('[READ] ', key) if options.verbose else 0
                result = client.exists(key)
                count += 1

            elif op == WRITE_OP:
                print('[WRITE]', key) if options.verbose else 0
                rec = {
                    'key': key[2]
                }
                client.put(key, rec)
                count += 1

            if options.heap_interval > 0 and (count % options.heap_interval) == 0:
                print("HEAP@{0}: {1}".format(count, heapy.heap()))

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
