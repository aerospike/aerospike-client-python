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
import random
import signal
import sys
import string
import time


from optparse import OptionParser
import copy
from tabulate import tabulate

# Guppy is only available in python 2.7, try to load it
try:
    from guppy import hpy
    HAVE_HEAPY = True
except ImportError:
    HAVE_HEAPY = False
# set maximum integer value compatible in python2 and 3
try:
    MAX_INT = sys.maxint
except AttributeError:
    MAX_INT = sys.maxsize

##########################################################################
# Options Parsing
##########################################################################

"""
TODO: change optparse to argparse
optparse is deprecated from Python V2.7
"""
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
    "--int-max", dest="int_max", type="int", default=MAX_INT,
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

optparser.add_option(
    "-l", "--latency", dest="latency", type="string", default="4,1",
    help="<number of latency columns>,<range shift increment> Show transaction latency percentages using elapsed time ranges.\
    <number of latency columns>: Number of elapsed time ranges. <range shift increment>: Power of 2 multiple between each range starting at column 3.\
    A latency definition of '-latency 7,1' results in this layout:\
        <=1ms >1ms >2ms >4ms >8ms >16ms >32ms\
           x%   x%   x%   x%   x%    x%    x%\
    A latency definition of '-latency 4,3' results in this layout:\
        <=1ms >1ms >8ms >64ms\
           x%   x%   x%    x%\
    Latency columns are cumulative. If a transaction takes 9ms, it will be included in both the >1ms and >8ms columns.")

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
# Generators
##########################################################################

READ_OP = 0
WRITE_OP = 1
CHOICE_OP = [READ_OP, WRITE_OP]

# Generator for operations


def operation(r, w):
    rn = r
    wn = w
    total = r + w
    while rn + wn > 0:
        if rn > 0 and wn > 0:
            op = random.randint(0, 100)
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


##########################################################################
# Application
##########################################################################

exitCode = 0

k = options.keys
r = options.reads
w = options.writes

total_count = 0
read_count = 0
write_count = 0
start = 0
intervals = []
if HAVE_HEAPY:
    heapy = hpy()

no_of_buckets, range_increment = [
    int(elem.strip()) for elem in options.latency.split(',')]

__buckets = [0, 1] + [elem << i for i in [0]
                      for elem in [1] * (no_of_buckets - 2) for i in [i + range_increment]]

# Separate buckets for read / write latency data
read_bucket = dict.fromkeys(__buckets, 0)
write_bucket = copy.deepcopy(read_bucket)

# return current time in ms
current_milliseconds_time = lambda: int(round(time.time() * 1000))

# returns latency table headers


def get_latency_table_headers():
    headers = ["", "<=1ms", ">1ms"] + \
        [">{0}ms".format(i) for i in __buckets[2:]]
    return headers

# manages the read / write operations counter in latency dictionaries


def increment_counters(bucket, time_in_millisecond):
    if time_in_millisecond <= 0:
        bucket[0] += 1
    for key in bucket.keys():
        if time_in_millisecond >= key > 0:
            bucket[key] += 1

# format the output as per tabulate standard and calculate percentages as well


def interprete_summary():
    global total_count, read_count, write_count, read_bucket, write_bucket

    read_summary = ['read'] + ['{0}%'.format(
        int(round(read_bucket[key] * 100 / read_count))) for key in read_bucket.keys()]
    write_summary = ['write'] + ['{0}%'.format(
        int(round(write_bucket[key] * 100 / write_count))) for key in write_bucket.keys()]

    return read_summary, write_summary


def total_summary():

    # stop time
    stop = time.time()

    # elapse time
    elapse = (stop - start)

    print()
    print("Summary:")
    print("     {0} keys generated".format(k))
    print("     {0} seconds for {1} operations".format(elapse, total_count))
    print("     {0} operations per second".format(total_count / elapse))
    print()
    print("Latency stats:")
    table = [info for info in interprete_summary()]
    print(tabulate(table, headers=get_latency_table_headers()))
    print()
    if HAVE_HEAPY:
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

        if HAVE_HEAPY:
            print("HEAP@{0}: {1}".format(0, heapy.heap()))

        # run the operatons
        for op in operation(r, w):

            key = (options.namespace, options.set,
                   keys[random.randint(0, k - 1)])

            if op == READ_OP:
                print('[READ] ', key) if options.verbose else 0
                now = current_milliseconds_time()
                result = client.exists(key)
                elapsed = current_milliseconds_time() - now
                print('[READ ELAPSED] {0}ms'.format(
                    elapsed)) if options.verbose else 0
                increment_counters(read_bucket, elapsed)
                total_count += 1
                read_count += 1

            elif op == WRITE_OP:
                print('[WRITE]', key) if options.verbose else 0
                rec = {
                    'key': key[2]
                }
                now = current_milliseconds_time()
                client.put(key, rec)
                elapsed = current_milliseconds_time() - now
                print('[WRITE ELAPSED] {0}ms'.format(
                    elapsed)) if options.verbose else 0
                increment_counters(write_bucket, elapsed)
                total_count += 1
                write_count += 1

            if options.heap_interval > 0 and (total_count % options.heap_interval) == 0:
                if HAVE_HEAPY:
                    print("HEAP@{0}: {1}".format(total_count, heapy.heap()))

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
