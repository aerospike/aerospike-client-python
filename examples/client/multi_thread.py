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
import random
import time
import threading

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
    "-h", "--host", dest="host", type="string", default="127.0.0.1",
    metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)],
    'lua': {'user_path': '.'}
}

##########################################################################
# Application
##########################################################################

try:
    client = aerospike.client(config).connect(
        options.username, options.password)
except e.ClientError as exception:
    print('Error: {0} [{1}]'.format(exception.msg, exception.code))
    sys.exit(1)

namespace = 'test'
testSet = 'test'
numKeys = 10000
numReads = 1000000
fNames = ('Jimmy', 'Johnny', 'Sammy', 'Sally', 'Sandy', 'Mandy', 'Billy')
lNames = ('Bama', 'Mama', 'Sama', 'Lama', 'Cama', 'Rama', 'Tama')
numThreads = 5


def writeWork(nKeys):
    t0 = float(time.time())

    for x in range(0, nKeys):
        kstr = 'k' + str(x)
        key = (namespace, testSet, kstr)

        try:
            # Write a record
            client.put(key, {
                'name': random.choice(fNames) + ' ' + random.choice(lNames),
                'age': random.randint(10, 100),
                'value': x
            })
        except Exception as e:
            print('write error {0}'.format(e))

        if x % 1000 == 0 and x > 0:
            print('Wrote {0} records at T = {1:.2f} sec'.format(
                                     x, float(time.time()) - t0))

    print('Wrote {0} records at T = {1:.2f} sec'.format(
                        nKeys, float(time.time()) - t0))


def readWork(nReads, thrName):
    print('Thread #{0} is starting to read {1} records'.format(
                                thrName, nReads))

    # Read records
    t0 = float(time.time())

    for x in range(0, nReads):
        kstr = 'k' + str(random.randrange(0, numKeys))
        key = (namespace, testSet, kstr)
        try:
            (key, _, _) = client.get(key)
        except aerospike.exception.ClientError as e:
            print('Aerospike Error: {0} [{1}]'.format(e.msg, e.code))
            return None

        if x % 10000 == 0 and x > 0:
            print('Thread #{0} : Read {1} records at T = {2:.2f} sec'.format(
                        thrName, x, float(time.time()) - t0))
    print('Thread #{0} : Read {1} records at T = {2:.2f} sec'.format(
                        thrName, nReads, float(time.time()) - t0))


print('Writing data into Aerospike DB')
writeWork(numKeys)

print('Reading data from Aerospike DB using {0} threads'.format(numThreads))
t = []

for i in range(numThreads):
    thread = threading.Thread(target=readWork,
                              args=(numReads // numThreads, str(i)))
    thread.start()
    t.append(thread)

for i in range(numThreads):
    t[i].join()

print('Finished. Closing Aerospike connection.')
client.close()
