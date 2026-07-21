
##########################################################################
# Copyright 2013-2026 Aerospike, Inc.
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


import aerospike
import sys
import random
import time
import threading

from .. import Example
from aerospike import exception as e


class Multithread(Example):

    numKeys = 10000
    numReads = 1000000
    fNames = ('Jimmy', 'Johnny', 'Sammy', 'Sally', 'Sandy', 'Mandy', 'Billy')
    lNames = ('Bama', 'Mama', 'Sama', 'Lama', 'Cama', 'Rama', 'Tama')
    numThreads = 5


    def writeWork(self, nKeys):
        t0 = float(time.time())

        for x in range(0, nKeys):
            kstr = 'k' + str(x)
            key = (self.namespace, self.set_name, kstr)

            try:
                # Write a record
                self.client.put(key, {
                    'name': random.choice(self.fNames) + ' ' + random.choice(self.lNames),
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


    def readWork(self, nReads, thrName):
        print('Thread #{0} is starting to read {1} records'.format(
                                    thrName, nReads))

        # Read records
        t0 = float(time.time())

        for x in range(0, nReads):
            kstr = 'k' + str(random.randrange(0, self.numKeys))
            key = (self.namespace, self.set_name, kstr)
            try:
                (key, _, _) = self.client.get(key)
            except aerospike.exception.ClientError as e:
                print('Aerospike Error: {0} [{1}]'.format(e.msg, e.code))
                return None

            if x % 10000 == 0 and x > 0:
                print('Thread #{0} : Read {1} records at T = {2:.2f} sec'.format(
                            thrName, x, float(time.time()) - t0))
        print('Thread #{0} : Read {1} records at T = {2:.2f} sec'.format(
                            thrName, nReads, float(time.time()) - t0))


    def run(self):
        print('Writing data into Aerospike DB')
        self.writeWork(self.numKeys)

        print('Reading data from Aerospike DB using {0} threads'.format(self.numThreads))
        t = []

        for i in range(self.numThreads):
            thread = threading.Thread(target=self.readWork,
                                    args=(self.numReads // self.numThreads, str(i)))
            thread.start()
            t.append(thread)

        for i in range(self.numThreads):
            t[i].join()
