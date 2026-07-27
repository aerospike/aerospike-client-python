
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


# Test the TTL feature for the Aerospike Server.
# We will write records that expire BEFORE the default namespace TTL,
# we will write records that expire with the default TTL, and we'll
# write records that NEVER expire.


import aerospike
import sys
import textwrap
import re
import time

from aerospike import exception as e

TTL_DEFAULT = 10

# TODO: include instructions to have docker commands to set up server instead of through python
# Define the Namespace Supervisor parms -- setting the period very short
# so that we know it will have visited all of our records before we look
# at them at each TTL interval.
# PARAMS_SERVICE = [[('nsup-period', 1)]]

# Define the default Namespace Time To Live at 10 seconds. We will write
# some records that expire EARLY (5 seconds), some records that expire at
# the default (10 seconds), some that expire LATE (15 seconds) and some
# that NEVER expire.
# PARAMS_NAMESPACE = [[('default-ttl', TTL_DEFAULT)]]


USER_KEYS_TO_TTL = {
    5: 5,
    15: 15,
    "ns_default": aerospike.TTL_NAMESPACE_DEFAULT,
    "dont_expire": aerospike.TTL_NEVER_EXPIRE,
}

from .. import Example

class TTL(Example):
    def run(self):
        self.KEYS = [(self.namespace, self.set_name, key) for key in USER_KEYS_TO_TTL]
        self.time_elapsed = 0
        self.write_records()
        self.check_records(0, 'Initial state')
        self.check_records(2, 'Expect all records with TTL<=2 to be gone.')
        self.check_records(6, 'Expect all records with TTL<=5 to be gone')
        self.check_records(3, 'Expect all records with TTL<=10 to be gone')
        self.check_records(6, 'Expect all records to be gone, except NO_EXPIRE')

    def __del__(self):
        self.client.batch_remove(self.KEYS)
        super().__del__()

    def print_histogram(self):
        request = f"histogram:namespace={self.namespace};type=ttl"
        response = self.client.info_random_node(request)
        print("Server TTL histogram:", response)

    def check_records(self, wait=0, message=None):
        if wait:
            time.sleep(wait)
            print(f"Waited {wait} seconds")
            self.time_elapsed += wait

        print(f"Total elapsed time is {self.time_elapsed}. {message}")
        brs = self.client.batch_read(self.KEYS)
        for br in brs.batch_records:
            print(f"Server returned error code {br.result} for record with ttl of {br.key[2]}")

        self.print_histogram()


    def write_records(self):
        for key in self.KEYS:
            print("writing key :=", key)
            user_key = key[2]
            self.client.put(key, {self.BIN_NAME: 1}, policy={"ttl": USER_KEYS_TO_TTL[user_key]})
