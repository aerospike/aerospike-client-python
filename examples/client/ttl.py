
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
TTL_MAX = 20

# TODO: include instructions to have docker commands to set up server instead of through python
# Define the Namespace Supervisor parms -- setting the period very short
# so that we know it will have visited all of our records before we look
# at them at each TTL interval.
# PARAMS_SERVICE = [[('nsup-period', 1)]]

# Define the default Namespace Time To Live at 10 seconds. We will write
# some records that expire EARLY (5 seconds), some records that expire at
# the default (10 seconds), some that expire LATE (15 seconds) and some
# that NEVER expire.
# We set MAX ttl to 20 to check that our flag (0xFFFFFFFF) is allowed in
# and does not trigger the "greater than max ttl" warning.
# Also, we'll check that one of our records DOES trigger the Max TTL warning
# with a TTL of greater than 20.
# PARAMS_NAMESPACE = [[('default-ttl', TTL_DEFAULT)]]

# TODO: needs to be updated
# Write Policy names and related values
AS_POLICY_W_TIMEOUT = "timeout"

# TODO: don't know what this is for...
AS_POLICY_KEY_STORE = 3  # Store the key (NOT YET IMPLEMENTED)

# TODO: verify this works?
AS_POLICY_GEN_DUP = 4  # Write a record creating a duplicate, ONLY if
# the generation collides (?)


# Setup write policy
wr_policy = {
    "total_timeout": 5000,
    "max_retries":   0,
    "key":     aerospike.POLICY_KEY_DIGEST,
    "gen":     aerospike.POLICY_GEN_IGNORE,
    "exists":  aerospike.POLICY_EXISTS_IGNORE
}

PRIMARY_KEYS_WITHOUT_TTL = list(range(1, 11))

KEYS_WITH_TTL = {
    20: {'ttl': 5,
         'desc': '5 sec TTL'},
    40: {'ttl': 15,
         'desc': '15 sec TTL'},
    60: {'ttl': aerospike.TTL_NEVER_EXPIRE,
         'desc': 'NO_EXPIRE TTL'},
    80: {'ttl': TTL_MAX + 1,
         'desc': 'Larger than MAX TTL'}
}

ALL_KEYS = PRIMARY_KEYS_WITHOUT_TTL + list(KEYS_WITH_TTL.keys())

def print_record(record_tuple, prefix=''):
    (key, meta, bins) = record_tuple
    print("%s%-4d %-4s %-8s %s" % (
        prefix,
        int(key[2] or 0),
        meta.get('gen') if meta and 'gen' in meta else '-',
        meta.get('ttl') if meta and 'ttl' in meta else '-',
        bins if bins else '-'
    ))


def print_records(records, prefix=''):
    header = "%s---- ---- -------- " % prefix
    header = header.ljust(80, '-')
    print()
    print("%s%-4s %-4s %-8s %s" % (prefix, "key", "gen", "ttl", "record"))
    print(header)
    [print_record(r, prefix) for r in records]


def print_header(header, message=None):
    print()
    print(''.ljust(80, '='))
    print(header)
    print(message) if message else None
    print(''.ljust(80, '-'))

def print_histogram(prefix=''):
    request = ''.join(["histogram:ns=", options.namespace, ";hist=ttl"])

    header = "%sHISTOGRAM (%s)" % (prefix, request)
    border = prefix.ljust(80, '-')

    print()
    print(header)
    print(border)
    for _, (error, response) in list(client.info(request).items()):
        if error:
            print('%serror: %s' % (prefix, error))
        else:
            for line in textwrap.wrap(response, 80 - len(prefix)):
                print("%s%s" % (prefix, line))


def check_records(start, wait=0, message=None):

    if wait:
        time.sleep(wait)

    stop = time.time()
    duration = int(stop - start)

    print_header('CHECK :: wait=%s duration=%s' % (wait, duration), message)

    try:
        print_records(
            [client.get((options.namespace, options.set, k)) for k in ALL_KEYS], '  ')
    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)

    print_histogram('  ')


def write_records():

    try:
        for key in ALL_KEYS:
            ttl = KEYS_WITH_TTL[key]['ttl'] if key in KEYS_WITH_TTL else None

            rec = {}
            rec['key'] = key
            rec['ttl'] = ttl if ttl else TTL_DEFAULT
            rec['desc'] = KEYS_WITH_TTL[key][
                'desc'] if key in KEYS_WITH_TTL else 'default TTL'

            try:
                # write a new record
                # ttl=None is equivalent to not setting a ttl
                print("writing key :=", key)
                client.put(
                    (options.namespace, options.set, key), rec, {'ttl': ttl})

            except Exception as e:
                ttlVal = int(ttl or 0)
                if ttlVal > TTL_MAX:
                    print('error: (correct) failed to write record with TTL(%d) > TTL_MAX(%d)' % (
                        ttlVal, TTL_MAX))
                else:
                    print('error: failed to write record with TTL = %d ' %
                          ttlVal)

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        sys.exit(1)

##########################################################################
# CONFIGURE SERVER
##########################################################################

# TODO: use docker container

from .. import Example

class TTL(Example):
    def run(self):
        start = time.time()

        check_records(start, 0, 'Clean state')

        write_records()

        check_records(start, 0, 'Initial state')
        check_records(start, 2, 'Expect all records with TTL-2')
        check_records(start, 6, 'Expect all records with TTL<=5 to be gone')
        check_records(start, 3, 'Expect all records with TTL<=10 to be gone')
        check_records(start, 6, 'Expect all records to be gone, except NO_EXPIRE')
