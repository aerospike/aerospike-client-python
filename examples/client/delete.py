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

import asyncio
import sys
import aerospike
import array

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--timeout", dest="timeout", type="int", default=1000, metavar="<MS>",
    help="Client timeout")

optparser.add_option(
    "-c", "--test_count", dest="test_count", type="int", default=128, metavar="<TEST_COUNT>",
    help="Number of test cases to run.")

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)],
    'policies': {
        'total_timeout': options.timeout
    }
}

##########################################################################
# Application
##########################################################################

exitCode = 0

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
        test_count = options.test_count
        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        policy = {
            'total_timeout': options.timeout
        }
        meta = None
        print(f"IO test count:{test_count}")
        def delete(namespace, set, test_count):
            for i in range(0, test_count):
                key = {'ns': namespace, \
                        'set':set, \
                        'key': str(i), \
                        'digest': aerospike.calc_digest(namespace, set, str(i))}

                policy = None

                client.remove(key)


        delete(namespace, set, test_count)
        print(f"Deleted {test_count} records")

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        rc = 1

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
