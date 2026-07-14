
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



import aerospike
import sys

from optparse import OptionParser

##########################################################################
# Option Parsing
##########################################################################

usage = "usage: %prog [options] key"

optparser.add_option(
    "--gen", dest="gen", type="int", default=10, metavar="<GEN>",
    help="Generation of the record being written.")

optparser.add_option(
    "--ttl", dest="ttl", type="int", default=1000, metavar="<TTL>",
    help="TTL of the record being written.")


if len(args) != 1:
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

        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        key = args.pop()

        record = {
            'example_name': 'John',
            'example_age': 1
        }

        meta = {'ttl': options.ttl, 'gen': options.gen}
        policy = None

        # invoke operation

        client.put((namespace, set, key), record, meta, policy)

        print("---")
        print("OK, 1 record written.")

        (returnedkey, meta, bins) = client.get((namespace, set, key))

        print("---")
        print("Before increment operation")
        print(bins)

        client.increment((namespace, set, key), "example_age", 5, meta, policy)
        print("---")
        print("OK, 1 record touched.")

        (returnedkey, meta, bins) = client.get((namespace, set, key))

        print("---")
        print("After increment operation")
        print(bins)

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        exitCode = 2

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
