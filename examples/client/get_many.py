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

from optparse import OptionParser


##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser.add_option(
    "-k", "--keys", dest="keys", type="string", default="", metavar="<KEYS>",
    help="Keys to be accessed in the database server. Should be specified as 'name','name1','name2' etc")

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
        # args.pop()

        keys = options.keys.split(',')
        keylist = []
        for key in keys:
            individualkey = (namespace, set, key)
            keylist.append(individualkey)

        records = client.get_many(keylist)

        if records is not None:
            print(records)
            print("---")
            print("OK, %d records found." % len(records))
        else:
            print('error: Not Found.', file=sys.stderr)
            exitCode = 1

    except Exception as eargs:
        print("error: {0}".format(eargs), file=sys.stderr)
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
