
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

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options] key bin [bin ...]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--no-key", dest="nokey", action="store_true",
    help="Do not return the key")

optparser.add_option(
    "--no-metadata", dest="nometadata", action="store_true",
    help="Do not return the metadata")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

if len(args) < 1:
    optparser.print_help()
    print()
    sys.exit(1)

from .. import ExampleWithRecord


class SelectRecord(ExampleWithRecord):
    def run(self):
        # TODO: both configurable
        key = args.pop(0)
        bins = []

        policy = None

        print(args)

        (key, metadata, record) = self.client.select(
            (self.namespace, self.set_name, key), bins, policy)

        if metadata is not None:
            if options.nometadata and options.nokey:
                print(record)
            elif options.nometadata:
                print(key, record)
            elif options.nokey:
                print(metadata, record)
            else:
                print(key, metadata, record)
            print("---")
            print("OK, 1 record found.")
        else:
            # TODO: not sure if this is right.
            print('error: Not Found.', file=sys.stderr)
            exitCode = 1
