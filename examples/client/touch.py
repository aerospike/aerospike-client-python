
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

# TODO: ttl/gen configurable

from .. import ExampleWithRecord


class Touch(ExampleWithRecord):
    def run(self):
        # TODO configurable
        key = args.pop()

        meta = {'ttl': options.ttl, 'gen': options.gen}
        policy = None

        (returnedkey, meta) = self.client.exists(self.key)

        print("---")
        print("Ttl before touch operation")
        print(meta)

        self.client.touch(self.key, options.ttl + 1000, meta, policy)
        print("---")
        print("OK, 1 record touched.")

        (returnedkey, meta) = self.client.exists(self.key)

        print("---")
        print("Ttl after touch operation")
        print(meta)
