
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

from .. import Example


class Scan(Example):
    def run(self):
        s = self.client.scan(self.namespace, self.set_name)

        # TODO: configurable
        bins = []
        # project specified bins
        s.select(*bins)

        records = []

        # callback to be called for each record read
        def callback(input_tuple):
            (_, _, record) = input_tuple
            records.append(record)
            print(record)

        # invoke the operations, and for each record invoke the callback
        s.foreach(callback)

        print("OK, %d records found." % len(records))
