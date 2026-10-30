
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


import os.path

from aerospike import predicates as p
from .. import ExampleWithIndex, UDFExample


class Aggregate(ExampleWithIndex, UDFExample):
    def run(self):
        predicates = [
            p.equals(self.BIN_NAME, 1),
            p.between(self.BIN_NAME, 1, 3)
        ]

        for predicate in predicates:
            # If predicate is provided, then perform a query
            query = self.client.query(self.namespace, self.set_name)
            query.where(predicate)
            BINS = [self.BIN_NAME]
            query.select(*BINS)

            MODULE = "stream_example"
            FUNCTION = "count"
            ARGS = []
            query.apply(MODULE, FUNCTION, ARGS)

            results = []

            # callback to be called for each record read
            def callback(result):
                results.append(result)
                print(result)

            # invoke the operations, and for each record invoke the callback
            query.foreach(callback)

            print(len(results))
