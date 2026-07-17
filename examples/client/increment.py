
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


from .. import ExampleWithRecord


class Increment(ExampleWithRecord):
    def run(self):
        record = {
            'example_name': 'John',
            'example_age': 1
        }

        # TODO: configurable
        # TODO: deprecated
        meta = {'ttl': 1000, 'gen': 10}
        policy = None

        # invoke operation

        self.client.put(self.key, record, meta, policy)

        (returnedkey, meta, bins) = self.client.get(self.key)

        print("Before increment operation")
        print(bins)

        self.client.increment(self.key, "example_age", 5, meta, policy)

        (returnedkey, meta, bins) = self.client.get(self.key)

        print("After increment operation")
        print(bins)
