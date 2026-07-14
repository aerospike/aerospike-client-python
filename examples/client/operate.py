
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

from aerospike_helpers.operations import operations as op_helpers


class Operate(Example):
    def run(self):
        record = {
            'example_name': 'John',
            'example_age': 1
        }

        meta = {'ttl': 1000, 'gen': 10}
        policy = None
        self.client.put(self.key, record, meta, policy)

        _, _, bins = self.client.get(self.key)
        print("Before operation:", bins)

        ops = [
            op_helpers.prepend("example_name", "Mr "),
            op_helpers.increment("example_age", 3),
            op_helpers.read("example_name")
        ]
        _, _, bins = self.client.operate(self.key, ops, meta, policy)
        print("Record returned by operate():", bins)

        _, _, bins = self.client.get(self.key)
        print("After operation:", bins)
