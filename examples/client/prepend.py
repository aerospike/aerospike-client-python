
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

from .. import Example


class Prepend(Example):
    def run(self):
        # TODO: can share this in a fixture class?
        record = {
            'example_name': 'John',
            'example_age': 1
        }

        self.client.put(self.key, record)

        self.client.prepend(self.key, "example_name", "Mr ")
        _, _, bins = self.client.get(self.key)
        print(bins)
