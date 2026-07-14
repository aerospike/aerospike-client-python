
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


class Append(Example):
    def run(self):
        record = {
            'example_name': 'John',
            'example_age': 1
        }

        # TODO meta gen/ttl should be options?
        # TODO: this is the deprecated way of setting ttl and maybe gen
        meta = {
            'ttl': 1000,
            'gen': 10
        }
        policy = None
        self.client.put(self.key, record, meta, policy)

        # TODO: print statements should mark when command successfully finishes?

        self.client.append(
            self.key, "example_name", " Smith", meta, policy)
        (key, meta, bins) = self.client.get(self.key)
        print(bins)
