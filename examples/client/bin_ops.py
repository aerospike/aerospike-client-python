
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


import aerospike
from aerospike_helpers.operations import operations
import pprint


class BinOps(Example):
    def run(self):
        pp = pprint.PrettyPrinter(indent=2)
        key = ('test', 'cats', 'mr. peppy')

        self.client.put(key, {'breed': 'persian'},
                   policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
                           'key': aerospike.POLICY_KEY_DIGEST, 'ttl': 120})
        (key, meta, bins) = self.client.get(key)

        print("Before:", bins)
        self.client.increment(
            key, 'lives', -1, {'gen': 2}, policy={'total_timeout': 1500, 'ttl': 1000})
        (key, meta, bins) = self.client.get(key)

        print("After:", bins)
        # the key we got back when we fetched the record with get() is useable
        # as-is because it contains the record's digest
        self.client.increment(key, 'lives', -1)
        (key, meta, bins) = self.client.get(key)

        # kitty lost a life, unfortunately
        print("Poor Kitty:", bins)
        self.client.put(key, {'owner': 'Fry'})
        self.client.prepend(key, 'owner', 'Philip J. ')
        self.client.append(key, 'owner', ' Esq.')

        # kitty loses another life, gains a color, all as part of a record
        # multi-op
        ops = [
            operations.write(bin_name="color", write_item="smoke"),
            operations.increment(bin_name="lives", amount=-1),
            operations.read("ailments"),
            operations.read("lives")
        ]
        (key, meta, bins) = self.client.operate(key, ops)
        print("After calling operate(), kitty is down to",
              bins['lives'], "lives")
        pp.pprint(bins)

        # display the record as it is after all the operations
        (key, meta, bins) = self.client.get(key)
        print("\nRecord\n======\nKey\n---")
        pp.pprint(key)
        print("Meta\n----")
        pp.pprint(meta)
        print("Bins\n----")
        pp.pprint(bins)
