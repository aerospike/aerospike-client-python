
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

config = {
    'policies': {
        # TODO: configurable
        'total_timeout': 1000
    }
}


class UnicodeSmiles(Example):
    def run(self):
        smile = "smilé"
        # TODO: configurable
        read_timeout = 1000

        key = (self.namespace, self.set_name, smile)
        bins = {'smiley': smile, 'smile_count': 1, 'mood': 'happy'}
        print("Storing ", bins, "at a record identified by the tuple", key)
        # overwrite the record if it exists, otherwise create it
        self.client.put(key, bins,
                   policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE,
                           'key': aerospike.POLICY_KEY_SEND})
        print("Retrieving the record from the server for comparison")
        (key, meta, record) = self.client.get(
            key, policy={'total_timeout': read_timeout})
        print("The value of the 'smiley' bin is", record['smiley'], "\n")
        print("By the way, this record has been written", meta['gen'], "times")
        future_gen = str(int(meta['gen']) + 2)
        print("Expect the record generation to be",
              future_gen, "with two more write operations\n")

        # add a dictionary under a bin named 'data'
        bins = {'data': {'smiley_key': smile, smile: 'this is a smiley '}}
        print("Storing ", bins, "at the record", key)
        self.client.put(key,  bins)
        (key, metadata, bins) = self.client.get(key)
        print("The value of the 'smiley_key' of the 'data' bin is",
              bins['data']['smiley_key'], "\n")
        # print("The value of the", smile, " key is:",
        #      bins['data'][smile], "\n")

        # append to the value of the smile key
        print("Before appending, the value of the 'mood' key is:",
              bins['mood'])
        self.client.append(key, 'mood', smile)
        (key, metadata, bins) = self.client.get(key)
        print("After appending, the value of the 'mood' key is:",
              bins['mood'], "\n")

        # prepend to the value of the smile key
        print("Before prepending, the value of the 'mood' key is:",
              bins['mood'])
        self.client.prepend(key, 'mood', smile)
        (key, metadata, bins) = self.client.get(key)
        print("After prepending, the value of the 'mood' key is:",
              bins['mood'], "\n")

        # multiple operations on the record using the operate() method
        ops = [
            operations.append(bin_name="smiley", append_item=smile),
            operations.increment(bin_name="smile_count", amount=5),
            operations.read(bin_name="smiley"),
        ]
        print("Setting the following multiops on the same record\n", ops)
        (key, meta, bins) = self.client.operate(key, ops)
        print("The value of the 'smiley' bin is", bins['smiley'], "\n")

        print("Displaying the key, metadata, and bins of the record")
        (key, meta, bins) = self.client.get(key)
        print(key)
        print(meta)
        print(bins, "\n")
        self.client.remove(key)

        # example of a bytearray primary key
        print("Save a new record with a bytearray primary key")
        smiley_pk = smile.encode("utf-8")
        self.client.put((self.namespace, self.set_name, smiley_pk), {'smiley': smile, 'smiley_pk':
                                                 smiley_pk})
        print("Display the bins of a record with a bytearray key")
        (key, meta, bins) = self.client.get((self.namespace, self.set_name, smiley_pk))
        print(bins)
        print("The value of the 'smiley_pk' bin is", bins['smiley_pk'], "\n")
        self.client.remove(key)
