
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


class ScanPartition(Example):
    def run(self):
        s = self.client.scan(self.namespace, self.set_name)

        partition_policy = None

        # TODO: configurable
        STARTING_PARTITION = 1
        if STARTING_PARTITION > 0:
            # project specified bins
            partition_policy = {'partition_filter': {'begin': STARTING_PARTITION, 'count': 1}}

        records = []

        # callback to be called for each record read
        def callback(input_tuple):
            (_, _, record) = input_tuple
            records.append(record)
            print(record)

        self.client.truncate(self.namespace, self.set_name, 0)

        # invoke the operations, and for each record invoke the callback
        s.foreach(callback, partition_policy)
        existing_count = len(records)
        if existing_count > 0:
            print(f"{existing_count} records already exist in partition: {STARTING_PARTITION}.")

        count = 0
        for i in range(1, 80000):
            rec_partition = self.client.get_key_partition_id(self.namespace, self.set_name, str(i))

            if rec_partition == STARTING_PARTITION: # and not client.exists(('test', 'demo', str(i))):

                count = count + 1
                rec = {
                    'i': i,
                    's': 'xyz',
                    'l': [2, 4, 8, 16, 32, None, 128, 256],
                    'm': {'partition': rec_partition, 'b': 4, 'c': 8, 'd': 16}
                }
                self.client.put((self.namespace, self.set_name, str(i)), rec)

        records.clear()
        # invoke the operations, and for each record invoke the callback
        s.foreach(callback, partition_policy)

        print("---")
        print(f"{count} records are put into partition: {STARTING_PARTITION}.")
        print(f"{len(records)} records are found in partition: {STARTING_PARTITION}.")
