
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


from aerospike_helpers.batch.records import BatchRecords
from .. import ExampleWithRecord


class BatchRead(ExampleWithRecord):
    def show_records(self, records: BatchRecords):
        print(f"{len(records.batch_records)} records were found")
        for br in records.batch_records:
            pk = br.key
            print("Record with digest", pk[3], "has result code", br.result, "with record", br.record)

    def run(self):
        # Get records
        keys = [self.key, self.non_existent_key]
        print("All bins should be returned")
        records = self.client.batch_read(keys)
        self.show_records(records)

        # Select bins
        print("\"a\" should be filtered out")
        records = self.client.batch_read(keys, bins=["b"])
        self.show_records(records)

        # Verify existence of records
        print("Bins should not be returned")
        records = self.client.batch_read(keys, bins=[])
        self.show_records(records)
