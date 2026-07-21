
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


from .. import ExampleWithRecord


# TODO: should use fixture with multiple records
class BatchRead(ExampleWithRecord):
    def run(self):
        keys = [f"key{i}" for i in range(5)]

        # Get records
        records = self.client.batch_read(keys)

        if records != None:
            print(f"{len(records)} records were found")
            print(records)
        else:
            print('error: Not Found.')

        # TODO: verify syntax
        # Verify existence of records
        records = self.client.batch_read(keys, bins=[])

        if records != None:
            print(f"{len(records)} records were found")
            print(records)
        else:
            print('error: Not Found.')
