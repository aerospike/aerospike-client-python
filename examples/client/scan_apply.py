
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


import aerospike


from .. import Example


class ScanApply(Example):
    def run(self):
        MODULE = "stream_example"
        FUNCTION = "count"

        policy = {}
        scan_id = self.client.scan_apply(
            self.namespace, self.set_name, MODULE, FUNCTION, [], policy)

        while True:
            response = self.client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break

        if response['status'] == aerospike.JOB_STATUS_COMPLETED:
            print("Background scan is successful")
        else:
            print("Scan_apply failed")
