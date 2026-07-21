
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

from .. import ExampleWithRecord

# TODO: missing this
config = {
    # TODO: this is deprecated?
    'policies': {
        'total_timeout': 1000
    }
}

class Delete(ExampleWithRecord):
    def run(self):
        # TODO; these two were configurable
        test_count = 128
        policy = {
            'total_timeout': 1000
        }
        meta = None
        print(f"IO test count:{test_count}")
        def delete(namespace, set, test_count):
            self.client.remove(self.key)
            # for i in range(0, test_count):

                # TODO
                # key = {'ns': namespace, \
                #         'set':set, \
                #         'key': str(i), \
                #         'digest': aerospike.calc_digest(namespace, set, str(i))}
                # self.client.remove(self.key)


        delete(self.namespace, set, test_count)
        print(f"Deleted {test_count} records")
