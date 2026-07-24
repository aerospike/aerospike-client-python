
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


class UDFGet(Example):
    def run(self):
        # TODO: configurable
        module = "./examples/client/example.lua"
        language = aerospike.UDF_TYPE_LUA
        policy = {}

        self.client.udf_put(module, language, policy)
        udf_contents = self.client.udf_get(module, language, policy)
        print("Module contents : ")
        print(udf_contents)
