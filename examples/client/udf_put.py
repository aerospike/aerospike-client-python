
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


class UDFPut(Example):
    def run(self):
        policy = {}
        # TODO
        # filename = args.pop()
        filename = "./examples/client/example.lua"
        udf_type = 0  # 0 for LUA

        self.client.udf_put(filename, udf_type, policy)
