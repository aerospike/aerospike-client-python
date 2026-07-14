# -*- coding: utf-8 -*-
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


# optparser.add_option(
#     "--timeout", dest="timeout", type="int", default=1000, metavar="<MS>",
#     help="Client timeout")

# optparser.add_option(
#     "--read-timeout", dest="read_timeout", type="int", default=1000, metavar="<MS>",
#     help="Client read timeout")

# config = {
#     'hosts': [(options.host, options.port)],
#     # TODO: not relevant to get()? C# client get() example doesn't have this
#     'policies': {
#         'total_timeout': options.timeout
#     }
# }

class Get(Example):
    def run(self):
        # TODO: This needs to be moved into a fixture.
        # TODO: there also needs to be a cleanup step.
        # TODO: at this point, I'm wondering if pytest can be used since
        # it has fixtures as a built-in feature
        key = (self.namespace, self.set_name, "docreadkey")
        self.client.put(key, bins={"a": 1})

        policy = {
            'total_timeout': 300
        }
        record = self.client.get(key, policy)
        print(record)
