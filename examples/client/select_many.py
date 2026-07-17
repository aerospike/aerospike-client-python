
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



import aerospike
import sys



# optparser.add_option(
#     "-k", "--keys", dest="keys", type="string", default="", metavar="<KEYS>",
#     help="Keys to be accessed in the database server. Should be specified as 'name','name1','name2' etc")

from .. import Example

class SelectMany(Example):
    def run(self):
        # args.pop()
        # TODO: configurable
        keys = []
        # keys = options.keys.split(',')
        # keylist = []
        # for key in keys:
        #     individualkey = (namespace, set, key)
        #     keylist.append(individualkey)

        records = self.client.select_many(keys, ['i', 'd'])

        if records is not None:
            print(records)
            print("---")
            print("OK, %d records found." % len(records))
        else:
            # TODO: not sure if this is right
            print('error: Not Found.', file=sys.stderr)
