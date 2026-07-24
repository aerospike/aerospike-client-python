
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

class KVS(Example):
    def run(self):
        print(
            '########################################################################')
        print('PUT')
        print(
            '########################################################################')

        for i in range(1, 1000):
            # print 'a'
            # j = igloo
            rec = {
                'i': i,
                's': 'xyz',
                'l': [2, 4, 8, 16, 32, None, 128, 256],
                'm': {'a': 2, 'b': 4, 'c': 8, 'd': 16}
            }
            print(rec)
            KEY = ('test', 'demo', str(i))
            self.client.put(KEY, rec)

        print(
            '########################################################################')
        print('EXISTS')
        print(
            '########################################################################')

        for i in range(1, 1000):
            KEY = ('test', 'demo', str(i))
            (key, metadata) = self.client.exists(KEY)
            print(key, metadata)

        print(
            '########################################################################')
        print('GET')
        print(
            '########################################################################')

        for i in range(1, 1000):
            KEY = ('test', 'demo', str(i))
            (key, metadata, record) = self.client.get(KEY)
            print(key, metadata, record)

        print(
            '########################################################################')
        print('APPLY')
        print(
            '########################################################################')

        self.client.udf_put('./examples/client/simple.lua')

        for i in range(1, 1000):
            key = ('test', 'demo', 'key{0}'.format(i))
            val1 = self.client.apply(key, 'simple', 'concat', ['a', 30000])
            print(val1)

        print(
            '########################################################################')
        print('REMOVE')
        print(
            '########################################################################')

        for i in range(1, 1000):
            KEY = ('test', 'demo', str(i))
            self.client.remove(KEY)
