from .. import Example


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


class Put(Example):
    def run(self):
        record = {
            'i': 123,
            'f': 3.1415,
            's': 'abc',
            'u': '안녕하세요',
            #  'b': bytearray(['d','e','f']),
            #  'l': [123, 'abc', bytearray(['d','e','f']), ['x', 'y', 'z'], {'x': 1, 'y': 2, 'z': 3}],
            #  'm': {'i': 123, 's': 'abc', 'u': '안녕하세요', 'b': bytearray(['d','e','f']), 'l': ['x', 'y', 'z'], 'd': {'x': 1, 'y': 2, 'z': 3}},
            'l': [123, 'abc', '안녕하세요', ['x', 'y', 'z'], {'x': 1, 'y': 2, 'z': 3}],
            'm': {'i': 123, 's': 'abc', 'u': '안녕하세요', 'l': ['x', 'y', 'z'], 'd': {'x': 1, 'y': 2, 'z': 3}}
        }
        self.client.put(self.key, record)
