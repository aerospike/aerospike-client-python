
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


class Info(Example):
    def run(self):
        # Default info request
        # TODO: configurable
        request = "statistics"

        # TODO: needs review
        response = self.client.info_all(request)
        for node, (_, res) in response.items():
            if res is None:
                continue

            res = res.strip()
            if len(res) == 0:
                continue

            entries = res.split(';')
            if len(entries) <= 1:
                print("{0}: {1}".format(node, res))

            print("{0}:".format(node))
            for entry in entries:
                entry = entry.strip()
                if len(entry) == 0:
                    continue
                if "=" not in entry:
                    continue

                (name, value) = entry.split('=')
                print("    - {0}: {1}".format(name, value))
