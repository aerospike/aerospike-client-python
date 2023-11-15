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

class HyperLogLog(bytes):
    """
    Represents a HyperLogLog value. This can be returned from the server or created in order to be sent to the server.

    The constructor takes in any argument that the :class:`bytes` constructor takes in.

    >>> h = HyperLogLog([1, 2, 3])
    >>> client.put(key, {"hyperloglog": h})
    """
    def __new__(cls, o) -> "HyperLogLog":
        return super().__new__(cls, o)
