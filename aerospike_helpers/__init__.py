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
    Represents a HyperLogLog value. This can be returned from or sent to the server.

    .. testcode::

        from aerospike_helpers.operations import hll_operations
        import aerospike

        client = aerospike.client({'hosts': [('localhost', 3000)]})

        BIN_NAME="hll"
        ops = [
            hll_operations.hll_init(BIN_NAME, index_bit_count=4, mh_bit_count=4)
        ]
        keyTuple = ("test", "demo", 1)
        client.operate(keyTuple, ops)
        _, _, bins = client.get(keyTuple)
        print(bins[BIN_NAME])

        client.put(keyTuple, bins)
        _, _, bins = client.get(keyTuple)
        print(bins[BIN_NAME])

        # Cleanup
        client.remove(keyTuple)

    .. testoutput::

        HyperLogLog(...)
        HyperLogLog(...)
    """
    def __new__(cls, o) -> "HyperLogLog":
        return super().__new__(cls, o)

    # We need to implement repr() and str() ourselves
    # Otherwise, this class will inherit these methods from bytes
    # making it indistinguishable from bytes objects when printed
    def __repr__(self) -> str:
        bytes_str = super().__repr__()
        return f"{self.__class__.__name__}({bytes_str})"

    def __str__(self) -> str:
        return self.__repr__()
