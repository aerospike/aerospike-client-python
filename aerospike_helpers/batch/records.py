##########################################################################
# Copyright 2013-2022 Aerospike, Inc.
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
'''
records.py defines objects for use with aerospike client batch APIS. Currently batch_write, batch_operate,
batch_remove, and batch_apply make use of objects in this file. Typically BatchReacords and underlying
BatchRecord objects are used as input and output for the aformentioned client methods.

    .. note:: APIs that utitlize these objects require server >= 6.0.0.

Example::

    import aerospike
    from aerospike_helpers.batch import records as br
    import aerospike_helpers.expressions as exp
    from aerospike_helpers.operations import operations as op
    from aerospike_helpers.operations import expression_operations as exop
    import sys

    # Configure the client.
    config = {"hosts": [("127.0.0.1", 3000)]}

    # Create a client and connect it to the cluster.
    try:
        client = aerospike.client(config).connect()
    except ex.ClientError as e:
        print("Error: {0} [{1}]".format(e.msg, e.code))
        sys.exit(1)

    # setup records
    namespace = "test"
    set = "demo"

    keys = [(namespace, set, i) for i in range(1000)]
    records = [{"id": i, "balance": i * 10} for i in range(1000)]
    for key, rec in zip(keys, records):
        client.put(key, rec)

    # Write a new bin to the records using batch_operate
    # The expression, "expr", means this bin will signify that the record
    # has an odd user key.
    expr = exp.Eq(exp.Mod(exp.KeyInt(), 2), 1).compile()
    ops = [
        exop.expression_write("odd", expr)
    ]

    client.batch_operate(keys, ops)


    # Batch add 10 to balance and read it if it's over
    # 1000 NOTE: batch_operate can't be used only reading
    # get_batch_ops or get_many can be used for that.
    expr = exp.GT(exp.IntBin("balance"), 1000).compile()
    ops = [
        op.increment("balance", 10),
        op.read("balance")
    ]
    policy_batch = {"expressions": expr}
    res = client.batch_operate(keys, ops, policy_batch)

    # record 100 should be None
    print(res.batch_records[100].record)
    # record 101 should be populated
    print(res.batch_records[101].record)


    # Apply different operations to different keys
    # using batch_write.
    w_batch_record = br.BatchRecords(
        [
            br.BatchRemove(
                key=(namespace, set, 1),
                policy={}
            ),
            br.BatchWrite(
                key=(namespace, set, 100),
                ops=[
                    op.write("id", 100),
                    op.write("balance", 100),
                    op.read("ilist_bin"),
                    op.read("id"),
                ],
                policy={}
            ),
            br.BatchRead(
                key=(namespace, set, 333),
                ops=[
                    op.read("id")
                ],
                policy={}
            ),
        ]
    )

    # batch_write modifies its BatchRecords argument.
    # results for each BatchRecord will be set in their result,
    # record, and in_doubt fields.
    client.batch_write(w_batch_record)

    # should be {'id': 333} 
    print(w_batch_record.batch_records[2].record[2])

.. seealso:: `Bits (Data Types) <https://www.aerospike.com/docs/guide/bitwise.html>`_.
'''

from typing import Any, List

class _Types():
    READ = 0
    WRITE = 1
    APPLY = 2
    REMOVE = 3

class BatchRecord:
    """ 
        _BatchRecord provides the base fields for BtachRecord objects.
        key is the aerospike key to operate on.
        ops are the operations to use.
        record, the record for the requested key.
        result is the status code of the operation.
        _type is the type of batch operation.
        _has_write does this batch subtransaction contain a write operation?
        in_doubt Is it possible that the write transaction completed even though this error was generated.
	        This may be the case when a client error occurs (like timeout) after the command was sent
	        to the server.
        policy Operation policy, type depends on batch type, write, read, apply, etc. TODO is this correct?
    """
    def __init__(self, key: tuple) -> None:
        self.key = key
        self.record = None
        self.result = 0 # TODO set this as the ok status code using the constant
        self.in_doubt = False


class BatchWrite(BatchRecord):
    """
        BatchWrite defines the object used for Batch write operations and
        retrieving batch write results.
    """

    def __init__(self, key: tuple, ops: List[dict], policy: dict = None) -> None:
        super().__init__(key)
        self.ops = ops
        self._type = _Types.WRITE
        self._has_write = True
        self.policy = policy


class BatchRead(BatchRecord):
    """
        BatchRead defines the object used for Batch read operations and
        retrieving batch read results.
    """

    def __init__(self, key: tuple, ops: List[dict], policy: dict = None) -> None:
        super().__init__(key)
        self.ops = ops
        self._type = _Types.READ
        self._has_write = False
        self.policy = policy


class BatchApply(BatchRecord):
    """
        BatchWrite defines the object used for Batch UDF apply operations and
        retrieving batch apply results.
    """

    def __init__(self, key: tuple, module: str, function: str, args: List[Any], policy: dict = None) -> None:
        super().__init__(key)
        self._type = _Types.APPLY
        self._has_write = True # TODO should this ba an arg set by user?
        self.module = module
        self.function = function
        self.args = args
        self.policy = policy


class BatchRemove(BatchRecord):
    """
        BatchWrite defines the object used for Batch remove operations and
        retrieving batch remove results.
    """

    def __init__(self, key: tuple, policy: dict = None) -> None:
        super().__init__(key)
        self._type = _Types.REMOVE
        self._has_write = True
        self.policy = policy


class BatchRecords:
    """ TODO refactor the description with Python types
        BatchRecords contains a list of batch request/response (as_batch_base_record) records. The record types can be
        as_batch_read_record, as_batch_write_record, as_batch_apply_record or as_batch_remove_record.
    """
    def __init__(self, batch_records: List[BatchRecord] = []) -> None:
        self.batch_records = batch_records
        self.result = 0





# TODO policy support (probably in its own module)
# class PolicyBatchWrite:
#     """
#         PolicyBatchWrite defines policy options for use with
#         BatchWrite operations.
#     """

#     def __init__(self) -> None:
#         self.expression = expression
#         self.key_policy = key_policy