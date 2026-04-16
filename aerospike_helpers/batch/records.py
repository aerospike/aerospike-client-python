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

from __future__ import annotations
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from typing import Optional, Any

    TypeOps = list[dict]
    TypeBatchPolicyWrite = dict | None
    TypeBatchPolicyRemove = dict | None
    TypeBatchPolicyApply = dict | None
    TypeBatchPolicyRead = dict | None
    TypeRecord = tuple | None
    TypeUDFArgs = list[Any]


class _Types:
    READ = 0
    WRITE = 1
    APPLY = 2
    REMOVE = 3


class BatchRecord:
    """ BatchRecord provides the base fields for BatchRecord objects.

        BatchRecord should usually be read from as a result and not created by the user. Its subclasses can be used as
        input to batch_write.
        Client methods :meth:`~aerospike.Client.batch_apply`, :meth:`~aerospike.Client.batch_operate`,
        :meth:`~aerospike.Client.batch_remove` with batch_records field as a list of these BatchRecord objects
        containing the batch request results.

        Attributes:
            key (:obj:`tuple`): The aerospike key to operate on.
            record (:ref:`aerospike_record_tuple`): The record corresponding to the requested key.
            result (int): The status code of the command.
            in_doubt (bool): Is it possible that the write command completed even though an error was generated. \
            This may be the case when a client error occurs (like timeout) after the command was sent \
            to the server.
    """
    key: tuple
    record: tuple | None
    result: int
    in_doubt: bool

    def __init__(self, key: tuple) -> None:
        self.key = key
        self.record = None
        self.result = 0
        self.in_doubt = False


class Write(BatchRecord):
    """ Write is used for executing Batch write commands with batch_write and retrieving batch write results.

        .. include:: ./deprecate_meta_ttl.rst

        Attributes:
            ops: A list of aerospike operation dictionaries to perform
                on the record at key.
            meta: the metadata to set for this command
            policy: An optional dictionary of batch write policy
                flags.
    """
    ops: TypeOps
    meta: Optional[dict]
    policy: TypeBatchPolicyWrite

    def __init__(
        self, key: tuple, ops: "TypeOps", meta: Optional[dict] = None, policy: "TypeBatchPolicyWrite" = None
    ) -> None:
        """
        Example::

            # Create a batch Write to increment bin "a" by 10 and read the result from the record.
            import aerospike
            import aerospike_helpers.operations as op
            from aerospike_helpers.batch.records import Write

            bin_name = "a"

            namespace = "test"
            set = "demo"
            user_key = 1
            key = (namespace, set, user_key)

            ops = [
                op.increment(bin_name, 10),
                op.read(bin_name)
            ]

            meta={"gen": 1, "ttl": aerospike.TTL_NEVER_EXPIRE}
            bw = Write(key, ops, meta=meta)
        """
        super().__init__(key)
        self.ops = ops
        self._type = _Types.WRITE
        self._has_write = True
        self.meta = meta
        self.policy = policy


class Read(BatchRecord):
    """ Read is used for executing Batch read commands with batch_write and retrieving results.

        .. deprecated:: 19.1.0 Deprecated the ``"ttl"`` option in the ``meta`` parameter. Use the policy parameter in a
            :py:obj:`~aerospike_helpers.batch.records.Write` BatchRecord to set the ``"ttl"`` instead.

        Attributes:
            ops: list of aerospike operation dictionaries to perform on
                the record at key.
            meta: the metadata to set for this command
            read_all_bins: An optional bool, if True, read all bins in the record.
            policy: An optional dictionary of :ref:`aerospike_batch_read_policies`.
    """
    ops: Optional[TypeOps]
    meta: Optional[dict]
    read_all_bins: bool
    policy: TypeBatchPolicyRead

    def __init__(
        self,
        key: tuple,
        ops: TypeOps | None,
        read_all_bins: bool = False,
        meta: Optional[dict] = None,
        policy: "TypeBatchPolicyRead" = None,
    ) -> None:
        """
        Example::

            # Create a batch Read to read bin "a" from the record.
            import aerospike
            import aerospike_helpers.operations as op
            from aerospike_helpers.batch.records import Read

            bin_name = "a"

            namespace = "test"
            set = "demo"
            user_key = 1
            key = (namespace, set, user_key)

            ops = [
                op.read(bin_name)
            ]

            meta={"gen": 1, "ttl": aerospike.TTL_NEVER_EXPIRE}
            br = Read(key, ops, meta=meta)
        """
        super().__init__(key)
        self.ops = ops
        self.read_all_bins = read_all_bins
        self._type = _Types.READ
        self._has_write = False
        self.meta = meta
        self.policy = policy


class Apply(BatchRecord):
    """ BatchApply is used for executing Batch UDF (user defined function) apply commands with batch_write and
        retrieving results.

        Attributes:
            module: Name of the lua module previously registered with the server.
            function: Name of the UDF to invoke.
            args: List of arguments to pass to the UDF.
            policy: An optional dictionary of batch apply policy
                flags.
    """
    module: str
    function: str
    args: TypeUDFArgs
    policy: TypeBatchPolicyApply

    def __init__(
        self, key: tuple, module: str, function: str, args: "TypeUDFArgs", policy: "TypeBatchPolicyApply" = None
    ) -> None:
        """
        Example::

            # Create a batch Apply to apply UDF "test_func" to bin "a" from the record.
            # Assume that "test_func" takes a bin name string as an argument.
            # Assume the appropriate UDF module has already been registered.
            import aerospike_helpers.operations as op


            module = "my_lua"
            function = "test_func"

            bin_name = "a"
            args = [
                bin_name
            ]

            namespace = "test"
            set = "demo"
            user_key = 1
            key = (namespace, set, user_key)

            ba = Apply(key, module, function, args)
        """
        super().__init__(key)
        self._type = _Types.APPLY
        self._has_write = True
        self.module = module
        self.function = function
        self.args = args
        self.policy = policy


class Remove(BatchRecord):
    """ Remove is used for executing Batch remove commands with batch_write and retrieving results.

        Attributes:
            policy (:ref:`aerospike_batch_remove_policies`, optional): An optional dictionary of batch remove policy
                flags.
    """
    policy: TypeBatchPolicyRemove

    def __init__(self, key: tuple, policy: "TypeBatchPolicyRemove" = None) -> None:
        """
        Example::

            # Create a batch Remove to remove the record.
            import aerospike_helpers.operations as op


            namespace = "test"
            set = "demo"
            user_key = 1
            key = (namespace, set, user_key)

            br = Remove(key, ops)
        """
        super().__init__(key)
        self._type = _Types.REMOVE
        self._has_write = True
        self.policy = policy


TypeBatchRecordList = list[BatchRecord]


class BatchRecords:
    """ BatchRecords is used as input and output for multiple batch APIs.

        Attributes:
            batch_records (list): A list of BatchRecord subtype objects used to \
            define batched commands and hold results. BatchRecord Types can be Remove, Write, \
            Read, and Apply.
            result (int): The status code of the last batch call that used this BatchRecords.
                ``0`` if all batched commands succeeded (or if the only failures were \
                    ``FILTERED_OUT`` or ``RECORD_NOT_FOUND``)
                Not ``0`` if an error occurred. The most common error is ``-16`` \
                    (One or more batched commands failed).
    """
    batch_records: TypeBatchRecordList
    result: int

    def __init__(self, batch_records: Optional[TypeBatchRecordList] = None) -> None:
        """
        Example::

            import aerospike
            import aerospike_helpers.operations.operations as op
            from aerospike_helpers.batch.records import BatchRecords, Remove, Write, Read

            # Setup
            config = {
                "hosts": [("127.0.0.1", 3000)]
            }
            client = aerospike.client(config)

            namespace = "test"
            set_ = "demo"
            keys = [
                (namespace, set_, 1),
                (namespace, set_, 2),
                (namespace, set_, 3),
            ]
            bin_name = "id"
            for key in keys:
                client.put(key, {bin_name: 1})

            # Create a BatchRecords to remove a record, write a bin, and read a bin.
            brs = BatchRecords(
                [
                    Remove(
                        key=keys[0],
                    ),
                    Write(
                        key=keys[1],
                        ops=[
                            op.write(bin_name, 100),
                            op.read(bin_name),
                        ]
                    ),
                    Read(
                        key=keys[2],
                        ops=[
                            op.read(bin_name)
                        ]
                    )
                ]
            )

            # Note this call will mutate brs and set results in it.
            client.batch_write(brs)
            for br in brs.batch_records:
                print(br.result)
                print(br.record)
            # 0
            # (('test', 'demo', 1, bytearray(b'...')), {'ttl': 4294967295, 'gen': 0}, {})
            # 0
            # (('test', 'demo', 2, bytearray(b'...')), {'ttl': 2592000, 'gen': 4}, {'id': 100})
            # 0
            # (('test', 'demo', 3, bytearray(b'...')), {'ttl': 2592000, 'gen': 3}, {'id': 1})
        """

        if batch_records is None:
            batch_records = []

        self.batch_records = batch_records
        self.result = 0
