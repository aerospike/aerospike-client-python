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
records.py defines objects for use with aerospike client batch APIs. Currently batch_write, batch_operate,
batch_remove, and batch_apply make use of objects in this file. Typically BatchReacords and underlying
BatchRecord objects are used as input and output for the aformentioned client methods.

    .. note:: APIs that utitlize these objects require server >= 6.0.0.

Example::

    import aerospike
    from aerospike import exception as ex
    from aerospike_helpers.batch import records as br
    import aerospike_helpers.expressions as exp
    from aerospike_helpers.operations import operations as op
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


    print("===== BATCH_OPERATE EXAMPLE =====")
    # Batch add 10 to balance and read it if it's over
    # 1000 NOTE: batch_operate ops must include a write op
    # get_batch_ops or get_many can be used for all read ops.
    expr = exp.GT(exp.IntBin("balance"), 1000).compile()
    ops = [
        op.increment("balance", 10),
        op.read("balance")
    ]
    policy_batch = {"expressions": expr}
    res = client.batch_operate(keys, ops, policy_batch)

    # res is an instance of BatchRecords
    # the field, batch_records, contains a BatchRecord instance
    # for each key used by the batch_operate call.
    # the field, results, is 0 if all batch subtransactions completed succesfully
    # or the only failures are FILTERED_OUT or RECORD_NOT_FOUND.
    # Otherwise its value corresponds to an as_status and signifies that
    # one or more of the batch subtransactions failed. Each BatchRecord instance
    # also has a results field that signifies the status of that batch subtransaction.

    if res.result == 0:
        # BatchRecord 100 should have a result code of 27 meaning it was filtered out by an expression.
        print("BatchRecord 100 result: {result}".format(result=res.batch_records[100].result))
        # BatchRecord 100,record should be None.
        print("BatchRecord 100 record: {record}".format(record=res.batch_records[100].record))
        # BatchRecord 101 should have a result code of 0 meaning it succeeded.
        print("BatchRecord 101 result: {result}".format(result=res.batch_records[101].result))
        # BatchRecord 101, record should be populated.
        print("BatchRecord 101 record: {record}".format(record=res.batch_records[101].record))
    else:
        # Some batch sub transaction failed.
        print("res result: {result}".format(result=res.result))


    print("===== BATCH_WRITE EXAMPLE =====")
    # Apply different operations to different keys
    # using batch_write.
    batch_writes = br.BatchRecords(
        [
            br.Remove(
                key=(namespace, set, 1),
                policy={}
            ),
            br.Write(
                key=(namespace, set, 100),
                ops=[
                    op.write("id", 100),
                    op.write("balance", 100),
                    op.read("id"),
                    op.read("balance"),
                ],
                policy={"expressions": exp.GT(exp.IntBin("balance"), 2000).compile()}
            ),
            br.Read(
                key=(namespace, set, 333),
                ops=[
                    op.read("id")
                ],
                policy=None
            ),
        ]
    )

    # batch_write modifies its BatchRecords argument.
    # Results for each requested key will be set in
    # their coresponding BatchRecord result,
    # record, and in_doubt fields.
    client.batch_write(batch_writes)
    print("batch_writes result: {result}".format(result=batch_writes.result))

    # should have bins {'id': 333}.
    print("batch_writes batch Write record: {result}".format(result=batch_writes.batch_records[2].record))


    print("===== BATCH_APPLY EXAMPLE =====")
    # Apply a user defined function (UDF) to a batch
    # of records using batch_apply.
    module = "test_record_udf"
    path_to_module = "/path/to/test_record_udf.lua"
    function = "bin_udf_operation_integer"
    args = ["balance", 10, 5]

    client.udf_put(path_to_module)

    # This should add 15 to each balance bin.
    res = client.batch_apply(keys, module, function, args)
    # NOTE res.result should be -16 (one or more batch sub transactions failed)
    # because the UDF failed on record 1 which was previously removed.
    print("res result: {result}".format(result=res.result))

    res_rec = res.batch_records[90].record
    bins = res_rec[2]
    # Should be 915.
    print("res BatchRecord 90 bins: {result}".format(result=bins))


    print("===== BATCH_REMOVE EXAMPLE =====")
    # Delete the records using batch_remove.
    res = client.batch_remove(keys)
    # Should be 0 signifying success.
    print("BatchRecords result: {result}".format(result=res.result))

'''

import typing as ty


TypeOps = ty.List[ty.Dict]
TypeBatchPolicyWrite = ty.Union[ty.Dict, None]
TypeBatchPolicyRemove = ty.Union[ty.Dict, None]
TypeBatchPolicyApply = ty.Union[ty.Dict, None]
TypeBatchPolicyRead = ty.Union[ty.Dict, None]
TypeRecord = ty.Union[ty.Tuple, None]
TypeUDFArgs = ty.List[ty.Any]

class _Types():
    READ = 0
    WRITE = 1
    APPLY = 2
    REMOVE = 3

#### BatchRecord ####
class BatchRecord:
    """ BatchRecord provides the base fields for BatchRecord objects.

        BatchRecord should usually be read from as a result and not created by the user. Its subclasses can be used as input to batch_write.
        Client methods :meth:`~Client.batch_apply`, :meth:`~Client.batch_operate`, :meth:`~Client.batch_remove`
        with batch_records field as a list of these BatchRecord objects containing the batch request results.

        Attributes:
            key (:obj:`tuple`): The aerospike key to operate on.
            record (TypeRecord): The record corresponding to the requested key.
            result (int): The status code of the operation.
            in_doubt (int): Is it possible that the write transaction completed even though an error was generated. \
            This may be the case when a client error occurs (like timeout) after the command was sent \
            to the server.
    """
    def __init__(self, key: tuple) -> None:
        self.key = key
        self.record = None
        self.result = 0
        self.in_doubt = False


class Write(BatchRecord):
    """ Write is used for executing Batch write operations with batch_write and retrieving batch write results.

        Attributes:
            key (:obj:`tuple`): The aerospike key to operate on.
            record (:obj:`tuple`): The record corresponding to the requested key.
            result (int): The status code of the operation.
            in_doubt (int): Is it possible that the write transaction completed even though an error was generated. \
            This may be the case when a client error occurs (like timeout) after the command was sent \
            to the server.
            ops (TypeOps): A list of aerospike operation dictionaries to perform on the record at key.
            policy (TypeBatchPolicyWrite, optional): An optional dictionary of batch write policy flags.
    """

    def __init__(self, key: tuple, ops: 'TypeOps', policy: 'TypeBatchPolicyWrite' = None) -> None:
        """
            Example::

                # Create a batch Write to increment bin "a" by 10 and read the result from the record.
                import aerospike_helpers.operations as op


                bin_name = "a"

                namespace = "test"
                set = "demo"
                user_key = 1
                key = (namespace, set, user_key)

                ops = [
                    op.increment(bin_name, 10),
                    op.read(bin_name)
                ]

                bw = Write(key, ops)
        """
        super().__init__(key)
        self.ops = ops
        self._type = _Types.WRITE
        self._has_write = True
        self.policy = policy


class Read(BatchRecord):
    """ Read is used for executing Batch read operations with batch_write and retrieving results.

        Attributes:
            key (:obj:`tuple`): The aerospike key to operate on.
            record (:obj:`tuple`): The record corresponding to the requested key.
            result (int): The status code of the operation.
            in_doubt (int): Is it possible that the write transaction completed even though an error was generated. \
            This may be the case when a client error occurs (like timeout) after the command was sent \
            to the server.
            ops (TypeOps): list of aerospike operation dictionaries to perform on the record at key.
            read_all_bins (bool, optional): An optional bool, if True, read all bins in the record.
            policy (TypeBatchPolicyRead, optional): An optional dictionary of batch read policy flags.
    """

    def __init__(self, key: tuple, ops: ty.Union[TypeOps, None], read_all_bins: bool = False, policy: 'TypeBatchPolicyRead' = None) -> None:
        """
            Example::

                # Create a batch Read to read bin "a" from the record.
                import aerospike_helpers.operations as op


                bin_name = "a"

                namespace = "test"
                set = "demo"
                user_key = 1
                key = (namespace, set, user_key)

                ops = [
                    op.read(bin_name)
                ]

                br = Write(key, ops)
        """
        super().__init__(key)
        self.ops = ops
        self.read_all_bins = read_all_bins
        self._type = _Types.READ
        self._has_write = False
        self.policy = policy


class Apply(BatchRecord):
    """ BatchApply is used for executing Batch UDF (user defined function) apply operations with batch_write and retrieving results.

        Attributes:
            key (:obj:`tuple`): The aerospike key to operate on.
            module (str): Name of the lua module previously registerd with the server.
            function (str): Name of the UDF to invoke.
            args (TypeUDFArgs): List of arguments to pass to the UDF.
            record (:obj:`tuple`): The record corresponding to the requested key.
            result (int): The status code of the operation.
            in_doubt (int): Is it possible that the write transaction completed even though an error was generated. \
            This may be the case when a client error occurs (like timeout) after the command was sent \
            to the server.
            ops (TypeOps): A list of aerospike operation dictionaries to perform on the record at key.
            policy (TypeBatchPolicyApply, optional): An optional dictionary of batch apply policy flags.
    """

    def __init__(self, key: tuple, module: str, function: str, args: 'TypeUDFArgs', policy: 'TypeBatchPolicyApply' = None) -> None:
        """
            Example::

                # Create a batch Apply to apply UDF "test_func" to bin "a" from the record.
                # Assume that "test_func" takes a bin name string as an argument.
                # Assume the appropriate UDF module has already been registerd.
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
    """ Remove is used for executing Batch remove operations with batch_write and retrieving results.

        Attributes:
            key (:obj:`tuple`): The aerospike key to operate on.
            record (:obj:`tuple`): The record corresponding to the requested key.
            result (int): The status code of the operation.
            in_doubt (int): Is it possible that the write transaction completed even though an error was generated. \
            This may be the case when a client error occurs (like timeout) after the command was sent \
            to the server.
            ops (TypeOps): A list of aerospike operation dictionaries to perform on the record at key.
            policy (TypeBatchPolicyRemove, optional): An optional dictionary of batch remove policy flags.
    """

    def __init__(self, key: tuple, policy: 'TypeBatchPolicyRemove' = None) -> None:
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


#### BatchRecords ####
TypeBatchRecordList = ty.List[BatchRecord]

class BatchRecords:
    """ BatchRecords is used as input and output for multiple batch APIs.

        Attributes:
            batch_records (TypeBatchRecordList): A list of BatchRecord subtype objects used to \
            define batch operations and hold results. BatchRecord Types can be Remove, Write, \
            Read, and Apply.
            result (int): The status code of the last batch call that used this BatchRecords.
            0 if all batch subtransactions succeeded (or if the only failures were FILTERED_OUT or RECORD_NOT_FOUND)
            non 0 if an error occured. The most common error being -16 (One or more batch sub transactions failed).
    """

    def __init__(self, batch_records: TypeBatchRecordList = []) -> None:
        """
            Example::

                # Create a BatchRecords to remove a record, write a bin, and read a bin.
                # Assume client is an instantiated and connected aerospike cleint.
                import aerospike_helpers.operations as op


                namespace = "test"
                set = "demo"
                bin_name = "id"
                keys = [
                    (namespace, set, 1),
                    (namespace, set, 2),
                    (namespace, set, 3)
                ]

                brs = BatchRecords(
                    [
                        Remove(
                            key=(namespace, set, 1),
                        ),
                        Write(
                            key=(namespace, set, 100),
                            ops=[
                                op.write(bin_name, 100),
                                op.read(bin_name),
                            ]
                        ),
                        BatchRead(
                            key=(namespace, set, 333),
                            ops=[
                                op.read(bin_name)
                            ]
                        )
                    ]
                )

                # Note this call will mutate brs and set results in it.
                client.batch_write(brs)
        """
        self.batch_records = batch_records
        self.result = 0