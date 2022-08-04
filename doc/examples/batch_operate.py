import aerospike
from aerospike import exception as ex
from aerospike_helpers.batch import records as br
import aerospike_helpers.expressions as exp
from aerospike_helpers.operations import operations as op
import sys

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

namespace = "test"
set = "demo"

keys = [(namespace, set, i) for i in range(150)]
records = [{"id": i, "balance": i * 10} for i in range(150)]
for key, rec in zip(keys, records):
    client.put(key, rec)

# Batch add 10 to the bin "balance" and read it if it's over
# 1000 NOTE: batch_operate ops must include a write op
# get_batch_ops or get_many can be used for all read ops use cases.
expr = exp.GT(exp.IntBin("balance"), 1000).compile()
ops = [
    op.increment("balance", 10),
    op.read("balance")
]
policy_batch = {"expressions": expr}

try:
    res = client.batch_operate(keys, ops, policy_batch)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    client.close()
    sys.exit(1)

# res is an instance of BatchRecords
# the field, batch_records, contains a BatchRecord instance
# for each key used by the batch_operate call.
# the field, results, is 0 if all batch subtransactions completed succesfully
# or the only failures are FILTERED_OUT or RECORD_NOT_FOUND.
# Otherwise its value corresponds to an as_status error code and signifies that
# one or more of the batch subtransactions failed. Each BatchRecord instance
# also has a results field that signifies the status of that batch subtransaction.

if res.result == 0:

    # BatchRecord 100 should have a result code of 27 meaning it was filtered out by an expression.
    print("BatchRecord 100 result: {result}".format(result=res.batch_records[100].result))

    # BatchRecord 100 should, record be None.
    print("BatchRecord 100 record: {record}".format(record=res.batch_records[100].record))

    # BatchRecord 101 should have a result code of 0 meaning it succeeded.
    print("BatchRecord 101 result: {result}".format(result=res.batch_records[101].result))

    # BatchRecord 101, record should be populated.
    print("BatchRecord 101 record: {record}".format(record=res.batch_records[101].record))

else:
    # Some batch sub transaction failed.
    print("res result: {result}".format(result=res.result))

for key in keys:
    client.remove(key)

client.close()