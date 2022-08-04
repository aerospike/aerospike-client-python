import aerospike
from aerospike import exception as ex
from aerospike_helpers.batch import records as br
import aerospike_helpers.expressions as exp
from aerospike_helpers.operations import operations as op
import sys

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

# Apply different operations to different keys
# using batch_write.
w_batch_record = br.BatchRecords(
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
                op.read("id"),
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

try:
    # batch_write modifies its BatchRecords argument.
    # Results for each BatchRecord will be set in their result,
    # record, and in_doubt fields.

    client.batch_write(w_batch_record)

    for batch_record in w_batch_record.batch_records:
        print(batch_record.result)
        print(batch_record.record)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)
finally:
    client.close()