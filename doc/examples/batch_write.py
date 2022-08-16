from aerospike_helpers.batch import records as br
from aerospike_helpers.operations import operations as op

# Keys
# Only insert two records with the first and second key
keyTuples = [
    ('test', 'demo', 'Robert'),
    ('test', 'demo', 'Daniel'),
    ('test', 'demo', 'Patrick'),
]
client.put(keyTuples[0], {'id': 100, 'balance': 400})
client.put(keyTuples[1], {'id': 101, 'balance': 200})
client.put(keyTuples[2], {'id': 102, 'balance': 300})

# Apply different operations to different keys
batchRecords = br.BatchRecords(
    [
        # Remove Robert from system
        br.Remove(
            key = keyTuples[0],
        ),
        # Modify Daniel's ID and balance 
        br.Write(
            key = keyTuples[1],
            ops = [
                op.write("id", 200),
                op.write("balance", 100),
                op.read("id"),
            ],
        ),
        # Read Patrick's ID
        br.Read(
            key = keyTuples[2],
            ops=[
                op.read("id")
            ],
            policy=None
        ),
    ]
)

client.batch_write(batchRecords)

# batch_write modifies its BatchRecords argument.
# Results for each BatchRecord will be set in the result, record, and in_doubt fields.
for batchRecord in batchRecords.batch_records:
    print(batchRecord.result)
    print(batchRecord.record)
# Note how written bins return None if their values aren't read
# And removed records have an empty bins dictionary
# 0
# (('test', 'demo', 'Robert', bytearray(b'...')), {'ttl': 4294967295, 'gen': 0}, {})
# 0
# (('test', 'demo', 'Daniel', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'id': 200, 'balance': None})
# 0
# (('test', 'demo', 'Patrick', bytearray(b'...')), {'ttl': 2592000, 'gen': 1}, {'id': 102})
