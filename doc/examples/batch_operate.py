from aerospike_helpers.operations import operations as op

# Insert 3 records
keys = [("test", "demo", f"employee{i}") for i in range(1, 4)]
bins = [
    {"id": 100, "balance": 200},
    {"id": 101, "balance": 400},
    {"id": 102, "balance": 300}
]
for key, bin in zip(keys, bins):
    client.put(key, bin)

# Increment ID by 100 and balance by 500 for all employees
# NOTE: batch_operate ops must include a write op
# get_batch_ops or get_many can be used for all read ops use cases.
ops = [
    op.increment("id", 100),
    op.increment("balance", 500),
    op.read("balance")
]

batchRecords = client.batch_operate(keys, ops)
print(batchRecords.result)
# 0

# Print each individual transaction's results
# and record if it was read from
for batchRecord in batchRecords.batch_records:
    print(f"{batchRecord.result}: {batchRecord.record}")
# 0: (('test', 'demo', 'employee1', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'id': None, 'balance': 700})
# 0: (('test', 'demo', 'employee2', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'id': None, 'balance': 900})
# 0: (('test', 'demo', 'employee3', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'id': None, 'balance': 800})
