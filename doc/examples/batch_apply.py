# Insert 3 records
keys = [("test", "demo", f"employee{i}") for i in range(1, 4)]
bins = [
    {"id": 100, "balance": 200},
    {"id": 101, "balance": 400},
    {"id": 102, "balance": 300}
]
for key, bin in zip(keys, bins):
    client.put(key, bin)

# Apply a user defined function (UDF) to a batch
# of records using batch_apply.
client.udf_put("batch_apply.lua")

args = ["balance", 0.5, 100]
batchRecords = client.batch_apply(keys, "batch_apply", "tax", args)

print(batchRecords.result)
# 0

for batchRecord in batchRecords.batch_records:
    print(f"{batchRecord.result}: {batchRecord.record}")
# 0: (('test', 'demo', 'employee1', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'SUCCESS': 0})
# 0: (('test', 'demo', 'employee2', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'SUCCESS': 100})
# 0: (('test', 'demo', 'employee3', bytearray(b'...')), {'ttl': 2592000, 'gen': 2}, {'SUCCESS': 50})
