# Insert 3 records
keys = [("test", "demo", f"employee{i}") for i in range(1, 4)]
bins = [
    {"id": 100, "balance": 200},
    {"id": 101, "balance": 400},
    {"id": 102, "balance": 300}
]
for key, bin in zip(keys, bins):
    client.put(key, bin)

batchRecords = client.batch_remove(keys)
# A result of 0 means success
print(batchRecords.result)
# 0