# Insert records for 3 players and their scores
keyTuples = [("test", "demo", i) for i in range(1, 4)]
bins = [
    {"scores": [1, 4, 3, 10]},
    {"scores": [20, 1, 4, 28]},
    {"scores": [50, 20, 10, 20]},
]
for keyTuple, bin in zip(keyTuples, bins):
    client.put(keyTuple, bin)

# Get highest scores for each player
from aerospike_helpers.operations import list_operations
ops = [
    list_operations.list_get_by_rank("scores", -1, aerospike.LIST_RETURN_VALUE)
]
records = client.batch_get_ops(keyTuples, ops)

# Print results
for _, _, bins in records:
    print(bins)
# {'scores': 10}
# {'scores': 28}
# {'scores': 50}
