import aerospike
import sys
from aerospike import exception as ex

config = {'hosts': [('127.0.0.1', 3000)]}
client = aerospike.client(config).connect()

# Create a client and connect it to the cluster
try:
    client = aerospike.client(config).connect()
    client.truncate('test', "demo", 0)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)

# Remove old indices
try:
    client.index_remove("test", "scoreIndex")
    client.index_remove("test", "eloIndex")
except ex.AerospikeError as e:
    # Ignore if no indices found
    pass

# Insert 4 records
keyTuples = [("test", "demo", f"player{i}") for i in range(4)]
bins = [
    {"score": 100, "elo": 1400},
    {"score": 20, "elo": 1500},
    {"score": 10, "elo": 1100},
    {"score": 200, "elo": 900}
]
for keyTuple, bin in zip(keyTuples, bins):
    client.put(keyTuple, bin)

query = client.query('test', 'demo')

# Queries require a secondary index for each bin name
client.index_integer_create("test", "demo", "score", "scoreIndex")
client.index_integer_create("test", "demo", "elo", "eloIndex")
