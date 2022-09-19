import aerospike

config = {'hosts': [('127.0.0.1', 3000)],
            'lua': {'system_path':'/usr/local/aerospike/lua/',
                    'user_path':'./'}}
client = aerospike.client(config).connect()
client.udf_put("example.lua")

# Remove index if it already exists
from aerospike import exception as ex
try:
    client.index_remove("test", "ageIndex")
except ex.IndexNotFound:
    pass

bins = [
    {"name": "Jeff", "age": 20},
    {"name": "Derek", "age": 24},
    {"name": "Derek", "age": 21},
    {"name": "Derek", "age": 29},
    {"name": "Jeff", "age": 29},
]
keys = [("test", "users", f"user{i}") for i in range(len(bins))]
for key, recordBins in zip(keys, bins):
    client.put(key, recordBins)

client.index_integer_create("test", "users", "age", "ageIndex")

query = client.query('test', 'users')
query.apply('example', 'group_count', ['name', 'age', 21])
names = query.results()

# we expect a dict (map) whose keys are names, each with a count value
print(names)
# One of the Jeffs is excluded because he is under 21
# [{'Derek': 3, 'Jeff': 1}]

# Cleanup
client.index_remove("test", "ageIndex")
client.batch_remove(keys)
client.close()
