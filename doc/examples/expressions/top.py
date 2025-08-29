import aerospike
from aerospike_helpers import expressions as exp
import pprint

# Connect to database
config = {"hosts": [("127.0.0.1", 3000)]}
client = aerospike.client(config).connect()

# Write player records to database
keys = [("test", "demo", i) for i in range(1, 5)]
records = [
            {'user': "Chief"  , 'scores': [6, 12, 4, 21], 'kd': 1.2},
            {'user': "Arbiter", 'scores': [5, 10, 5, 8] , 'kd': 1.0},
            {'user': "Johnson", 'scores': [8, 17, 20, 5], 'kd': 0.9},
            {'user': "Regret" , 'scores': [4, 2, 3, 5]  , 'kd': 0.3}
        ]
for key, record in zip(keys, records):
    client.put(key, record)

# Example #1: Get players with a K/D ratio >= 1.0

kdGreaterThan1 = exp.GE(exp.FloatBin("kd"), 1.0).compile()
policy = {"expressions": kdGreaterThan1}
brs = client.batch_read(keys, policy=policy)

# Pretty print records' bins
for br in brs.batch_records:
    # error code for FILTERED_OUT = 27
    pprint.pprint(br.record[2] if br.result != 27 else None)
# {'user': 'Chief', 'scores': [6, 12, 4, 21], 'kd': 1.2}
# {'user': 'Arbiter', 'scores': [5, 10, 5, 8], 'kd': 1.0}
# None
# None

# Example #2: Get player with scores higher than 20
# By nesting expressions, we can create complicated filters

# Get top score
getTopScore = exp.ListGetByRank(
                None,
                aerospike.LIST_RETURN_VALUE,
                exp.ResultType.INTEGER,
                -1,
                exp.ListBin("scores")
                )
# ...then compare it
scoreHigherThan20 = exp.GE(getTopScore, 20).compile()
policy = {"expressions": scoreHigherThan20}
brs = client.batch_read(keys, policy=policy)

for br in brs.batch_records:
    pprint.pprint(br.record[2] if br.result != 27 else None)
# {'user': 'Chief', 'scores': [6, 12, 4, 21], 'kd': 1.2}
# None
# {'user': 'Johnson', 'scores': [8, 17, 20, 5], 'kd': 0.9}
# None
