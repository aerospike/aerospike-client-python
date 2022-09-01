# Insert a record
client.put(keyTuple, {"bin1": 4})

# Try to remove it with the wrong generation
try:
    client.remove(keyTuple, meta={'gen': 5}, policy={'gen': aerospike.POLICY_GEN_EQ})
except ex.AerospikeError as e:
    # Error: AEROSPIKE_ERR_RECORD_GENERATION [3]
    print("Error: {0} [{1}]".format(e.msg, e.code))

# Remove it ignoring generation
client.remove(keyTuple)
