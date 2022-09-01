# Get nonexistent record
try:
    client.get(keyTuple)
except ex.RecordNotFound as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    # Error: 127.0.0.1:3000 AEROSPIKE_ERR_RECORD_NOT_FOUND [2]

# Get existing record
client.put(keyTuple, {'bin1': 4})
(key, meta, bins) = client.get(keyTuple)

print(key) # ('test', 'demo', None, bytearray(b'...'))
print(meta) # {'ttl': 2592000, 'gen': 1}
print(bins) # {'bin1': 4}
