# Keys
# Only insert two records with the first and second key
keyTuples = [
    ('test', 'demo', '1'),
    ('test', 'demo', '2'),
    ('test', 'demo', '3'),
]
client.put(keyTuples[0], {'bin1': 'value'})
client.put(keyTuples[1], {'bin1': 'value'})

# Check for existence of records using all three keys
keyMetadata = client.exists_many(keyTuples)
print(keyMetadata[0])
print(keyMetadata[1])
print(keyMetadata[2])

# (('test', 'demo', '1', bytearray(...)), {'ttl': 2592000, 'gen': 1})
# (('test', 'demo', '2', bytearray(...)), {'ttl': 2592000, 'gen': 1})
# (('test', 'demo', '3', bytearray(...)), None)
