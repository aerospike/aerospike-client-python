# Keys
keyTuples = [
    ('test', 'demo', '1'),
    ('test', 'demo', '2'),
    ('test', 'demo', '3'),
]
# Only insert two records with the first and second key
client.put(keyTuples[0], {'bin1': 'value'})
client.put(keyTuples[1], {'bin1': 'value'})

# Try to get records with all three keys
records = client.get_many(keyTuples)
# The third record tuple should have 'meta' and 'bins' set to none
# Because there is no third record
print(records[0])
print(records[1])
print(records[2])

# Expected output:
# (('test', 'demo', '1', bytearray(...)), {'ttl': 2592000, 'gen': 1}, {'bin1': 'value'})
# (('test', 'demo', '2', bytearray(...)), {'ttl': 2592000, 'gen': 1}, {'bin1': 'value'})
# (('test', 'demo', '3', bytearray(...)), None, None)
