# Insert 4 records with these keys
keyTuples = [
    ('test', 'demo', 1),
    ('test', 'demo', 2),
    ('test', 'demo', 3),
    ('test', 'demo', 4)
]
# Only records 1, 2, 4 have a bin called bin2
client.put(keyTuples[0], {'bin1': 20, 'bin2': 40})
client.put(keyTuples[1], {'bin1': 11, 'bin2': 50})
client.put(keyTuples[2], {'bin1': 50,             'bin3': 20})
client.put(keyTuples[3], {'bin1': 87, 'bin2': 76, 'bin3': 40})

# Get all 4 records and filter out every bin except bin2
records = client.select_many(keyTuples, ['bin2'])
for record in records:
    print(record)

# (('test', 'demo', 1, bytearray(...)), {'ttl': 2592000, 'gen': 1}, {'bin2': 40})
# (('test', 'demo', 2, bytearray(...)), {'ttl': 2592000, 'gen': 1}, {'bin2': 50})
# (('test', 'demo', 3, bytearray(...)), {'ttl': 2592000, 'gen': 1}, {})
# (('test', 'demo', 4, bytearray(...)), {'ttl': 2592000, 'gen': 1}, {'bin2': 76})
