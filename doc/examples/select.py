# Record to select from
client.put(keyTuple, {'bin1': 4, 'bin2': 3})

# Only get bin1
(key, meta, bins) = client.select(keyTuple, ['bin1'])

# Similar output to get()
print(key) # ('test', 'demo', 'key', bytearray(b'...'))
print(meta) # {'ttl': 2592000, 'gen': 1}
print(bins) # {'bin1': 4}

# Get all bins
(key, meta, bins) = client.select(keyTuple, ['bin1', 'bin2'])
print(bins) # {'bin1': 4, 'bin2': 3}

# Get nonexistent bin
(key, meta, bins) = client.select(keyTuple, ['bin3'])
print(bins) # {}
