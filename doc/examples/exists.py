# Check non-existent record
(key, meta) = client.exists(keyTuple)

print(key) # ('test', 'demo', 'key', bytearray(b'...'))
print(meta) # None

# Check existing record
client.put(keyTuple, {'bin1': 4})
(key, meta) = client.exists(keyTuple)

print(key) # ('test', 'demo', 'key', bytearray(b'...'))
print(meta) # {'ttl': 2592000, 'gen': 1}
