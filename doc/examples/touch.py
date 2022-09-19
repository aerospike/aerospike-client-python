# Insert record and get its metadata
client.put(keyTuple, bins = {"bin1": 4})
(key, meta) = client.exists(keyTuple)
print(meta) # {'ttl': 2592000, 'gen': 1}

# Explicitly set TTL to 120
# and increment generation
client.touch(keyTuple, 120)

# Record metadata should be updated
(key, meta) = client.exists(keyTuple)
print(meta) # {'ttl': 120, 'gen': 2}
