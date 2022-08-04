# Insert record
bins = {"bin1": 0, "bin2": 1}
client.put(keyTuple, bins)

# Remove bin1
client.remove_bin(keyTuple, ['bin1'])

# Only bin2 shold remain
(keyTuple, meta, bins) = client.get(keyTuple)
print(bins)
# {'bin2': 1}
