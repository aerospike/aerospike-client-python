# Insert a record with bin1
client.put(keyTuple, {'bin1': 4})

# Insert another bin named bin2
client.put(keyTuple, {'bin2': "value"})

# Remove bin1 from this record
client.put(keyTuple, {'bin2': aerospike.null()})

# Removing the last bin should delete this record
client.put(keyTuple, {'bin1': aerospike.null()})
