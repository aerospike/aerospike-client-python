from aerospike_helpers.operations import operations

# Add name, update age, and return attributes
client.put(keyTuple, {'age': 25, 'career': 'delivery boy'})
ops = [
    operations.increment("age", 1000),
    operations.write("name", "J."),
    operations.prepend("name", "Phillip "),
    operations.append("name", " Fry"),
    operations.read("name"),
    operations.read("career"),
    operations.read("age")
]
(key, meta, bins) = client.operate_ordered(keyTuple, ops)

# Same output for key and meta as operate()
# But read operations are outputted as bin-value pairs
print(bins)
# [('name': 'Phillip J. Fry'), ('career': 'delivery boy'), ('age': 1025)]
