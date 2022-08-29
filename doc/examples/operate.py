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
(key, meta, bins) = client.operate(key, ops)

print(key) # ('test', 'demo', None, bytearray(b'...'))
# The generation should only increment once
# A transaction is *atomic*
print(meta) # {'ttl': 2592000, 'gen': 2}
print(bins) # Will display all bins selected by read operations
# {'name': 'Phillip J. Fry', 'career': 'delivery boy', 'age': 1025}
