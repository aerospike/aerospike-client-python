import aerospike
import marshal
import json

# Serializers and deserializers
# Both local and global serializers use json library
# Functions print which one is being used

def classSerializer(obj):
    print("Using class serializer")
    return json.dumps(obj)

def classDeserializer(bytes):
    print("Using class deserializer")
    return json.loads(bytes)

def localSerializer(obj):
    print("Using local serializer")
    return json.dumps(obj)

def localDeserializer(bytes):
    print("Using local deserializer")
    return json.loads(bytes)

# First client has class-level serializer set in aerospike module
aerospike.set_serializer(classSerializer)
aerospike.set_deserializer(classDeserializer)
config = {
    'hosts': [('127.0.0.1', 3000)]
}
client = aerospike.client(config).connect()

# Second client has instance-level serializer set in client config
config['serialization'] = (localSerializer, localDeserializer)
client2 = aerospike.client(config).connect()

# Keys: foo1, foo2, foo3
keys = [('test', 'demo', f'foo{i}') for i in range(1, 4)]
# Tuple is an unsupported type
tupleBin = {'bin': (1, 2, 3)}

# Use the default, built-in serialization (cPickle)
client.put(keys[0], tupleBin)
(_, _, bins) = client.get(keys[0])
print(bins)

# Expected output:
# {'bin': (1, 2, 3)}

# First client uses class-level, user-defined serialization
# No instance-level serializer was declared
client.put(keys[1], tupleBin, serializer=aerospike.SERIALIZER_USER)
(_, _, bins) = client.get(keys[1])
# Notice that json-encoding a tuple produces a list
print(bins)

# Expected output:
# Using class serializer
# Using class deserializer
# {'bin': [1, 2, 3]}

# Second client uses instance-level, user-defined serialization
# Instance-level serializer overrides class-level serializer
client2.put(keys[2], tupleBin, serializer=aerospike.SERIALIZER_USER)
(_, _, bins) = client2.get(keys[2])
print(bins)

# Expected output:
# Using local serializer
# Using local deserializer
# {'bin': [1, 2, 3]}

# Cleanup
client.batch_remove(keys)
client.close()
client2.close()
