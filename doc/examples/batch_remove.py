import aerospike
from aerospike import exception as ex
import sys

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

keys = [(namespace, set, i) for i in range(10)]

# Delete the records using batch_remove
try:
    res = client.batch_remove(keys)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    client.close()
    sys.exit(1)

# Should be 0 signifying success.
print("BatchRecords result: {result}".format(result=res.result))

client.close()