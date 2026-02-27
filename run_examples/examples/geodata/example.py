# Imports
import aerospike
from aerospike import exception as ex
import sys

# Configure the client
config = {
    'hosts': [ ('127.0.0.1', 3000)]
}

# Create a client and connect it to the cluster
try:
    client = aerospike.client(config)
    client.truncate('test', "demo", 0)
except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)

# Record key tuple: (namespace, set, key)
keyTuple = ('test', 'demo', 'key')


# Example code
import aerospike
# Create GeoJSON point using WGS84 coordinates.
latitude = 28.608389
longitude = -80.604333
loc = aerospike.geodata({'type': "Point",
                        'coordinates': [longitude, latitude]})

client.close()