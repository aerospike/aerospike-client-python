from __future__ import print_function
import aerospike
from aerospike_helpers import predexp
#from aerospike import predexp
from aerospike import exception as ex
import sys
import time

config = { 'hosts': [('127.0.0.1', 3000)]}
client = aerospike.client(config).connect()

# register udf
try:
    client.udf_put('/home/dylan/Desktop/python-client/aerospike-client-python/examples/my_udf.lua')
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    client.close()
    sys.exit(1)

# put records and run scan
try:
    keys = [('test', 'demo', 1), ('test', 'demo', 2), ('test', 'demo', 3)]
    records = [{'number': 1}, {'number': 2}, {'number': 3}]
    for i in range(3):
        client.put(keys[i], records[i])

    scan = client.scan('test', 'demo')

    # preds = [ # check that the record has value < 2 or value == 3 in bin 'name'
    #     predexp.integer_bin('number'),
    #     predexp.integer_value(2),
    #     predexp.integer_less(),
    #     predexp.integer_bin('number'),
    #     predexp.integer_value(3),
    #     predexp.integer_equal(),
    #     predexp.predexp_or(2)
    # ]

    expr = predexp.And(predexp.EQ(predexp.IntBin("foo"), 5),
               predexp.EQ(predexp.IntBin("bar"), predexp.IntBin("baz")),
               predexp.EQ(predexp.IntBin("buz"), predexp.IntBin("baz")))
    
    print(expr.compile())

    policy = {
        'predexp': expr.compile()
    }

    records = scan.results(policy)
    print(records)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)
finally:
    client.close()
# the scan only returns records that match the predexp
# EXPECTED OUTPUT:
# [
#   (('test', 'demo', 1, bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')), {'gen': 2, 'ttl': 2591999}, {'number': 1}),
#   (('test', 'demo', 3, bytearray(b'\xb1\xa5`g\xf6\xd4\xa8\xa4D9\xd3\xafb\xbf\xf8ha\x01\x94\xcd')), {'gen': 13, 'ttl': 2591999}, {'number': 3})
# ]
