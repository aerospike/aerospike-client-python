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
    records = [{'buz': 1, 'baz': 1, 'foo': 5}, {'buz': 2, 'baz': 3, 'foo': 5}, {'buz': 3, 'baz': 3, 'foo': 3}]
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

    expr = predexp.And(
               predexp.EQ(predexp.IntBin("foo"), 3),
               predexp.EQ(predexp.IntBin("buz"), predexp.IntBin("foo")),
               predexp.EQ(predexp.IntBin("buz"), predexp.IntBin("baz"))
            )

    #expr = predexp.EQ(predexp.IntBin("foo"), 5)

    #expr = predexp.EQ(predexp.IntBin("buz"), predexp.IntBin("baz"))
    #expr = predexp.IntBin("bux")
    
    print(expr.compile())

    policy = {
        'predexp': expr.compile()
    }

    #try:
    records = scan.results(policy)
    # except:
    #     print('hi')
    #     exc_type, exc_value, tb = sys.exc_info()
    #     from pprint import pprint
    #     pprint(tb.tb_frame.f_locals)
    #     inner_frame = tb.tb_next.tb_frame
    #     pprint(inner_frame.f_locals)
    
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
