import aerospike
from aerospike import exception as ex
from aerospike_helpers import expressions as exp
from aerospike import exception as ex
import sys
config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()
try:
    # assume the fourth key has no matching record
    keys = [
        ('test', 'demo', '1'),
        ('test', 'demo', '2'),
        ("test", "demo", "batch-ops-non_existent_key")
    ]
    expr = Let(Def("bal", IntBin("balance")),
                Cond(
                    LT(Var("bal"), 50),
                    Add(Var("bal"), 50),
                    Unknown()
                )
            ).compile()
    ops = [
        expressions.expression_read("test_name", expr, aerospike.EXP_READ_DEFAULT)
    ]
    meta = {'gen': 1}
    policy = {'timeout': 1001}
    records = client.batch_get_ops(keys, ops, meta, policy)
    print(records)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)
finally:
    client.close()