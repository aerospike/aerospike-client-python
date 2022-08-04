import aerospike
from aerospike import exception as ex
import sys

config = { 'hosts': [('127.0.0.1', 3000)] }
client = aerospike.client(config).connect()

keys = [(namespace, set, i) for i in range(10)]

# Apply a user defined function (UDF) to a batch
# of records using batch_apply.
module = "test_record_udf"
path_to_module = "/path/to/test_record_udf.lua"
function = "bin_udf_operation_integer"
args = ["balance", 10, 5]

client.udf_put(path_to_module)

try:
    # This should add 15 to each balance bin.
    res = client.batch_apply(keys, module, function, args)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    client.close()
    sys.exit(1)

print("res result: {result}".format(result=res.result))
for batch_record in res.batch_records:
    print(batch_record.result)
    print(batch_record.record)


client.close()

# bin_udf_operation_integer lua
# --[[UDF which performs arithmetic operation on bin containing
#     integer value.
# --]]
# function bin_udf_operation_integer(record, bin_name, x, y)
#     record[bin_name] = (record[bin_name] + x) + y
#     if aerospike:exists(record) then
#         aerospike:update(record)
#     else
#         aerospike:create(record)
#     end
#     return record[bin_name]
# end