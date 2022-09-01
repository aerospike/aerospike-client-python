from aerospike_helpers import expressions as exp

# Compile expression
expr = exp.Eq(exp.IntBin("bin1"), 6).compile()

base64 = client.get_expression_base64(expr)
print(base64)
# kwGTUQKkYmluMQY=
