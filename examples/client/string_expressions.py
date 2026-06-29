from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import expression_operations as expr_ops

expr = exp.Contains(exp.StrBin("email"), "@company.com")

ops = [
    expr_ops.expression_read("upper_name", exp.Upper(exp.StrBin("name")))
]
