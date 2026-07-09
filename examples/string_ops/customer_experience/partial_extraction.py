import aerospike
from . import Example

class PartialExtraction(Example):
    def run(self):
        # Old

        user_id = 1
        key = ("test", "users", user_id)
        _, _, record = self.client.get(key)
        domain = record["email"].split("@")[1]
        print(domain)

        # New

        from aerospike_helpers.expressions.resources import ResultType
        from aerospike_helpers import expressions as exp

        get_domain_expr = exp.ListGetByIndex(
            ctx=None,
            return_type=aerospike.LIST_RETURN_VALUE,
            value_type=ResultType.STRING,
            index=1,
            bin=exp.SplitSeparator("@", bin="email")
        ).compile()

        from aerospike_helpers.operations import expression_operations
        ops = [
            expression_operations.expression_read(
                bin_name="domain",
                expression=get_domain_expr
            )
        ]

        _, _, bins = self.client.operate(key, ops)
        print(bins["domain"])
