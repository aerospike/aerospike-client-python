import aerospike
from . import CustomerExperienceExample

from aerospike_helpers.expressions.resources import ResultType
from aerospike_helpers import expressions as exp
from aerospike_helpers.operations import expression_operations

class PartialExtractionNew(CustomerExperienceExample):
    def run(self):
        get_domain_expr = exp.ListGetByIndex(
            ctx=None,
            return_type=aerospike.LIST_RETURN_VALUE,
            value_type=ResultType.STRING,
            index=1,
            bin=exp.SplitSeparator("@", bin="email")
        ).compile()

        ops = [
            expression_operations.expression_read(
                bin_name="domain",
                expression=get_domain_expr
            )
        ]

        _, _, bins = self.client.operate(self.key, ops)
        print(bins["domain"])
