from . import CustomerExperienceExample

from aerospike_helpers.operations import string_operations as so
from aerospike_helpers import expressions as exp

class EmailNormalizationNew(CustomerExperienceExample):
    def run(self):
        ops = [so.trim("email"), so.lower("email")]
        self.client.operate(self.key, ops)

        expr = exp.Contains("@company.com", exp.StrBin("email")).compile()
        query = self.client.query("test", "users")
        records = query.results(policy={"expressions": expr})

        print(records)
