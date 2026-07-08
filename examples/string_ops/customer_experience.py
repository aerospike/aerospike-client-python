import aerospike

config = {
    "hosts": [("127.0.0.1", 3000)]
}
client = aerospike.client(config)

user_id = 1
key = ("test", "users", user_id)
ORIG_EMAIL = "  asdf@COMPANY.com "

client.put(key, bins={"email": ORIG_EMAIL})

# Old
_, _, record = client.get(key)
email = record["email"].strip().lower()

client.put(key, {"email": email})

# Reset
client.put(key, bins={"email": ORIG_EMAIL})

# New

from aerospike_helpers.operations import string_operations as so
from aerospike_helpers.operations import expression_operations
from aerospike_helpers import expressions as exp

ops = [so.trim("email"), so.lower("email")]
client.operate(key, ops)

expr = exp.Contains("@company.com", exp.StrBin("email")).compile()
query = client.query("test", "users")
records = query.results(policy={"expressions": expr})

print(records)

# Partial extraction

from aerospike_helpers.expressions.resources import ResultType

_, _, record = client.get(key)
domain = record["email"].split("@")[1]
print(domain)

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

_, _, bins = client.operate(key, ops)
print(bins["domain"])
