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
from aerospike_helpers import expressions as exp

ops = [so.trim("email"), so.lower("email")]
client.operate(key, ops)

expr = exp.Contains("@company.com", exp.StrBin("email")).compile()
query = client.query("test", "users")
records = query.results(policy={"expressions": expr})

print(records)

# Partial extraction

_, _, record = client.get(key)
domain = record["email"].split("@")[1]
