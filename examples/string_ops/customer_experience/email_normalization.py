from .. import Example

class EmailNormalization(Example):
    def run(self):
        user_id = 1
        key = ("test", "users", user_id)
        ORIG_EMAIL = "  asdf@COMPANY.com "

        self.client.put(key, bins={"email": ORIG_EMAIL})

        # Old
        _, _, record = self.client.get(key)
        email = record["email"].strip().lower()

        self.client.put(key, {"email": email})

        # Reset
        self.client.put(key, bins={"email": ORIG_EMAIL})

        # New

        from aerospike_helpers.operations import string_operations as so
        from aerospike_helpers import expressions as exp

        ops = [so.trim("email"), so.lower("email")]
        self.client.operate(key, ops)

        expr = exp.Contains("@company.com", exp.StrBin("email")).compile()
        query = self.client.query("test", "users")
        records = query.results(policy={"expressions": expr})

        print(records)
