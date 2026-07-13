from ... import Example

class CustomerExperienceExample(Example):
    def __init__(self):
        super().__init__()

        user_id = 1
        self.key = ("test", "users", user_id)
        ORIG_EMAIL = "  asdf@COMPANY.com "

        self.client.put(self.key, bins={"email": ORIG_EMAIL})

    def __del__(self):
        self.client.remove(self.key)
        super().__del__()
