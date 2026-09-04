from . import CustomerExperienceExample

class EmailNormalizationOld(CustomerExperienceExample):
    def run(self):
        _, _, record = self.client.get(self.key)
        email = record["email"].strip().lower()

        self.client.put(self.key, {"email": email})
