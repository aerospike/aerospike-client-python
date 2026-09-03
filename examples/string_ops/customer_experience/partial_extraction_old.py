from . import CustomerExperienceExample

class PartialExtractionOld(CustomerExperienceExample):
    def run(self):
        _, _, record = self.client.get(self.key)
        domain = record["email"].split("@")[1]
        print(domain)
