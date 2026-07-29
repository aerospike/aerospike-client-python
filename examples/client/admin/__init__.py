from ... import Example


class AdminExample(Example):
    def __init__(self):
        # TODO: admin user doesn't have enough permissions
        super().__init__(user="admin", password="admin")
