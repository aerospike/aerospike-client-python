from ... import Example


class AdminExample(Example):
    def __init__(self):
        super().__init__(user="admin", password="admin")
