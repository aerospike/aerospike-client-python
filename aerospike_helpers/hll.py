# Hyper log log definitions
import aerospike


class _hll:
    """
    Class used to represent a single ctx_operation.
    """
    def __init__(self, id=None, value=None):
        self.id = id
        self.value = value