import aerospike


# self: test class which contains the already connected client
# methodParent: name of module or class this method belongs to
def get_cdtctx_base64_method(self, methodParent: str):
    parentToMethod = {
        "aerospike": aerospike.get_cdtctx_base64,
        "client": self.as_connection.get_cdtctx_base64
    }
    get_cdtctx_base64 = parentToMethod[methodParent]
    return get_cdtctx_base64


def get_expression_base64_method(self, methodParent: str):
    parentToMethod = {
        "aerospike": aerospike.get_expression_base64,
        "client": self.as_connection.get_expression_base64
    }
    get_expression_base64 = parentToMethod[methodParent]
    return get_expression_base64
