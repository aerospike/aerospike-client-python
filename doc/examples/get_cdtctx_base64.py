import aerospike
from aerospike_helpers import cdt_ctx

config = {'hosts': [('127.0.0.1', 3000)]}
client = aerospike.client(config)

ctxs = [cdt_ctx.cdt_ctx_list_index(0)]
ctxs_base64 = client.get_cdtctx_base64(ctxs)
print("Base64 encoding of ctxs:", ctxs_base64)

client.close()
