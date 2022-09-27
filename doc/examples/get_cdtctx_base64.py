import aerospike
from aerospike_helpers import cdt_ctx

list_index = "list_index"
list_rank = "list_rank"
list_value = "list_value"
map_index = "map_index"
map_key = "map_key"
map_rank = "map_rank"
map_value = "map_value"

ctx_ops = {
    list_index: cdt_ctx.cdt_ctx_list_index,
    list_rank: cdt_ctx.cdt_ctx_list_rank,
    list_value: cdt_ctx.cdt_ctx_list_value,
    map_index: cdt_ctx.cdt_ctx_map_index,
    map_key: cdt_ctx.cdt_ctx_map_key,
    map_rank: cdt_ctx.cdt_ctx_map_rank,
    map_value: cdt_ctx.cdt_ctx_map_value,
}

def add_ctx_op(ctx_type, value):
    ctx_func = ctx_ops[ctx_type]
    return ctx_func(value)

def get_aerospike():
    endpoints = [
        ('x.y.z.a', 3000)]

    hosts = [(address[0], address[1]) for address in endpoints]

    config = {
        'hosts': hosts,
    }
    return aerospike.client(config)

ctx_list_index = []
ctx_list_index.append(add_ctx_op(list_index, 0))

client = get_aerospike().connect()
bs_b4_cdt = client.get_cdtctx_base64({'ctx':ctx_list_index})
print("base64 encoded _cdt_ctx: ", bs_b4_cdt)
assert bs_b4_cdt == "khAA"
client.close()