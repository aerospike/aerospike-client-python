# -*- coding: utf-8 -*-

import pytest
from .test_base_class import TestBaseClass
import aerospike
from aerospike import exception as e
from .index_helpers import ensure_dropped_index
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


ctx_list_index = []
ctx_list_index.append(add_ctx_op(list_index, 0))

ctx_list_rank = []
ctx_list_rank.append(add_ctx_op(list_rank, -1))

ctx_list_value = []
ctx_list_value.append(add_ctx_op(list_value, 3))

ctx_map_index = []
ctx_map_index.append(add_ctx_op(map_index, 0))

ctx_map_key = []
ctx_map_key.append(add_ctx_op(map_key, "sb"))

ctx_map_rank = []
ctx_map_rank.append(add_ctx_op(map_rank, -1))

ctx_map_value = []
ctx_map_value.append(add_ctx_op(map_value, 3))

ctx_empty = []


class TestCDTIndexB64(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        if TestBaseClass.major_ver < 6 or (TestBaseClass.major_ver == 6 and TestBaseClass.minor_ver == 0):
            pytest.skip("It only applies to >= 6.1 enterprise edition")

        keys = []
        for i in range(5):
            key = ("test", "demo", i)
            rec = {
                "name": "name%s" % (str(i)),
                "addr": "name%s" % (str(i)),
                "numeric_list": [1, 2, 3, 4],
                "string_list": ["a", "b", "c", "d"],
                "geojson_list": [
                    aerospike.GeoJSON({"type": "Point", "coordinates": [-122.096449, 37.421868]}),
                    aerospike.GeoJSON({"type": "Point", "coordinates": [-122.053321, 37.434212]}),
                ],
                "numeric_map": {"a": 1, "b": 2, "c": 3},
                "string_map": {"sa": "a", "sb": "b", "sc": "c"},
                "age": i,
                "no": i,
            }
            as_connection.put(key, rec)
            keys.append(key)

        def teardown():
            """
            Teardown method.
            """
            for key in keys:
                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    def test_get_cdtctxb64_with_correct_parameters(self):
        """
        Invoke get_cdtctx_base64() with correct arguments
        """
        policy = {}
        bs_b4_cdt = self.as_connection.get_cdtctx_base64([cdt_ctx.cdt_ctx_list_index(0)])

        r = []
        r.append("sindex-create:ns=test;set=demo;indexname=test_string_list_cdt_index")
        r.append(";indextype=%s" % (cdt_ctx.index_type_string(aerospike.INDEX_TYPE_LIST)))
        r.append(";indexdata=string_list,%s" % (cdt_ctx.index_datatype_string(aerospike.INDEX_STRING)))
        r.append(";context=%s" % (bs_b4_cdt))
        req = "".join(r)

        # print("req is ==========={}", req)
        retobj = self.as_connection.info_all(req, policy=None)
        # print("retobj is ==========={}", retobj)

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj != 0

    def test_get_cdtctxb64_with_invalid_parameters(self):
        """
        Invoke get_cdtctx_base64() with invalid arguments
        """
        try:
            self.as_connection.get_cdtctx_base64({})
        except e.ParamError:
            pass

    def test_get_cdtctxb64_with_invalid_ctx(self):
        """
        Invoke get_cdtctx_base64() with invalid arguments
        """
        try:
            self.as_connection.get_cdtctx_base64(ctx_empty)
        except e.ParamError:
            pass
