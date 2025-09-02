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


class TestCDTIndex(object):
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

    def test_pos_cdtindex_with_correct_parameters(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    def test_pos_cdtindex_with_info_command(self):
        """
        Invoke index_cdt_create() with info command
        """
        policy = {}

        bs_b4_cdt = self.as_connection.get_cdtctx_base64(ctx_list_index)

        r = []
        r.append("sindex-create:ns=test;set=demo;indexname=test_string_list_cdt_index")
        r.append(";indextype=%s" % (cdt_ctx.index_type_string(aerospike.INDEX_TYPE_LIST)))
        r.append(";indexdata=string_list,%s" % (cdt_ctx.index_datatype_string(aerospike.INDEX_STRING)))
        r.append(";context=%s" % (bs_b4_cdt))
        req = "".join(r)

        # print("req is ==========={}", req)
        retobj = self.as_connection.info_all(req, policy=None)
        # print("res is ==========={}", res)

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj != 0

    def test_pos_cdtindex_with_listrank_correct_parameters(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_rank},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    def test_pos_cdtindex_with_listvalue_correct_parameters(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_value},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    def test_pos_cdtindex_with_mapindex_correct_parameters(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_MAPKEYS,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_map_index},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    def test_pos_cdtindex_with_mapvalue_correct_parameters(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_MAPVALUES,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_map_value},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    def test_pos_cdtindex_with_maprankvalue_correct_parameters(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_MAPVALUES,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_map_rank},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    # TODO: duplicate test name
    def test_pos_cdtindex_with_correct_parameters1(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_MAPVALUES,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_map_rank},
            policy,
        )

        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

        assert retobj == 0

    def test_pos_cdtindex_with_correct_parameters_numeric(self):
        """
        Invoke index_cdt_create() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "numeric_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_NUMERIC,
            "test_numeric_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "test_numeric_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_numeric_list_cdt_index")

    def test_pos_cdtindex_with_correct_parameters_set_length_extra(self):
        # Invoke index_cdt_create() with correct arguments and set length
        # extra
        set_name = "a"
        for _ in range(100):
            set_name = set_name + "a"
        policy = {}
        try:
            self.as_connection.index_cdt_create(
                "test",
                set_name,
                "string_list",
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_STRING,
                "test_string_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )
            assert False
        except e.InvalidRequest as exception:
            assert exception.code == 4
        except Exception as exception:
            assert isinstance(exception, e.InvalidRequest)

    def test_pos_cdtindex_with_incorrect_bin(self):
        """
        Invoke createindex() with incorrect bin
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list1",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

    def test_pos_create_same_cdtindex_multiple_times(self):
        """
        Invoke createindex() with multiple times on same bin
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "numeric_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_NUMERIC,
            "test_numeric_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )
        if retobj == 0:
            try:
                self.as_connection.index_cdt_create(
                    "test",
                    "demo",
                    "numeric_list",
                    aerospike.INDEX_TYPE_LIST,
                    aerospike.INDEX_NUMERIC,
                    "test_numeric_list_cdt_index",
                    {"ctx": ctx_list_index},
                    policy,
                )
            except e.IndexFoundError:
                assert self.server_version < [6, 1]
            self.as_connection.index_remove("test", "test_numeric_list_cdt_index", policy)
            ensure_dropped_index(self.as_connection, "test", "test_numeric_list_cdt_index")
        else:
            assert False

    def test_pos_create_same_cdtindex_multiple_times_different_bin(self):
        """
        Invoke createindex() with multiple times on different bin
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )
        if retobj == 0:
            with pytest.raises(e.IndexFoundError):
                retobj = self.as_connection.index_cdt_create(
                    "test",
                    "demo",
                    "numeric_list",
                    aerospike.INDEX_TYPE_LIST,
                    aerospike.INDEX_NUMERIC,
                    "test_string_list_cdt_index",
                    {"ctx": ctx_list_index},
                    policy,
                )
                self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
                ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

            self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
            ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")
        else:
            assert True is False

    def test_pos_create_different_cdtindex_multiple_times_same_bin(self):
        """
                    Invoke createindex() with multiple times on same bin with different
        name
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )
        if retobj == 0:
            try:
                retobj = self.as_connection.index_cdt_create(
                    "test",
                    "demo",
                    "string_list",
                    aerospike.INDEX_TYPE_LIST,
                    aerospike.INDEX_STRING,
                    "test_string_list_cdt_index1",
                    {"ctx": ctx_list_index},
                    policy,
                )
            except e.IndexFoundError:
                assert self.server_version < [6, 1]

            self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
            ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")
        else:
            assert True is False

    def test_pos_createcdtindex_with_policy(self):
        """
        Invoke createindex() with policy
        """
        policy = {"timeout": 10000}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "num_list_pol",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_NUMERIC,
            "test_numeric_list_cdt_index_pol",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "test_numeric_list_cdt_index_pol", policy)
        ensure_dropped_index(self.as_connection, "test", "test_numeric_list_cdt_index_pol")

    def test_pos_createcdtindex_with_policystring(self):
        """
        Invoke createindex() with policy
        """
        policy = {"timeout": 10000}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "test_string_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

    """
    This test case causes a db crash and hence has been commented. Work pending
on the C-client side
    def test_createindex_with_long_index_name(self):
            Invoke createindex() with long index name
        policy = {}
        retobj = self.as_connection.index_cdt_create( 'test', 'demo',
'age',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9qwfa\
sdcfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc', {'ctx': ctx_list_index}, policy)

        assert retobj == 0L
        self.as_connection.index_remove(policy, 'test',
'bin2_integer_indexsdadadfasdfasdfeartfqrgahfasdfheudsdfasdfawf312342q3453rf9qwfasd\
cfasdcalskdcbacfq34915rwcfasdcascnabscbaskjdbcalsjkbcdasc');
    """

    def test_pos_create_liststringindex_unicode_positive(self):
        """
        Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "string_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_STRING,
            "uni_name_index",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "uni_name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "uni_name_index")

    def test_pos_create_list_integer_index_unicode(self):
        """
        Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo",
            "numeric_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_NUMERIC,
            "uni_age_index",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "uni_age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "uni_age_index")

    # def test_pos_create_list_geojson_index(self):
    #     """
    #         Invoke createindex() with correct arguments
    #     """
    #     policy = {}
    #     retobj = self.as_connection.index_cdt_create(
    #         'test', 'demo', 'geojson_list', aerospike.INDEX_TYPE_LIST,
    #         aerospike.INDEX_GEO2DSPHERE,
    #         'geo_index', {'ctx': ctx_list_index}, policy)

    #     assert retobj == 0
    #     self.as_connection.index_remove('test', 'geo_index', policy)
    #     ensure_dropped_index(self.as_connection, 'test', 'geo_index')

    # Negative tests
    def test_neg_cdtindex_with_namespace_is_none(self):
        """
        Invoke createindex() with namespace is None
        """
        policy = {}
        try:
            self.as_connection.index_cdt_create(
                None,
                "demo",
                "string_list",
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_STRING,
                "test_string_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Namespace should be a string"

    def test_neg_cdtindex_with_set_is_int(self):
        """
        Invoke createindex() with set is int
        """
        policy = {}
        try:
            self.as_connection.index_cdt_create(
                "test",
                1,
                "string_list",
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_STRING,
                "test_string_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )
            assert False
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Set should be string, unicode or None"
        except Exception as exception:
            assert isinstance(exception, e.ParamError)

    def test_neg_cdtindex_with_set_is_none(self):
        """
        Invoke createindex() with set is None
        """
        policy = {}
        try:
            self.as_connection.index_cdt_create(
                "test",
                None,
                "string_list",
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_STRING,
                "test_string_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Set should be a string"
        self.as_connection.index_remove("test", "test_string_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_string_list_cdt_index")

    def test_neg_cdtindex_with_bin_is_none(self):
        """
        Invoke createindex() with bin is None
        """
        policy = {}
        try:
            self.as_connection.index_cdt_create(
                "test",
                "demo",
                None,
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_NUMERIC,
                "test_numeric_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Bin should be a string"

    def test_neg_cdtindex_with_index_is_none(self):
        """
        Invoke createindex() with index is None
        """
        policy = {}
        try:
            self.as_connection.index_cdt_create(
                "test",
                "demo",
                "string_list",
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_STRING,
                None,
                {"ctx": ctx_list_index},
                policy,
            )

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Index name should be string or unicode"

    def test_neg_cdtindex_with_incorrect_namespace(self):
        """
        Invoke createindex() with incorrect namespace
        """
        policy = {}

        try:
            self.as_connection.index_cdt_create(
                "test1",
                "demo",
                "numeric_list",
                aerospike.INDEX_TYPE_DEFAULT,
                aerospike.INDEX_NUMERIC,
                "test_numeric_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )

        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_neg_cdtindex_with_incorrect_set(self):
        """
        Invoke createindex() with incorrect set
        """
        policy = {}
        retobj = self.as_connection.index_cdt_create(
            "test",
            "demo1",
            "numeric_list",
            aerospike.INDEX_TYPE_LIST,
            aerospike.INDEX_NUMERIC,
            "test_numeric_list_cdt_index",
            {"ctx": ctx_list_index},
            policy,
        )

        assert retobj == 0
        self.as_connection.index_remove("test", "test_numeric_list_cdt_index", policy)
        ensure_dropped_index(self.as_connection, "test", "test_numeric_list_cdt_index")

    def test_neg_cdtindex_with_correct_parameters_no_connection(self):
        """
        Invoke index_cdt_create() with correct arguments no connection
        """
        policy = {}
        config = TestBaseClass.get_connection_config()
        client1 = aerospike.client(config)
        client1.close()

        try:
            client1.index_cdt_create(
                "test",
                "demo",
                "string_list",
                aerospike.INDEX_TYPE_LIST,
                aerospike.INDEX_STRING,
                "test_string_list_cdt_index",
                {"ctx": ctx_list_index},
                policy,
            )

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_cdtindex_with_no_paramters(self):
        """
        Invoke index_cdt_create() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_cdt_create()

        assert "argument 'ns' (pos 1)" in str(typeError.value)
