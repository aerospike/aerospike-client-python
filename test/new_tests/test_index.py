# -*- coding: utf-8 -*-

import pytest
from .as_status_codes import AerospikeStatus
from .index_helpers import ensure_dropped_index
from .test_base_class import TestBaseClass
from aerospike import exception as e

import aerospike
from aerospike_helpers.expressions.base import GeoBin


class TestIndex(object):
    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        for i in range(5):
            key = ("test", "demo", i)
            rec = {"name": "name%s" % (str(i)), "addr": "name%s" % (str(i)), "age": i, "no": i, "bytes": b'123'}
            as_connection.put(key, rec)

        def teardown():
            ensure_dropped_index(self.as_connection, "test", "age_index")
            ensure_dropped_index(self.as_connection, "test", "name_index")
            for i in range(5):
                key = ("test", "demo", i)
                # TODO: unneeded variable?
                rec = {"name": "name%s" % (str(i)), "addr": "name%s" % (str(i)), "age": i, "no": i}  # noqa: F841
                as_connection.remove(key)

        request.addfinalizer(teardown)

    def test_create_indexes_with_no_parameters(self):
        """
        Invoke without any
        mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.index_single_value_create()

        assert "argument 'ns' (pos 1)" in str(typeError.value)

    def test_create_integer_index_with_correct_parameters(self):
        """
        Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_integer_index_with_set_name_too_long(self):
        # Invoke createindex with a set name beyond the maximum
        set_name = "a" * 128

        policy = {}
        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_single_value_create("test", set_name, "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_integer_index_with_incorrect_namespace(self):
        """
        Invoke createindex() with non existent namespace
        """
        policy = {}
        with pytest.raises(e.NamespaceNotFound) as err_info:
            self.as_connection.index_single_value_create("fake_namespace", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_create_integer_index_with_incorrect_set(self):
        """
        Invoke createindex() with nonexistent set
        It should succeed
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo1", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_integer_index_with_incorrect_bin(self):
        """
        Invoke createindex() with a nonexistent bin
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "fake_bin", aerospike.INDEX_NUMERIC, "age_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_integer_index_with_namespace_is_none(self):
        """
        Invoke createindex() with namespace is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create(None, "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_creat_integer_eindex_with_set_is_none(self):
        # Invoke createindex() with set is None
        policy = {}

        retobj = self.as_connection.index_single_value_create("test", None, "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_integer_index_with_set_is_int(self):
        # Invoke createindex() with set is int
        policy = {}

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create("test", 1, "age", aerospike.INDEX_NUMERIC, "age_index", policy)
        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_integer_index_with_bin_is_none(self):
        """
        Invoke createindex() with bin is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create("test", "demo", None, aerospike.INDEX_NUMERIC, "age_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_integer_index_with_index_is_none(self):
        """
        Invoke createindex() with index_name is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, None, policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_same_integer_index_multiple_times(self):
        """
        Invoke createindex() with the same arguments
        multiple times on the same bin

        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_same_integer_index_multiple_times_different_bin(self):
        """
        Invoke createindex() with the same index name,
        multiple times on different bin names
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_single_value_create("test", "demo", "no", aerospike.INDEX_NUMERIC, "age_index", policy)
            self.as_connection.index_remove("test", "age_index", policy)

        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_different_integer_index_multiple_times_same_bin(self):
        """
                    Invoke createindex() with multiple times on same bin with different
        name
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK
        try:
            retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index1", policy)
            self.as_connection.index_remove("test", "age_index1", policy)
        except e.IndexFoundError:
            assert self.server_version <= [6, 0]

        ensure_dropped_index(self.as_connection, "test", "age_index")

    def test_create_integer_index_with_policy(self):
        """
        Invoke createindex() with policy
        """
        policy = {"timeout": 180000}
        retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        ensure_dropped_index(self.as_connection, "test", "age_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_blob_index(self):
        if self.server_version < [7, 0]:
            pytest.skip("Blob secondary indexes are only supported in server 7.0+")

        self.as_connection.index_single_value_create(ns="test", set="demo", bin="bytes", index_datatype=aerospike.INDEX_BLOB, name="bytes_index", policy={}, ctx=None)

        ensure_dropped_index(self.as_connection, "test", "bytes_index")

    def test_create_string_index_positive(self):
        """
        Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)

        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")

        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_set_length_too_long(self):
        # Invoke createindex() with correct arguments set length extra
        set_name = "a" * 100
        policy = {}

        with pytest.raises(e.InvalidRequest) as err_info:
            self.as_connection.index_single_value_create("test", set_name, "name", aerospike.INDEX_STRING, "name_index", policy)
        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_string_index_with_correct_parameters_ns_length_extra(self):
        # Invoke createindex() with correct arguments ns length extra
        ns_name = "a" * 50
        policy = {}

        with pytest.raises((e.InvalidRequest, e.NamespaceNotFound)) as err_info:
            self.as_connection.index_single_value_create(ns_name, "demo", "name", aerospike.INDEX_STRING, "name_index", policy)

        err_code = err_info.value.code
        if (TestBaseClass.major_ver, TestBaseClass.minor_ver) >= (7, 2):
            assert err_code is AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND
        else:
            assert err_code is AerospikeStatus.AEROSPIKE_ERR_REQUEST_INVALID

    def test_create_string_index_with_incorrect_namespace(self):
        """
        Invoke create string index() with incorrect namespace
        """
        policy = {}

        with pytest.raises(e.NamespaceNotFound) as err_info:
            self.as_connection.index_single_value_create("fake_namespace", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_NAMESPACE_NOT_FOUND

    def test_create_string_index_with_incorrect_set(self):
        """
        Invoke create string index() with incorrect set
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo1", "name", aerospike.INDEX_STRING, "name_index", policy)

        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_incorrect_bin(self):
        """
        Invoke create string index() with incorrect bin
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name1", aerospike.INDEX_STRING, "name_index", policy)

        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_namespace_is_none(self):
        """
        Invoke create string index() with namespace is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create(None, "demo", "name", aerospike.INDEX_STRING, "name_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_string_index_with_set_is_none(self):
        # Invoke create string index() with set is None
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", None, "name", aerospike.INDEX_STRING, "name_index", policy)

        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_string_index_with_bin_is_none(self):
        """
        Invoke create string index() with bin is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create("test", "demo", None, aerospike.INDEX_STRING, "name_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_string_index_with_index_is_none(self):
        """
        Invoke create_string_index() with index name is None
        """
        policy = {}
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, None, policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_create_same_string_index_multiple_times(self):
        """
        Invoke create string index() with multiple times on same bin
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK
        try:
            retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)
        except e.IndexFoundError:
            assert self.server_version <= [6, 0]

        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")

    def test_create_same_string_index_multiple_times_different_bin(self):
        """
        Invoke create string index() with multiple times on different bin
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK

        with pytest.raises(e.IndexFoundError):
            retobj = self.as_connection.index_single_value_create("test", "demo", "addr", aerospike.INDEX_STRING, "name_index", policy)
            self.as_connection.index_remove("test", "name_index", policy)
            ensure_dropped_index(self.as_connection, "test", "name_index")

        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")

        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_create_different_string_index_multiple_times_same_bin(self):
        """
        Invoke create string index() with multiple times on same
        bin with different name
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)
        assert retobj == AerospikeStatus.AEROSPIKE_OK
        try:
            retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index1", policy)
            self.as_connection.index_remove("test", "name_index1", policy)
        except e.IndexFoundError:
            assert self.server_version <= [6, 0]

        ensure_dropped_index(self.as_connection, "test", "name_index")

    def test_create_string_index_with_policy(self):
        """
        Invoke create string index() with policy
        """
        policy = {"timeout": 180000}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "name_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", "name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "name_index")

    def test_drop_invalid_index(self):
        """
        Invoke drop invalid index()
        """
        policy = {}
        try:
            self.as_connection.index_remove("test", "notarealindex", policy)
        except e.IndexNotFound:
            assert self.server_version <= [6, 0]

    def test_drop_valid_index(self):
        """
        Invoke drop valid index()
        """
        policy = {}
        self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)
        retobj = self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_drop_valid_index_policy(self):
        """
        Invoke drop valid index() policy
        """
        policy = {"timeout": 180000}
        self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)
        retobj = self.as_connection.index_remove("test", "age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "age_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_createindex_with_long_index_name(self):
        # Invoke createindex() with long index name
        policy = {}
        with pytest.raises(e.InvalidRequest):
            self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "index" * 100, policy)

    def test_create_string_index_unicode_positive(self):
        """
        Invoke create string index() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "name", aerospike.INDEX_STRING, "uni_name_index", policy)

        self.as_connection.index_remove("test", "uni_name_index", policy)
        ensure_dropped_index(self.as_connection, "test", "uni_name_index")
        assert retobj == AerospikeStatus.AEROSPIKE_OK

    def test_createindex_integer_unicode(self):
        """
        Invoke createindex() with correct arguments
        """
        policy = {}
        retobj = self.as_connection.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "uni_age_index", policy)

        assert retobj == AerospikeStatus.AEROSPIKE_OK
        self.as_connection.index_remove("test", "uni_age_index", policy)
        ensure_dropped_index(self.as_connection, "test", "uni_age_index")

    def test_createindex_with_correct_parameters_without_connection(self):
        # Invoke createindex() with correct arguments without connection
        policy = {}
        config = TestBaseClass.get_connection_config()
        client1 = aerospike.client(config)
        client1.close()

        with pytest.raises(e.ClusterError) as err_info:
            client1.index_single_value_create("test", "demo", "age", aerospike.INDEX_NUMERIC, "age_index", policy)

        err_code = err_info.value.code
        assert err_code is AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_index_remove_no_args(self):

        with pytest.raises(TypeError):
            self.as_connection.index_remove()

    def test_index_remove_no_index(self):

        with pytest.raises(TypeError):
            self.as_connection.index_remove("test")

    def test_index_remove_extra_args(self):
        # pass 'ns', 'idx_name', 'policy', and an extra argument
        with pytest.raises(TypeError):
            self.as_connection.index_remove("test", "demo", {}, "index_name")

    @pytest.mark.parametrize(
        "ns, idx_name, policy",
        (("test", "idx", "policy"), ("test", 5, {}), (5, "idx", {}), ("test", None, {}), (None, "idx", {})),
    )
    def test_index_remove_wrong_arg_types(self, ns, idx_name, policy):
        with pytest.raises(e.ParamError):
            self.as_connection.index_remove(ns, idx_name, policy)

    def test_index_expr_create_wrong_args(self):
        with pytest.raises(TypeError):
            # Missing a required argument
            self.as_connection.index_expr_create(
                ns="test",
                set="demo",
                index_type=aerospike.INDEX_TYPE_DEFAULT,
                index_datatype=aerospike.INDEX_BLOB,
                expressions=GeoBin("geo_point").compile()
            )

    def test_index_expr_create_invalid_expr(self):
        with pytest.raises(e.ParamError):
            self.as_connection.index_expr_create(
                ns="test",
                set="demo",
                index_type=aerospike.INDEX_TYPE_DEFAULT,
                index_datatype=aerospike.INDEX_BLOB,
                # Common mistake: uncompiled expression
                name="test",
                expressions=GeoBin("geo_point")
            )
