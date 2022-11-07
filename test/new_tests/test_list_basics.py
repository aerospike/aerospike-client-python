# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestListBasics(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):

        key = ('test', 'map_test', 1)
        rec = {'list': [1, 2, 3, 4, 5]}
        as_connection.put(key, rec)

        def teardown():
            """
            Teardown method.
            """
            key = ('test', 'map_test', 1)
            binname = 'my_map'
            self.as_connection.remove(key)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "method_name",
        (
            "list_append",
            "list_extend",
            "list_insert",
            "list_insert_items",
            "list_pop",
            "list_pop_range",
            "list_remove",
            "list_remove_range",
            "list_clear",
            "list_set",
            "list_get",
            "list_get_range",
            "list_trim",
            "list_size"
        )
    )
    def test_calling_list_methods_with_no_args(self, method_name):
        method = getattr(self.as_connection, method_name)
        with pytest.raises(TypeError):
            method()
