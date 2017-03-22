# -*- coding: utf-8 -*-
import pytest
import sys
import pdb
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestOperate(object):

    def setup_class(cls):
        """
        Setup class.
        """
        hostlist, user, password = TestBaseClass.get_hosts()

        config_no_typechecks = {'hosts': hostlist, 'strict_types': False}
        if user is None and password is None:
            TestOperate.client_no_typechecks = aerospike.client(
                config_no_typechecks).connect()
        else:
            TestOperate.client_no_typechecks = aerospike.client(
                config_no_typechecks).connect(user, password)

    def teardown_class(cls):
        TestOperate.client_no_typechecks.close()

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        #pdb.set_trace()

        def teardown():
            """
            Teardown Method
            """
        request.addfinalizer(teardown)

    @pytest.mark.parametrize("key, llist, expected", [
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 97},
             {"op": aerospike.OP_MAP_INCREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            {'my_map': 98}),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 22},
             {"op": aerospike.OP_MAP_DECREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            {'my_map': 21}),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT_ITEMS,
              "bin": "my_map",
              "val": {'name': 'bubba', 'occupation': 'dancer'}},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "name",
              "return_type": aerospike.MAP_RETURN_KEY_VALUE}],
            {'my_map': [('name', 'bubba')]})
    ])
    def test_pos_operate_with_correct_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """
        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == expected
        self.as_connection.remove(key)

    @pytest.mark.parametrize("key, llist, expected", [
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 97},
             {"op": aerospike.OP_MAP_INCREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            [None, None, ('my_map', 98)]),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT,
              "bin": "my_map",
              "key": "age",
              "val": 22},
             {"op": aerospike.OP_MAP_DECREMENT,
              "bin": "my_map",
              "key": "age",
              "val": 1},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "age",
              "return_type": aerospike.MAP_RETURN_VALUE}],
            [None, None, ('my_map', 21)]),
        (('test', 'map_test', 1),
            [{"op": aerospike.OP_MAP_PUT_ITEMS,
              "bin": "my_map",
              "val": {'name': 'bubba', 'occupation': 'dancer'}},
             {"op": aerospike.OP_MAP_GET_BY_KEY,
              "bin": "my_map",
              "key": "name",
              "return_type": aerospike.MAP_RETURN_KEY_VALUE}],
            [None, ('my_map', [('name', 'bubba')])])
    ])
    def test_pos_operate_ordered_with_correct_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """
        key, _, bins = self.as_connection.operate_ordered(key, llist)

        assert bins == expected
        self.as_connection.remove(key)

    def test_pos_operate_set_map_policy(self):
        key = ('test', 'map_test', 1)
        llist = [{"op": aerospike.OP_MAP_SET_POLICY,
                  "bin": "my_map",
                  "map_policy": {'map_sort': aerospike.MAP_KEY_ORDERED}}]
        key, _, _ = self.as_connection.operate(key, llist)
        self.as_connection.remove(key)
        pass

    def test_pos_map_clear(self):
        key = ('test', 'map_test', 1)
        binname = 'my_map'
        llist = [{"op": aerospike.OP_MAP_PUT,
                  "bin": binname,
                  "key": "age",
                  "val": 97}]
        key, _, _ = self.as_connection.operate(key, llist)

        llist = [{"op": aerospike.OP_MAP_SIZE,
                  "bin": binname}]
        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == {binname: 1}

        key, _, _ = self.as_connection.operate(key, llist)
        llist = [{"op": aerospike.OP_MAP_CLEAR,
                  "bin": binname}]
        key, _, _ = self.as_connection.operate(key, llist)

        llist = [{"op": aerospike.OP_MAP_SIZE,
                  "bin": binname}]
        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == {binname: 0}
        self.as_connection.remove(key)
