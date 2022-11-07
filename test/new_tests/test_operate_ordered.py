# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestOperateOrdered(object):

    def setup_class(cls):
        """
        Setup class.
        """
        cls.client_no_typechecks = TestBaseClass.get_new_connection(
          {'strict_types': False})

    def teardown_class(cls):
        TestOperateOrdered.client_no_typechecks.close()

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method
        """
        keys = []

        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            as_connection.put(key, rec)

        key = ('test', 'demo', 6)
        rec = {"age": 6.3}
        as_connection.put(key, rec)
        keys.append(key)

        key = ('test', 'demo', 'bytearray_key')
        rec = {"bytearray_bin": bytearray("asd;as[d'as;d", "utf-8")}
        as_connection.put(key, rec)

        keys.append(key)

        key = ('test', 'demo', 'list_key')
        rec = {"int_bin": [1, 2, 3, 4], "string_bin": ['a', 'b', 'c', 'd']}
        as_connection.put(key, rec)

        keys.append(key)

        key = ('test', 'demo', 'existing_key')
        rec = {"dict": {"a": 1}, "bytearray": bytearray("abc", "utf-8"),
               "float": 3.4, "list": ['a']}
        as_connection.put(key, rec)

        keys.append(key)

        def teardown():
            """
            Teardown Method
            """
            for i in range(5):
                key = ('test', 'demo', i)
                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    pass
            for key in keys:
                try:
                    as_connection.remove(key)
                except e.RecordNotFound:
                    pass

        request.addfinalizer(teardown)

    @pytest.mark.parametrize("key, llist, expected", [
        (('test', 'demo', 1),
            [{"op": aerospike.OPERATOR_PREPEND,
              "bin": "name",
              "val": u"ram"},
             {"op": aerospike.OPERATOR_INCR,
              "bin": "age",
              "val": 3},
             {"op": aerospike.OPERATOR_READ,
              "bin": "name"}],
            [('name', 'ramname1')]),
        (('test', 'demo', 1),                 # with_write_float_value
            [{"op": aerospike.OPERATOR_WRITE,
              "bin": "write_bin",
              "val": {"no": 89.8}},
             {"op": aerospike.OPERATOR_READ,
              "bin": "write_bin"}],
            [('write_bin', {u'no': 89.8})]),
        (('test', 'demo', 1),                            # write positive
            [{"op": aerospike.OPERATOR_WRITE,
              "bin": "write_bin",
              "val": {"no": 89}},
             {"op": aerospike.OPERATOR_READ, "bin": "write_bin"}],
            [('write_bin', {u'no': 89})]),
        (('test', 'demo', 1),                       # write_tuple_positive
            [{"op": aerospike.OPERATOR_WRITE,
              "bin": "write_bin",
              "val": tuple('abc')},
             {"op": aerospike.OPERATOR_READ, "bin": "write_bin"}],
            [('write_bin', ('a', 'b', 'c'))]),
        (('test', 'demo', 1),                # with_bin_bytearray
            [{"op": aerospike.OPERATOR_PREPEND,
              "bin": bytearray("asd[;asjk", "utf-8"),
              "val": u"ram"},
             {"op": aerospike.OPERATOR_READ,
              "bin": bytearray("asd[;asjk", "utf-8")}],
            [('asd[;asjk', 'ram')]),
        (('test', 'demo', 'bytearray_key'),  # append_val bytearray
            [{"op": aerospike.OPERATOR_APPEND,
              "bin": "bytearray_bin",
              "val": bytearray("abc", "utf-8")},
             {"op": aerospike.OPERATOR_READ,
              "bin": "bytearray_bin"}],
            [('bytearray_bin', bytearray("asd;as[d'as;dabc", "utf-8"))]),
        (('test', 'demo', 'bytearray_new'),  # append bytearray_newrecord
            [{"op": aerospike.OPERATOR_APPEND,
              "bin": "bytearray_bin",
              "val": bytearray("asd;as[d'as;d", "utf-8")},
             {"op": aerospike.OPERATOR_READ,
              "bin": "bytearray_bin"}],
            [('bytearray_bin', bytearray("asd;as[d'as;d", "utf-8"))]),
        (('test', 'demo', 'bytearray_key'),  # prepend_valbytearray
            [{"op": aerospike.OPERATOR_PREPEND,
              "bin": "bytearray_bin",
              "val": bytearray("abc", "utf-8")},
             {"op": aerospike.OPERATOR_READ,
              "bin": "bytearray_bin"}],
            [('bytearray_bin', bytearray("abcasd;as[d'as;d", "utf-8"))]),
        (('test', 'demo', 'bytearray_new'),  # prepend_valbytearray_newrecord
            [{"op": aerospike.OPERATOR_PREPEND,
              "bin": "bytearray_bin",
              "val": bytearray("asd;as[d'as;d", "utf-8")},
             {"op": aerospike.OPERATOR_READ,
              "bin": "bytearray_bin"}],
            [('bytearray_bin', bytearray("asd;as[d'as;d", "utf-8"))]),
    ])
    def test_pos_operate_ordered_correct_paramters(self, key, llist, expected):
        """
        Invoke operate_ordered() with correct parameters
        """

        _, _, bins = self.as_connection.operate_ordered(key, llist)
        self.as_connection.remove(key)
        assert bins == expected

    def test_pos_operate_ordered_with_correct_policy(self):
        """
        Invoke operate_ordered() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_READ,
                  "bin": "name"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}]

        _, _, bins = self.as_connection.operate_ordered(
          key, llist, {}, policy)
        self.as_connection.remove(key)
        assert bins == [('name', 'name1aa')]

    def test_pos_operate_ordered_with_policy_key_digest(self):
        """
        Invoke operate_ordered() with policy_key_digest
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'name%s' % (str(1)), 'age': 1}
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_DIGEST}
        self.as_connection.put(key, rec)

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_READ,
                  "bin": "name"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3},
                 {"op": aerospike.OPERATOR_READ,
                  "bin": "age"}]

        _, _, bins = self.as_connection.operate_ordered(
          key, llist, {}, policy)

        assert bins == [('name', 'name1aa'), ('age', 4)]
        self.as_connection.remove(key)

    @pytest.mark.parametrize("key, policy, meta, llist", [
        (('test', 'demo', 1),
         {'timeout': 1000,
          'key': aerospike.POLICY_KEY_SEND,
          'gen': aerospike.POLICY_GEN_IGNORE,
          'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL},
         {'gen': 10, 'ttl': 1200},
         [{"op": aerospike.OPERATOR_APPEND,
           "bin": "name",
           "val": "aa"},
          {"op": aerospike.OPERATOR_INCR,
           "bin": "age",
           "val": 3},
          {"op": aerospike.OPERATOR_READ,
           "bin": "name"}]),
    ])
    def test_pos_operate_ordered_policy_gen_ignore(self, key, policy, meta,
                                                   llist):
        """
        Invoke operate_ordered() with gen ignore.
        """
        key, meta, bins = self.as_connection.operate_ordered(key, llist,
                                                             meta, policy)

        assert bins == [('name', 'name1aa')]

    def test_pos_operate_ordered_with_policy_gen_GT(self):
        """
        Invoke operate_ordered() with gen GT positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1200}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3},
                 {"op": aerospike.OPERATOR_READ,
                  "bin": "name"}]

        (key, meta, bins) = self.as_connection.operate_ordered(
            key, llist, meta, policy)

        assert bins == [('name', 'name1aa')]

    def test_pos_operate_ordered_with_nonexistent_key(self):
        """
        Invoke operate_ordered() with non-existent key
        """
        key1 = ('test', 'demo', "key11")
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "loc",
             "val": "mumbai"},
            {"op": aerospike.OPERATOR_READ,
             "bin": "loc"}
        ]
        _, _, bins = self.as_connection.operate_ordered(key1, llist)
        self.as_connection.remove(key1)
        assert bins == [('loc', 'mumbai')]

    def test_pos_operate_ordered_increment_nonexistent_key(self):
        """
        Invoke operate_ordered() with increment with nonexistent_key
        """
        key = ('test', 'demo', "non_existentkey")
        llist = [{"op": aerospike.OPERATOR_INCR, "bin": "age", "val": 5}]

        _, _, op_bins = self.as_connection.operate_ordered(key, llist)
        _, _, get_bins = self.as_connection.get(key)
        self.as_connection.remove(key)

        assert op_bins == []
        assert get_bins == {"age": 5}

    def test_pos_operate_ordered_increment_nonexistent_bin(self):
        """
        Invoke operate_ordered() with increment with nonexistent_bin
        """
        key = ('test', 'demo', 1)
        llist = [{"op": aerospike.OPERATOR_INCR, "bin": "my_age", "val": 5}]

        _, _, op_bins = self.as_connection.operate_ordered(key, llist)
        _, _, get_bins = self.as_connection.get(key)
        assert op_bins == []
        assert get_bins == {"my_age": 5, "age": 1, "name": "name1"}

    def test_pos_operate_ordered_write_set_to_aerospike_null(self):
        """
        Invoke operate_ordered() with write command bin set to aerospike_null
        """
        key = ('test', 'demo', 'null_record')

        bins = {"name": "John", "no": 3}

        assert 0 == self.as_connection.put(key, bins)

        llist = [
            {
                "op": aerospike.OPERATOR_WRITE,
                "bin": "no",
                "val": aerospike.null()
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "no"
            }
        ]

        _, _, bins = self.as_connection.operate_ordered(key, llist)
        self.as_connection.remove(key)
        # we read a non existent bin, so nothing is returned
        assert [] == bins

    # List operation testcases
    @pytest.mark.parametrize("list, expected", [
        ([
            {"op": aerospike.OP_LIST_APPEND,
             "bin": "int_bin",
             "val": 7},
            {"op": aerospike.OP_LIST_GET,
             "bin": "int_bin",
             "index": 4},
        ], [('int_bin', 5), ('int_bin', 7)]),
        ([
            {"op": aerospike.OP_LIST_APPEND_ITEMS,
             "bin": "int_bin",
             "val": [7, 9]},
            {"op": aerospike.OP_LIST_GET_RANGE,
             "bin": "int_bin",
             "index": 3,
             "val": 3},
        ], [('int_bin', 6), ('int_bin', [4, 7, 9])]),
        ([
            {"op": aerospike.OP_LIST_INSERT,
             "bin": "int_bin",
             "val": 7,
             "index": 2},
            {"op": aerospike.OP_LIST_POP,
             "bin": "int_bin",
             "index": 2}
        ], [('int_bin', 5), ('int_bin', 7)]),
        ([
            {"op": aerospike.OP_LIST_INSERT_ITEMS,
             "bin": "int_bin",
             "val": [7, 9],
             "index": 2},
            {"op": aerospike.OP_LIST_POP_RANGE,
             "bin": "int_bin",
             "index": 2,
             "val": 2}
        ], [('int_bin', 6), ('int_bin', [7, 9])]),
        ([
            {"op": aerospike.OP_LIST_SET,
             "bin": "int_bin",
             "index": 2,
             "val": 18},
            {"op": aerospike.OP_LIST_GET,
             "bin": "int_bin",
             "index": 2}
        ], [('int_bin', 18)]),
        ([
            {"op": aerospike.OP_LIST_SET,
             "bin": "int_bin",
             "index": 6,
             "val": 10},
            {"op": aerospike.OP_LIST_GET,
             "bin": "int_bin",
             "index": 6}
        ], [('int_bin', 10)])
    ])
    def test_pos_operate_ordered_with_list_addition_operations(self, list,
                                                               expected):
        """
        Invoke operate_ordered() with list addition operations
        """
        key = ('test', 'demo', 'list_key')

        key, _, bins = self.as_connection.operate_ordered(key, list)

        assert bins == expected

    @pytest.mark.parametrize("list, expected", [
        ([
            {"op": aerospike.OP_LIST_REMOVE,
             "bin": "int_bin",
             "index": 2},
        ], [("int_bin", 1)]),
        ([
            {"op": aerospike.OP_LIST_REMOVE_RANGE,
             "bin": "int_bin",
             "index": 2,
             "val": 2},
        ], [("int_bin", 2)]),
        ([
            {"op": aerospike.OP_LIST_TRIM,
             "bin": "int_bin",
             "index": 2,
             "val": 2},
        ], [("int_bin", 2)]),
        ([
            {"op": aerospike.OP_LIST_CLEAR,
             "bin": "int_bin"}
        ], [])
    ])
    def test_pos_operate_ordered_with_list_remove_operations(self, list,
                                                             expected):
        """
        Invoke operate_ordered() with list remove operations
        """
        key = ('test', 'demo', 'list_key')

        _, _, bins = self.as_connection.operate_ordered(key, list)

        assert bins == expected

    def test_pos_operate_ordered_with_list_size(self):
        """
        Invoke operate_ordered() with list_size operation
        """
        key = ('test', 'demo', 'list_key')
        list = [
            {"op": aerospike.OP_LIST_SIZE,
             "bin": "int_bin"}
        ]

        key, _, bins = self.as_connection.operate_ordered(key, list)

        assert bins == [('int_bin', 4)]

    def test_pos_operate_ordered_with_list_get_range_val_out_of_bounds(self):
        """
        Invoke operate_ordered() list_get_range op and value out of bounds
        """
        key = ('test', 'demo', 'list_key')
        list = [{"op": aerospike.OP_LIST_GET_RANGE,
                 "bin": "int_bin",
                 "index": 2,
                 "val": 9}]

        (key, meta, bins) = self.as_connection.operate_ordered(key, list)

        assert bins == [('int_bin', [3, 4])]

    def test_pos_operate_ordered_with_list_trim_val_with_negative_value(self):
        """
        Invoke operate_ordered() with list_trimoperation and value is negative
        """
        key = ('test', 'demo', 'list_key')
        list = [{"op": aerospike.OP_LIST_TRIM,
                 "bin": "int_bin",
                 "index": 1,
                 "val": -9}]

        (key, meta, bins) = self.as_connection.operate_ordered(key, list)

        assert bins == [('int_bin', 1)]

        (key, meta, bins) = self.as_connection.get(key)

        assert bins['int_bin'] == [2, 3, 4]

    def test_pos_operate_ordered_with_list_insert_index_negative(self):
        """
        Invoke operate_ordered()-list_insert and item index is a negative value
        """
        key = ('test', 'demo', 'list_key')
        list = [{"op": aerospike.OP_LIST_INSERT,
                 "bin": "int_bin",
                 "index": -2,
                 "val": 9}]

        (key, meta, bins) = self.as_connection.operate_ordered(key, list)

        assert bins == [('int_bin', 5)]

        (key, meta, bins) = self.as_connection.get(key)

        assert bins['int_bin'] == [1, 2, 9, 3, 4]

    @pytest.mark.parametrize("list, expected", [
        ([
            {"op": aerospike.OP_LIST_APPEND,
             "bin": "string_bin",
             "val": {"new_val": 1}},
            {"op": aerospike.OP_LIST_GET,
             "bin": "string_bin",
             "index": 4}
         ], [('string_bin', 5), ('string_bin', {'new_val': 1})]),
        ([
            {"op": aerospike.OP_LIST_APPEND_ITEMS,
             "bin": "string_bin",
             "val": [['z', 'x'], ('y', 'w')]},
            {"op": aerospike.OP_LIST_GET_RANGE,
             "bin": "string_bin",
             "index": 3,
             "val": 3}
        ], [('string_bin', 6), ('string_bin', ['d', ['z', 'x'], ('y', 'w')])]),
        ([
            {"op": aerospike.OP_LIST_INSERT,
             "bin": "string_bin",
             "val": True,
             "index": 2},
            {"op": aerospike.OP_LIST_POP,
             "bin": "string_bin",
             "index": 2}
        ], [('string_bin', 5), ('string_bin', True)]),
        ([
            {"op": aerospike.OP_LIST_INSERT_ITEMS,
             "bin": "string_bin",
             "val": [bytearray("abc", "utf-8"), u"xyz"],
             "index": 2},
            {"op": aerospike.OP_LIST_POP_RANGE,
             "bin": "string_bin",
             "index": 2,
             "val": 2}
        ], [('string_bin', 6), ('string_bin', [bytearray(b'abc'), 'xyz'])]),
    ])
    def test_pos_operate_ordered_with_list_ops_different_datatypes(self, list,
                                                                   expected):
        """
        Invoke operate_ordered() with list operations using different datatypes
        """
        key = ('test', 'demo', 'list_key')

        key, _, bins = self.as_connection.operate_ordered(key, list)

        assert bins == expected

    # No typecheck test cases
    @pytest.mark.parametrize("key, llist, expected", [
        (('test', 'demo', 'prepend_int'),                   # prepend_with_int
            [{
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "age",
                "val": 4},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "age"
            }],
            [('age', 4)]),
        (('test', 'demo', 'append_dict'),                  # append_with_dict
            [{
                "op": aerospike.OPERATOR_APPEND,
                "bin": "dict",
                "val": {"a": 1, "b": 2}},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "dict"
            }],
            [('dict', {"a": 1, "b": 2})]),
        (('test', 'demo', 'incr_string'),               # incr_with_string
            [{
                "op": aerospike.OPERATOR_INCR,
                "bin": "name",
                "val": "aerospike"},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "name"
            }],
            [('name', 'aerospike')]),
    ])
    def test_pos_operate_ordered_new_record(self, key, llist, expected):
        """
        Invoke operate_ordered() with prepend command on a new record
        """
        try:
            self.as_connection.remove(key)
        except:
            pass
        _, _, bins = TestOperateOrdered.client_no_typechecks.operate_ordered(key, llist)
        assert bins == expected

        try:
            self.as_connection.remove(key)
        except:
            pass

    def test_pos_operate_ordered_with_bin_length_extra_nostricttypes(self):
        """
        Invoke operate_ordered() with bin length extra. Strict types disabled
        """
        key = ('test', 'demo', 1)

        max_length = 'a' * 21

        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": max_length,
             "val": 3}
        ]

        _, _, bins = TestOperateOrdered.\
            client_no_typechecks.operate_ordered(key, llist)

        assert bins == []

        (key, _, bins) = TestOperateOrdered.client_no_typechecks.get(key)

        assert bins == {"name": "ramname1", "age": 1}

    def test_pos_operate_ordered_with_command_invalid_nostricttypes(self):
        """
        Invoke operate_ordered() with an invalid command. Strict types disabled
        """
        key = ('test', 'demo', 1)

        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": 3,
             "bin": "age",
             "val": 3},
            {"op": aerospike.OPERATOR_READ,
             "bin": "name"}
        ]

        _, _, bins = TestOperateOrdered.\
            client_no_typechecks.operate_ordered(key, llist)

        assert bins == [('name', 'ramname1')]

    # Negative tests
    def test_neg_operate_ordered_with_no_parameters(self):
        """
        Invoke opearte_ordered() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.operate_ordered()
        assert "argument 'key' (pos 1)" in str(
            typeError.value)

    def test_neg_operate_ordered_with_policy_gen_EQ_not_equal(self):
        """
        Invoke operate_ordered() with gen not equal.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {
            'gen': gen + 5,
            'ttl': 1200
        }
        llist = [
            {
                "op": aerospike.OPERATOR_APPEND,
                "bin": "name",
                "val": "aa"
            },
            {
                "op": aerospike.OPERATOR_INCR,
                "bin": "age",
                "val": 3
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "name"
            }
        ]
        try:
            key, meta, _ = self.as_connection.operate_ordered(key, llist,
                                                              meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3

        (key, meta, bins) = self.as_connection.get(key)
        assert bins == {"age": 1, 'name': 'name1'}
        assert key == ('test', 'demo', None,
                       bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_neg_operate_ordered_with_policy_gen_GT_lesser(self):
        """
        Invoke operate_ordered() with gen GT lesser.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3},
                 {"op": aerospike.OPERATOR_READ,
                  "bin": "name"}]

        try:
            (key, meta, _) = self.as_connection.operate_ordered(
                key, llist, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3

        (key, meta, bins) = self.as_connection.get(key)
        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None,
                       bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_neg_operate_ordered_without_connection(self):
        """
        Invoke operate_ordered() with correct parameters without connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3},
            {"op": aerospike.OPERATOR_READ,
             "bin": "name"}
        ]

        try:
            key, _, _ = client1.operate_ordered(key, llist)

        except e.ClusterError as exception:
            assert exception.code == 11

    def test_neg_operate_ordered_prepend_set_to_aerospike_null(self):
        """
        Invoke operate_ordered() with prepend command bin set to aerospike_null
        """
        key = ('test', 'demo', 'null_record')

        bins = {"name": "John", "no": 3}

        assert 0 == self.as_connection.put(key, bins)

        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "no",
                "val": aerospike.null()
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "no"
            }
        ]

        try:
            (key, _, bins) = self.as_connection.operate_ordered(key, llist)

        except e.InvalidRequest as exception:
            assert exception.code == 4
        self.as_connection.remove(key)

    def test_neg_operate_ordered_with_command_invalid(self):
        """
        Invoke operate_ordered() with an invalid command. Strict types enabled
        """
        key = ('test', 'demo', 1)

        llist = [
            {"op": 3,
             "bin": "age",
             "val": 3},
            {"op": aerospike.OPERATOR_READ,
             "bin": "name"}
        ]

        try:
            key, _, _ = self.as_connection.operate_ordered(key, llist)

        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_operate_ordered_with_bin_length_extra(self):
        """
        Invoke operate_ordered() with bin length extra. Strict types enabled
        """
        key = ('test', 'demo', 1)

        max_length = 'a'
        for _ in range(20):
            max_length = max_length + 'a'

        llist = [
            {"op": aerospike.OPERATOR_INCR,
             "bin": max_length,
             "val": 3},
            {"op": aerospike.OPERATOR_READ,
             "bin": "name"}
        ]

        try:
            key, _, _ = self.as_connection.operate_ordered(key, llist)

        except e.BinNameError as exception:
            assert exception.code == 21

    def test_neg_operate_ordered_empty_string_key(self):
        """
        Invoke operate_ordered() with empty string key
        """
        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "name",
                "val": "ram"
            }
        ]
        try:
            self.as_connection.operate_ordered("", llist)

        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_operate_ordered_with_extra_parameter(self):
        """
        Invoke operate_ordered() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"}
        ]
        with pytest.raises(TypeError) as typeError:
            self.as_connection.operate_ordered(key, llist, {}, policy, "")

        assert "operate_ordered() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_neg_operate_ordered_policy_is_string(self):
        """
        Invoke operate_ordered() with policy is string
        """
        key = ('test', 'demo', 1)
        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "name",
                "val": "ram"
            }
        ]
        try:
            self.as_connection.operate_ordered(key, llist, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_operate_ordered_key_is_none(self):
        """
        Invoke operate_ordered() with key is none
        """
        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "name",
                "val": "ram"
            }
        ]
        try:
            self.as_connection.operate_ordered(None, llist)

        except e.ParamError as exception:
            assert exception.code == -2

    @pytest.mark.parametrize("key, policy, list, ex_code", [
        (('test', 'demo', 1),
         {'timeout': 1000},
         [{"op": aerospike.OPERATOR_APPEND,
           "bin": "name"},
          {"op": aerospike.OPERATOR_INCR,
           "bin": "age",
           "val": 3}],
         -2),
        (('test', 'demo', 1),
         {'timeout': 1000},
         [{"op": aerospike.OPERATOR_APPEND,
           "bin": "name",
           "val": 3,
           "aa": 89}, ],
         -2),
        (('test', 'demo', 1),                  # with_incr_value_string
         {'timeout': 1000,
          'key': aerospike.POLICY_KEY_SEND,
          'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER},
         [{"op": aerospike.OPERATOR_INCR,
           "bin": "age",
           "val": "3"},
          {"op": aerospike.OPERATOR_READ,
           "bin": "age"}],
         -2),
    ])
    def test_neg_operate_ordered_append_without_value_parameter(self, key,
                                                                policy, list,
                                                                ex_code):
        """
        Invoke operate_ordered() with append op and append val is not given
        """

        try:
            self.as_connection.operate_ordered(key, list, {}, policy)

        except e.ParamError as exception:
            assert exception.code == ex_code

    def test_neg_operate_ordered_append_value_integer(self):
        """
        Invoke operate_ordered() with append value is of type integer
        """
        key = ('test', 'demo', 1)
        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": 12},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3},
                 {"op": aerospike.OPERATOR_READ,
                  "bin": "name"}]

        try:
            self.as_connection.operate_ordered(key, llist)
        except e.ParamError as exc:
            assert exc.code == -2

    def test_neg_operate_ordered_with_incorrect_policy(self):
        """
        Invoke operate_ordered() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 0.5}
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3},
            {"op": aerospike.OPERATOR_READ,
             "bin": "name"}
        ]

        try:
            self.as_connection.operate_ordered(key, llist, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_operate_ordered_list_operation_bin_notlist(self):
        """
        Invoke operate_ordered() with a list op and bin does not contain list
        """
        key = ('test', 'demo', 1)
        list = [{"op": aerospike.OP_LIST_INSERT,
                 "bin": "age",
                 "index": 2,
                 "val": 9}]

        try:
            (key, _, _) = self.as_connection.operate_ordered(key, list)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

    def test_neg_operate_ordered_append_items_not_a_list(self):
        """
        Invoke operate_ordered() with list addition operations negative
        """
        key = ('test', 'demo', 'list_key')

        list = [
            {"op": aerospike.OP_LIST_APPEND_ITEMS,
             "bin": "int_bin",
             "val": 7},
        ]

        try:
            key, _, bins = self.as_connection.operate_ordered(key, list)

        except e.ParamError as exception:
            assert exception.code == -2

    @pytest.mark.parametrize("key, llist", [
        (('test', 'demo', 1),
            [{                                          # int
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "age",
                "val": 4},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "age"
            }]),
        (('test', 'demo', 'existing_key'),              # Existing list
            [{
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "list",
                "val": ['c']},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "list"
            }]),
        (('test', 'demo', 'existing_key'),              # Existing dict
            [{
                "op": aerospike.OPERATOR_APPEND,
                "bin": "dict",
                "val": {"c": 2}},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "dict"
            }]),
        (('test', 'demo', 'existing_key'),              # Exiting float
            [{
                "op": aerospike.OPERATOR_APPEND,
                "bin": "float",
                "val": 3.4},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "float"
            }]),
        (('test', 'demo', 1),                           # Existing string
            [{
                "op": aerospike.OPERATOR_INCR,
                "bin": "name",
                "val": "aerospike"},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "name"
            }]),
        (('test', 'demo', 'existing_key'),              # Existing Bytearray
            [{
                "op": aerospike.OPERATOR_INCR,
                "bin": "bytearray",
                "val": bytearray("abc", "utf-8")},
             {
                "op": aerospike.OPERATOR_READ,
                "bin": "bytearray"
            }]),
    ])
    def test_neg_operate_ordered_notypecheck_existing_record(self, key, llist):
        """
        Invoke operate() with no typecheck on existing record
        """
        try:
            (key, _, _) = TestOperateOrdered.\
                client_no_typechecks.operate_ordered(key, llist)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

        TestOperateOrdered.client_no_typechecks.remove(key)
