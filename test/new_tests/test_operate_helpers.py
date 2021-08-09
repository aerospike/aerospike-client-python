# -*- coding: utf-8 -*-
import pytest
import sys
from .test_base_class import TestBaseClass
from aerospike_helpers.operations import operations, list_operations, map_operations
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
    from aerospike import exception as e
except:
    print("Please install aerospike python client.")
    sys.exit(1)

# OPERATIONS
# aerospike.OPERATOR_WRITE
# aerospike.OPERATOR_APPEND
# aerospike.OPERATOR_PREPEND
# aerospike.OPERATOR_INCR
# aerospike.OPERATOR_READ
# aerospike.OPERATOR_TOUCH
# aerospike.OP_LIST_APPEND
# aerospike.OP_LIST_APPEND_ITEMS
# aerospike.OP_LIST_INSERT
# aerospike.OP_LIST_INSERT_ITEMS
# aerospike.OP_LIST_POP
# aerospike.OP_LIST_POP_RANGE
# aerospike.OP_LIST_REMOVE
# aerospike.OP_LIST_REMOVE_RANGE
# aerospike.OP_LIST_CLEAR
# aerospike.OP_LIST_SET
# aerospike.OP_LIST_GET
# aerospike.OP_LIST_GET_RANGE
# aerospike.OP_LIST_TRIM
# aerospike.OP_LIST_SIZE
# aerospike.OP_MAP_SET_POLICY
# aerospike.OP_MAP_PUT
# aerospike.OP_MAP_PUT_ITEMS
# aerospike.OP_MAP_INCREMENT
# aerospike.OP_MAP_DECREMENT
# aerospike.OP_MAP_SIZE
# aerospike.OP_MAP_CLEAR
# aerospike.OP_MAP_REMOVE_BY_KEY
# aerospike.OP_MAP_REMOVE_BY_KEY_LIST
# aerospike.OP_MAP_REMOVE_BY_KEY_RANGE
# aerospike.OP_MAP_REMOVE_BY_VALUE
# aerospike.OP_MAP_REMOVE_BY_VALUE_LIST
# aerospike.OP_MAP_REMOVE_BY_VALUE_RANGE
# aerospike.OP_MAP_REMOVE_BY_INDEX
# aerospike.OP_MAP_REMOVE_BY_INDEX_RANGE
# aerospike.OP_MAP_REMOVE_BY_RANK
# aerospike.OP_MAP_REMOVE_BY_RANK_RANGE
# aerospike.OP_MAP_GET_BY_KEY
# aerospike.OP_MAP_GET_BY_KEY_RANGE
# aerospike.OP_MAP_GET_BY_VALUE
# aerospike.OP_MAP_GET_BY_VALUE_RANGE
# aerospike.OP_MAP_GET_BY_INDEX
# aerospike.OP_MAP_GET_BY_INDEX_RANGE
# aerospike.OP_MAP_GET_BY_RANK
# aerospike.OP_MAP_GET_BY_RANK_RANGE


class TestOperate(object):

    def setup_class(cls):
        """
        Setup class.
        """
        cls.client_no_typechecks = TestBaseClass.get_new_connection(
          {'strict_types': False})

    def teardown_class(cls):
        TestOperate.client_no_typechecks.close()

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

        key = ('test', 'demo', 'bytes_key')
        rec = {"bytes_bin": b''}
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
        (
            ('test', 'demo', 1),
            [
                operations.prepend("name", u"ram"),
                operations.increment("age", 3),
                operations.read("name")
            ],
            {'name': 'ramname1'}),
        (
            ('test', 'demo', 1),                           # with_write_float_value
            [
                operations.write("write_bin", {"no": 89.8}),
                operations.read("write_bin")
            ],
            {'write_bin': {u'no': 89.8}}),
        (
            ('test', 'demo', 1),                            # write positive
            [
                operations.write("write_bin", {"no": 89}),
                operations.read("write_bin")
            ],
            {'write_bin': {u'no': 89}}),
        (
            ('test', 'demo', 1),                               # write_tuple_positive
            [
                operations.write("write_bin", ('a', 'b', 'c')),
                operations.read("write_bin")
            ],
            {'write_bin': ('a', 'b', 'c')}),
        (
            ('test', 'demo', 1),                               # with_bin_bytearray
            [
                operations.prepend("asd[;asjk", "ram"),
                operations.read("asd[;asjk")
            ],
            {'asd[;asjk': 'ram'}),
        (
            ('test', 'demo', 'bytearray_key'),                  # with_operator append_val bytearray
            [
                operations.append("bytearray_bin", bytearray("abc", "utf-8")),
                operations.read("bytearray_bin")
            ],
            {'bytearray_bin': bytearray("asd;as[d'as;dabc", "utf-8")}),
        (
            ('test', 'demo', 'bytearray_new'),                   # with_operator append_val bytearray_newrecord
            [
                operations.append("bytearray_bin",  bytearray("asd;as[d'as;d", "utf-8")),
                operations.read("bytearray_bin")
            ],
            {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}),
        (
            ('test', 'demo', 'bytes_key'),                  # with_operator append_val bytes
            [
                operations.append("bytes_bin", b"abc"),
                operations.read("bytes_bin")
            ],
            {'bytes_bin': b'abc'}),
        (
            ('test', 'demo', 'bytes_new'),                   # with_operator append_val bytes_newrecord
            [
                operations.append("bytes_bin",  b"asd;as[d'as;d"),
                operations.read("bytes_bin")
            ],
            {'bytes_bin': b"asd;as[d'as;d"}),
        (
            ('test', 'demo', 'bytearray_key'),  # with_operatorprepend_valbytearray
            [
                operations.prepend("bytearray_bin", bytearray("abc", "utf-8")),
                operations.read("bytearray_bin")
            ],
            {'bytearray_bin': bytearray("abcasd;as[d'as;d", "utf-8")}),
        (
            ('test', 'demo', 'bytearray_new'),                 # with_operatorprepend_valbytearray_newrecord
            [
                operations.prepend("bytearray_bin", bytearray("asd;as[d'as;d", "utf-8")),
                operations.read("bytearray_bin")
            ],
            {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}),
        (
            ('test', 'demo', 'bytes_key'),                  # with_operator prepend_val bytes
            [
                operations.prepend("bytes_bin", b"abc"),
                operations.read("bytes_bin")
            ],
            {'bytes_bin': b'abc'}),
        (
            ('test', 'demo', 'bytes_new'),                   # with_operator prepend_val bytes_newrecord
            [
                operations.prepend("bytes_bin",  b"asd;as[d'as;d"),
                operations.read("bytes_bin")
            ],
            {'bytes_bin': b"asd;as[d'as;d"}),
        (
            ('test', 'demo', 1),                               # write_bool_positive
            [
                operations.write("write_bin", True),
                operations.read("write_bin")
            ],
            {'write_bin': 1}),
        (
            ('test', 'demo', 1),                               # write_bool_positive
            [
                operations.write("write_bin", False),
                operations.read("write_bin")
            ],
            {'write_bin': 0}),
    ])
    def test_pos_operate_with_correct_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """

        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == expected
        self.as_connection.remove(key)

    @pytest.mark.parametrize("key, llist, expected", [
        (
            ('test', 'demo', 1),
            [
                operations.write("write_bin", {"no": 89.8}),
                operations.write("write_bin2", {"no": 100}),
                operations.delete(),
            ],
            {}),
    ])
    def test_pos_operate_delete_with_correct_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """

        key, _, bins = self.as_connection.operate(key, llist)

        assert bins == expected

    def test_pos_operate_with_increment_positive_float_value(self):
        """
        Invoke operate() with correct parameters
        """
        if TestOperate.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        llist = [
            operations.increment("age", 3.5),
            operations.read("age")
        ]

        _, _, bins = self.as_connection.operate(key, llist)

        assert bins == {'age': 9.8}

    def test_pos_operate_with_correct_policy(self):
        """
        Invoke operate() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        llist = [
            operations.append("name", "aa"),
            operations.increment("age", 3),
            operations.read("name")
        ]

        _, _, bins = self.as_connection.operate(key, llist, {}, policy)

        assert bins == {'name': 'name1aa'}

        self.as_connection.remove(key)


    @pytest.mark.parametrize("key, policy, meta, llist", [
        (('test', 'demo', 1),
            {'total_timeout': 1000,
             'key': aerospike.POLICY_KEY_SEND,
             'gen': aerospike.POLICY_GEN_IGNORE,
             'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL},
            {'gen': 10, 'ttl': 1200},
            [  
                operations.append("name","aa"),
                operations.increment("age", 3),
                operations.read("name")
            ]),
    ])
    def test_pos_operate_with_policy_gen_ignore(
            self, key, policy, meta, llist):
        """
        Invoke operate() with gen ignore.
        """

        key, meta, bins = self.as_connection.operate(key, llist, meta, policy)

        assert bins == {'name': 'name1aa'}


    def test_pos_operate_with_policy_gen_EQ(self):
        """
        Invoke operate() with gen EQ positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen}

        llist = [
            operations.append("name", "aa"),
            operations.increment("age", 3),
            operations.read("name")
        ]
        (key, meta, bins) = self.as_connection.operate(
            key, llist, meta, policy)

        assert bins == {'name': 'name1aa'}

    @pytest.mark.parametrize("key, llist", [
        (
            ('test', 'demo', 1),
            [
                operations.touch(4000)
            ]
        ),
        (
            ('test', 'demo', 1),
            [operations.touch(4000)]
        )
    ])
    def test_pos_operate_touch_operation_with_bin_and_value_combination(
            self, key, llist):
        """
        Invoke operate() with touch value with bin and value combination.
        """

        self.as_connection.operate(key, llist)

        (key, meta) = self.as_connection.exists(key)

        assert meta['ttl'] is not None

    def test_pos_operate_with_policy_gen_EQ_not_equal(self):
        """
        Invoke operate() with a mismatched generation, and verify
        it does not succeed
        """
        key = ('test', 'demo', 1)
        policy = {
            'gen': aerospike.POLICY_GEN_EQ
        }

        (_, meta) = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {
            'gen': gen + 5,
        }
        llist = [
            operations.append("name", "aa"),
            operations.increment("age", 3),
        ]

        with pytest.raises(e.RecordGenerationError):
            self.as_connection.operate(key, llist, meta, policy)

        _, _, bins = self.as_connection.get(key)
        assert bins == {"age": 1, 'name': 'name1'}


    def test_pos_operate_with_policy_gen_GT_mismatch(self):
        """
        Invoke operate() with gen GT policy, with amatching generation
        then verify that the operation was rejected properly
        """
        key = ('test', 'demo', 1)
        policy = {
            'gen': aerospike.POLICY_GEN_GT
        }
        _, meta = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen}

        llist = [
            operations.append("name", "aa"),
            operations.increment("age", 3)
        ]

        with pytest.raises(e.RecordGenerationError):
            self.as_connection.operate(
                key, llist, meta, policy)

        _, _, bins = self.as_connection.get(key)
        assert bins == {'age': 1, 'name': 'name1'}


    def test_pos_operate_touch_with_meta(self):
        """
        Invoke operate() OPERATE_TOUCH using meta to pass in ttl.
        """
        key = ('test', 'demo', 1)
        meta = {'ttl': 1200}

        llist = [operations.touch()]

        self.as_connection.operate(key, llist, meta)

        _, meta = self.as_connection.exists(key)

        assert meta['ttl'] <= 1200 and meta['ttl'] >= 1150

    def test_pos_operate_with_policy_gen_GT(self):
        """
        Invoke operate() with gen GT positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'gen': aerospike.POLICY_GEN_GT
        }
        _, meta = self.as_connection.exists(key)
        gen = meta['gen']
        meta = {'gen': gen + 5}

        llist = [
            operations.append("name", "aa"),
            operations.increment("age", 3),
            operations.read("name")
        ]
        _, _, bins = self.as_connection.operate(
            key, llist, meta, policy)

        assert bins == {'name': 'name1aa'}

    def test_pos_operate_with_nonexistent_key(self):
        """
        Invoke operate() with non-existent key
        """
        new_key = ('test', 'demo', "key11")
        llist = [
            operations.prepend("loc", "mumbai"),
            operations.read("loc")
        ]
        _, _, bins = self.as_connection.operate(new_key, llist)

        assert bins == {'loc': 'mumbai'}
        self.as_connection.remove(new_key)

    def test_pos_operate_with_nonexistent_bin(self):
        """
        Invoke operate() with non-existent bin
        """
        key = ('test', 'demo', 1)
        llist = [
            operations.append("addr", "pune"),
            operations.read("addr")
        ]
        _, _, bins = self.as_connection.operate(key, llist)

        assert bins == {'addr': 'pune'}

    def test_pos_operate_increment_nonexistent_key(self):
        """
        Invoke operate() with increment with nonexistent_key
        """
        key = ('test', 'demo', "non_existentkey")
        llist = [operations.increment("age", 5)]

        self.as_connection.operate(key, llist)

        _, _, bins = self.as_connection.get(key)

        assert bins == {"age": 5}

        self.as_connection.remove(key)


    def test_pos_operate_with_correct_paramters_without_connection(self):
        """
        Invoke operate() with correct parameters without connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        llist = [
            operations.touch()
        ]

        with pytest.raises(e.ClusterError):
            client1.operate(key, llist)


    def test_pos_operate_write_set_to_aerospike_null(self):
        """
        Invoke operate() with write command with bin set to aerospike_null
        """
        key = ('test', 'demo', 'null_record')

        bins = {"name": "John", "no": 3}

        self.as_connection.put(key, bins)

        llist = [
            operations.write("no", aerospike.null()),
            operations.read("no")
        ]

        _, _, bins = self.as_connection.operate(key, llist)
        assert {} == bins

        self.as_connection.remove(key)

    @pytest.mark.parametrize("key, llist, expected", [
        (
            ('test', 'demo', 'prepend_int'),                   # prepend_with_int
            [
                operations.prepend("age", 4),
                operations.read("age")
            ],
            {'age': 4}),
        (
            ('test', 'demo', 'append_dict'),                  # append_with_dict
            [
                operations.append("dict", {"a": 1, "b": 2}),
                operations.read("dict")
            ],
            {'dict': {"a": 1, "b": 2}}),
        (
            ('test', 'demo', 'incr_string'),               # incr_with_string
            [
                operations.increment("name", "aerospike"),
                operations.read("name")
            ],
            {'name': 'aerospike'}),
    ])
    def test_pos_operate_new_record(self, key, llist, expected):
        """
        Invoke operate() with prepend command on a new record
        """
        _, _, bins = TestOperate.client_no_typechecks.operate(key, llist)
        assert expected == bins
        TestOperate.client_no_typechecks.remove(key)

    @pytest.mark.parametrize("key, llist", [
        (
            ('test', 'demo', 1),
            [
                operations.prepend("age", 4),
                operations.read("age")
            ]
        ),
        (
            ('test', 'demo', 'existing_key'),                             # Existing list
            [
                operations.prepend("list", ['c']),
                operations.read("list")
            ]
        ),
        (
            ('test', 'demo', 'existing_key'),                         # Existing dict
            [
                operations.append("dict", {"c": 2}),
                operations.read("dict")
            ]
        ),
        (
            ('test', 'demo', 'existing_key'),                          # Exiting float
            [
                operations.append("float", 3.4),
                operations.read("float")
            ]
        ),
        (
            ('test', 'demo', 1),                                       # Existing string
            [
                operations.increment("name", "aerospike"),
                operations.read("name")
            ]
        ),
        (
            ('test', 'demo', 'existing_key'),                          # Existing Bytearray
            [
                operations.increment("bytearray", bytearray("abc", "utf-8")),
                operations.read("bytearray")
            ]),
    ])
    def test_pos_operate_prepend_with_existing_record(self, key, llist):
        """
        Invoke operate() with prepend command on a existing record
        """

        with pytest.raises(e.BinIncompatibleType):
           TestOperate.client_no_typechecks.operate(key, llist)

        TestOperate.client_no_typechecks.remove(key)

    def test_pos_operate_incr_with_geospatial_new_record(self):
        """
        Invoke operate() with incr command on a new record
        """
        key = ('test', 'demo', 'geospatial_key')

        llist = [
            operations.increment("geospatial", aerospike.GeoJSON({"type": "Point", "coordinates":
                                          [42.34, 58.62]})),
            operations.read("geospatial")
        ]

        _, _, bins = TestOperate.client_no_typechecks.operate(key, llist)

        assert bins['geospatial'].unwrap() == {
            'coordinates': [42.34, 58.62], 'type': 'Point'}
        TestOperate.client_no_typechecks.remove(key)

    def test_pos_operate_with_bin_length_extra_nostricttypes(self):
        """
        Invoke operate() with bin length extra. Strict types disabled
        """
        key = ('test', 'demo', 1)

        max_length = 'a' * 21

        llist = [
            operations.prepend("name", "ram"),
            operations.increment(max_length, 3)
        ]

        TestOperate.client_no_typechecks.operate(key, llist)

        _, _, bins = TestOperate.client_no_typechecks.get(key)

        assert bins == {"name": "ramname1", "age": 1}

    def test_pos_operate_prepend_set_to_aerospike_null(self):
        """
        Invoke operate() with prepend command with bin set to aerospike_null
        """
        key = ('test', 'demo', 'null_record')

        bins = {"name": "John", "no": 3}

        assert 0 == self.as_connection.put(key, bins)

        (key, _, bins) = self.as_connection.get(key)

        assert {"name": "John", "no": 3} == bins

        llist = [
            operations.prepend("no", aerospike.null()),
            operations.read("no")
        ]

        try:
            (key, _, bins) = self.as_connection.operate(key, llist)

        except e.InvalidRequest as exception:
            assert exception.code == 4
        self.as_connection.remove(key)

    @pytest.mark.parametrize("list, result, bin, expected", [
        (
            [
                list_operations.list_append("int_bin", 7),
                list_operations.list_get("int_bin", 4)
            ],
            {"int_bin": 7},
            "int_bin",
            [1, 2, 3, 4, 7]),
        (
            [
                list_operations.list_append_items("int_bin", [7, 9]),
                list_operations.list_get_range("int_bin", 3, 3),
            ],
            {'int_bin': [4, 7, 9]},
            "int_bin",
            [1, 2, 3, 4, 7, 9]
        ),
        (
            [
                list_operations.list_insert("int_bin", 2, 7),
                list_operations.list_pop("int_bin", 2)
            ],
            {'int_bin': 7},
            "int_bin",
            [1, 2, 3, 4]
        ),
        (
            [
                list_operations.list_insert_items("int_bin", 2, [7, 9]),
                list_operations.list_pop_range("int_bin", 2, 2)

            ],
            {'int_bin': [7, 9]},
            "int_bin", [1, 2, 3, 4]
        ),
        (
            [
                list_operations.list_set("int_bin", 2, 18),
                list_operations.list_get("int_bin", 2)
            ],
            {'int_bin': 18},
            "int_bin",
            [1, 2, 18, 4]
        ),
        (
            [
                list_operations.list_set("int_bin", 6, 10),
                list_operations.list_get("int_bin", 6)
            ],
            {'int_bin': 10},
            "int_bin",
            [1, 2, 3, 4, None, None, 10] # Inserting outside of the range adds nils in between
        )
    ])
    def test_pos_operate_with_list_addition_operations(self, list, result, bin,
                                                       expected):
        """
        Invoke operate() with list addition operations
        """
        key = ('test', 'demo', 'list_key')

        key, _, bins = self.as_connection.operate(key, list)

        assert bins == result

        key, _, bins = self.as_connection.get(key)
        assert bins[bin] == expected

    @pytest.mark.parametrize("list, bin, expected", [
        (
            [
            list_operations.list_remove("int_bin", 2)
            ],
            "int_bin", [1, 2, 4]
        ),
        (
            [
                list_operations.list_remove_range("int_bin", 2, 2)
            ],
            "int_bin",
            [1, 2]
        ),
        (   
            [
                list_operations.list_trim("int_bin", 2, 2)
            ],
            "int_bin",
            [3, 4]
        ),
        (
            [
                list_operations.list_clear("int_bin")
            ],
            "int_bin",
            []
        )
    ])
    def test_pos_operate_with_list_remove_operations(self, list, bin,
                                                     expected):
        """
        Invoke operate() with list remove operations
        """
        key = ('test', 'demo', 'list_key')

        self.as_connection.operate(key, list)

        key, _, bins = self.as_connection.get(key)

        assert bins[bin] == expected

    def test_pos_operate_with_list_size(self):
        """
        Invoke operate() with list_size operation
        """
        key = ('test', 'demo', 'list_key')
        list = [
            list_operations.list_size("int_bin")
        ]

        key, _, bins = self.as_connection.operate(key, list)

        assert bins == {'int_bin': 4}

    def test_list_increment_with_valid_value(self):
        '''
        previous list was [1, 2, 3, 4]
        new should be [1, 2, 23, 4]
        '''
        key = ('test', 'demo', 'list_key')
        list = [
            list_operations.list_increment("int_bin", 2, 20)
        ]

        _, _, bins = self.as_connection.operate(key, list)

        assert bins == {'int_bin': 23}
        _, _, bins = self.as_connection.get(key)

        assert bins['int_bin'] == [1, 2, 23, 4]

    def test_list_increment_with_incorrect_value_type(self):
        '''
        previous list was [1, 2, 3, 4]
        new should be [1, 2, 23, 4]
        '''
        key = ('test', 'demo', 'list_key')
        list = [
            list_operations.list_increment("int_bin", 2, "twenty")
        ]

        with pytest.raises(e.AerospikeError):
            self.as_connection.operate(key, list)

    def test_pos_operate_with_list_get_range_val_out_of_bounds(self):
        """
        Invoke operate() with list_get_range operation and value out of bounds
        """
        key = ('test', 'demo', 'list_key')
        list = [
            list_operations.list_get_range("int_bin", 2, 9)
        ]

        _, _, bins = self.as_connection.operate(key, list)

        assert bins == {'int_bin': [3, 4]}

    def test_pos_operate_with_list_trim_val_with_negative_value(self):
        """
        Invoke operate() with list_trimoperation and value is negative
        """
        key = ('test', 'demo', 'list_key')
        list = [
            list_operations.list_trim("int_bin", 1, -9)
        ]

        self.as_connection.operate(key, list)

        _, _, bins = self.as_connection.get(key)

        assert bins['int_bin'] == [2, 3, 4]

    def test_pos_operate_with_list_insert_index_negative(self):
        """
        Invoke operate() with list_insert and item index is a negative value
        """
        key = ('test', 'demo', 'list_key')
        list = [
            list_operations.list_insert("int_bin", -2, 9)
        ]

        self.as_connection.operate(key, list)

        _, _, bins = self.as_connection.get(key)

        assert bins['int_bin'] == [1, 2, 9, 3, 4]

    @pytest.mark.parametrize("list, result, bin, expected", [
        ([
            list_operations.list_append("string_bin", {"new_val": 1}),
            list_operations.list_get("string_bin", 4)
        ], {"string_bin": {"new_val": 1}}, "string_bin", ['a', 'b', 'c',
                                                          'd', {'new_val': 1}]),
        ([
            list_operations.list_append_items("string_bin", [['z', 'x'], ('y', 'w')]),
            list_operations.list_get_range("string_bin", 3, 3)
        ], {"string_bin": ['d', ['z', 'x'], ('y', 'w')]}, "string_bin", ['a', 'b', 'c',
                                                                         'd', ['z', 'x'], ('y', 'w')]),
        ([
            list_operations.list_insert("string_bin", 2, True),
            list_operations.list_pop("string_bin", 2)
        ], {'string_bin': True}, "string_bin", ['a', 'b', 'c', 'd']),
        ([
            list_operations.list_insert_items("string_bin", 2, [bytearray("abc", "utf-8"), u"xyz"]),
            list_operations.list_pop_range("string_bin", 2, 2)
        ], {'string_bin': [bytearray(b'abc'), 'xyz']}, "string_bin", ['a', 'b',
                                                                      'c', 'd']),
    ])
    def test_pos_operate_with_list_operations_different_datatypes(self, list, result, bin,
                                                                  expected):
        """
        Invoke operate() with list operations using different datatypes
        """
        key = ('test', 'demo', 'list_key')

        _, _, bins = self.as_connection.operate(key, list)

        assert bins == result

        _, _, bins = self.as_connection.get(key)

        assert bins[bin] == expected

    # Negative Tests
    def test_neg_operate_with_no_parameters(self):
        """
        Invoke operate() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.operate()
        assert "key" in str(typeError.value)

    @pytest.mark.parametrize("key, llist, expected", [
        (
            ('test', 'demo', 'bad_key'),
            [
                operations.delete(),
            ],
            e.RecordNotFound)
    ])
    def test_pos_operate_delete_with_incorrect_paramters(self, key, llist, expected):
        """
        Invoke operate() with correct parameters
        """

        with pytest.raises(expected):
            self.as_connection.operate(key, llist)

    def test_neg_operate_list_operation_bin_notlist(self):
        """
        Invoke operate() with a list operation and bin does not contain list
        """
        key = ('test', 'demo', 1)
        list = [
            list_operations.list_insert("age", 2, 9)
        ]

        with pytest.raises(e.BinIncompatibleType):
            self.as_connection.operate(key, list)

    def test_neg_operate_append_items_not_a_list(self):
        """
        Invoke operate() with list addition operations negative
        """
        key = ('test', 'demo', 1)
        ops = [list_operations.list_append_items("int_bin", 7)]
        with pytest.raises(e.ParamError):
            self.as_connection.operate(key, ops)

    @pytest.mark.parametrize("list", [
        (
            [list_operations.list_get("int_bin", 7)]
        ),
        (
            [
                list_operations.list_clear("int_bin"),
                list_operations.list_pop("int_bin", 2)
            ]
        ),
        (
            [
                list_operations.list_clear("int_bin"),
                list_operations.list_remove("int_bin", 2)
            ]
        )
    ])
    def test_neg_operate_list_invalid_requests(self, list):
        """
        Invoke operate() with list addition operations negative
        """
        key = ('test', 'demo', 'list_key')
        with pytest.raises(e.OpNotApplicable):
            self.as_connection.operate(key, list)


    def test_neg_operate_with_bin_length_extra(self):
        """
        Invoke operate() with bin length extra. Strict types enabled
        """
        key = ('test', 'demo', 1)

        max_length = 'a' * 21
        

        llist = [
            operations.prepend("name", "ram"),
            operations.increment(max_length, 3)
        ]

        with pytest.raises(e.BinNameError):
            self.as_connection.operate(key, llist)

    def test_neg_operate_empty_string_key(self):
        """
        Invoke operate() with empty string key
        """
        llist = [
            operations.prepend("name", "ram")
        ]
        with pytest.raises(e.ParamError):
            self.as_connection.operate("", llist)


    def test_neg_operate_with_extra_parameter(self):
        """
        Invoke operate() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        llist = [
            operations.prepend("name", "ram")
        ]
        with pytest.raises(TypeError) as typeError:
            self.as_connection.operate(key, llist, {}, policy, "")

    def test_neg_operate_policy_is_string(self):
        """
        Invoke operate() with policy is string
        """
        key = ('test', 'demo', 1)
        llist = [
            operations.prepend("name", "ram")
        ]
        try:
            self.as_connection.operate(key, llist, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_operate_key_is_none(self):
        """
        Invoke operate() with key is none
        """
        llist = [
            operations.prepend("name", "ram")
        ]
        try:
            self.as_connection.operate(None, llist)

        except e.ParamError as exception:
            assert exception.code == -2


    def test_neg_operate_append_value_integer(self):
        """
        Invoke operate() with append value is of type integer
        """
        key = ('test', 'demo', 1)
        llist = [
            operations.append("name", 12)
        ]

        try:
            self.as_connection.operate(key, llist)
        except e.ParamError as exception:
            assert exception.code == -2

    def test_neg_operate_with_incorrect_polic(self):
        """
        Invoke operate() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {'total_timeout': 0.5}
        llist = [
            operations.prepend("name", "ram")
        ]

        with pytest.raises(e.ParamError):
            self.as_connection.operate(key, llist, {}, policy)

