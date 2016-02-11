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


class TestOperate(object):

    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            TestOperate.client = aerospike.client(config).connect()
        else:
            TestOperate.client = aerospike.client(config).connect(user,
                                                                  password)
        config_no_typechecks = {'hosts': hostlist, 'strict_types': False}
        if user is None and password is None:
            TestOperate.client_no_typechecks = aerospike.client(
                config_no_typechecks).connect()
        else:
            TestOperate.client_no_typechecks = aerospike.client(
                config_no_typechecks).connect(user, password)

        TestOperate.skip_old_server = True
        versioninfo = TestOperate.client.info('version')
        for keys in versioninfo:
            for value in versioninfo[keys]:
                if value is not None:
                    versionlist = value[
                        value.find("build") + 6:value.find("\n")].split(".")
                    if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 6:
                        TestOperate.skip_old_server = False

    def teardown_class(cls):
        TestOperate.client.close()
        TestOperate.client_no_typechecks.close()

    def setup_method(self, method):
        TestOperate.keys = []

        for i in range(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestOperate.client.put(key, rec)
        key = ('test', 'demo', 6)
        rec = {"age": 6.3}
        TestOperate.client.put(key, rec)

        key = ('test', 'demo', 'bytearray_key')
        rec = {"bytearray_bin": bytearray("asd;as[d'as;d", "utf-8")}
        TestOperate.client.put(key, rec)

        TestOperate.keys.append(key)

        key = ('test', 'demo', 'existing_key')
        rec = {"dict": {"a": 1}, "bytearray": bytearray("abc", "utf-8"),
               "float": 3.4, "list": ['a']}
        TestOperate.client.put(key, rec)

        TestOperate.keys.append(key)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in range(5):
            key = ('test', 'demo', i)
            try:
                TestOperate.client.remove(key)
            except e.RecordNotFound:
                pass
        for key in TestOperate.keys:
            try:
                TestOperate.client.remove(key)
            except e.RecordNotFound:
                pass

    def test_operate_with_no_parameters_negative(self):
        """
        Invoke opearte() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestOperate.client.operate()
        assert "Required argument 'key' (pos 1) not found" in str(
            typeError.value)

    def test_operate_with_correct_paramters_positive(self):
        """
        Invoke operate() with correct parameters
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'name': 'ramname1'}

    def test_operate_with_increment_positive_float_value(self):
        """
        Invoke operate() with correct parameters
        """
        if TestOperate.skip_old_server is True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        llist = [
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3.5},
            {"op": aerospike.OPERATOR_READ,
             "bin": "age"}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'age': 9.8}

    def test_operate_with_correct_policy_positive(self):
        """
        Invoke operate() with correct policy
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
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        key, _, bins = TestOperate.client.operate(key, llist, {}, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

        TestOperate.client.remove(key)

    def test_operate_with_policy_key_digest(self):
        """
        Invoke operate() with correct policy
        """
        key = ('test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
                                               "utf-8"))
        rec = {'name': 'name%s' % (str(1)), 'age': 1, }
        policy = {'timeout': 1000, 'key': aerospike.POLICY_KEY_DIGEST}
        TestOperate.client.put(key, rec)

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        key, _, bins = TestOperate.client.operate(key, llist, {}, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', None,
                       bytearray(b"asd;as[d\'as;djk;uyfl"))

    def test_operate_with_policy_gen_ignore(self):
        """
        Invoke operate() with gen ignore.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_IGNORE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }

        meta = {'gen': 10, 'ttl': 1200}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        key, meta, bins = TestOperate.client.operate(key, llist, meta, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_operate_with_policy_gen_EQ_positive(self):
        """
        Invoke operate() with gen EQ positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestOperate.client.exists(key)
        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        (key, meta, bins) = TestOperate.client.operate(
            key, llist, meta, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_operate_touch_operation_nobin_withvalue(self):
        """
        Invoke operate() with touch value. No bin specified. Value is specified
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_TOUCH,
             "val": 4000}
        ]

        TestOperate.client.operate(key, llist)

        (key, meta) = TestOperate.client.exists(key)

        assert meta['ttl'] != None

    def test_operate_touch_operation_withbin_withvalue(self):
        """
        Invoke operate() with touch operation. Bin and value both specified
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_TOUCH,
             "bin": "age",
             "val": 4000}
        ]

        TestOperate.client.operate(key, llist)

        (key, meta) = TestOperate.client.exists(key)

        assert meta['ttl'] != None

    def test_operate_touch_operation_withbin_novalue(self):
        """
        Invoke operate() with touch operation. Bin is specified but no value
        specified
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_TOUCH,
             "bin": "age"}
        ]

        TestOperate.client.operate(key, llist)

        (key, meta) = TestOperate.client.exists(key)

        assert meta['ttl'] != None

    def test_operate_touch_operation_nobin_novalue(self):
        """
        Invoke operate() with touch operation. Bin and value not specified
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_TOUCH}
        ]

        TestOperate.client.operate(key, llist)

        (key, meta) = TestOperate.client.exists(key)

        assert meta['ttl'] != None

    def test_operate_with_policy_gen_EQ_not_equal(self):
        """
        Invoke operate() with gen not equal.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_EQ
        }

        (key, meta) = TestOperate.client.exists(key)
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
            key, meta, _ = TestOperate.client.operate(key, llist, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3

        (key, meta, bins) = TestOperate.client.get(key)
        assert bins == {"age": 1, 'name': 'name1'}
        assert key == ('test', 'demo', None,
                       bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_operate_with_policy_gen_GT_lesser(self):
        """
        Invoke operate() with gen GT lesser.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestOperate.client.exists(key)
        gen = meta['gen']
        meta = {'gen': gen, 'ttl': 1200}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        try:
            (key, meta, _) = TestOperate.client.operate(
                key, llist, meta, policy)

        except e.RecordGenerationError as exception:
            assert exception.code == 3

        (key, meta, bins) = TestOperate.client.get(key)
        assert bins == {'age': 1, 'name': 'name1'}
        assert key == ('test', 'demo', None,
                       bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_operate_with_policy_gen_GT_positive(self):
        """
        Invoke operate() with gen GT positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'gen': aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestOperate.client.exists(key)
        gen = meta['gen']
        meta = {'gen': gen + 5, 'ttl': 1200}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": "aa"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        (key, meta, bins) = TestOperate.client.operate(
            key, llist, meta, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
        )

    def test_opearte_with_incorrect_policy_negative(self):
        """
        Invoke operate() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 0.5}
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            TestOperate.client.operate(key, llist, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_opearte_on_same_bin_negative(self):
        """
        Invoke operate() on same bin
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 5000}
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"},
            {"op": aerospike.OPERATOR_APPEND,
             "bin": "name",
             "val": "aa"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            TestOperate.client.operate(key, llist, {}, policy)

        except e.InvalidRequest as exception:
            assert exception.code == 4

    def test_operate_with_nonexistent_key_positive(self):
        """
        Invoke operate() with non-existent key
        """
        key1 = ('test', 'demo', "key11")
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "loc",
             "val": "mumbai"}, {"op": aerospike.OPERATOR_READ,
                                "bin": "loc"}
        ]
        _, _, bins = TestOperate.client.operate(key1, llist)

        assert bins == {'loc': 'mumbai'}
        TestOperate.client.remove(key1)

    def test_operate_with_nonexistent_bin_positive(self):
        """
        Invoke operate() with non-existent bin
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_APPEND,
             "bin": "addr",
             "val": "pune"}, {"op": aerospike.OPERATOR_READ,
                              "bin": "addr"}
        ]
        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'addr': 'pune'}

    def test_operate_empty_string_key_negative(self):
        """
        Invoke operate() with empty string key
        """
        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "name",
                "val": "ram"
            }
        ]
        try:
            TestOperate.client.operate("", llist)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_operate_with_extra_parameter_negative(self):
        """
        Invoke operate() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"}
        ]
        with pytest.raises(TypeError) as typeError:
            TestOperate.client.operate(key, llist, {}, policy, "")

        assert "operate() takes at most 4 arguments (5 given)" in str(
            typeError.value)

    def test_operate_policy_is_string_negative(self):
        """
        Invoke operate() with policy is string
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
            TestOperate.client.operate(key, llist, {}, "")

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_operate_key_is_none_negative(self):
        """
        Invoke operate() with key is none
        """
        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "name",
                "val": "ram"
            }
        ]
        try:
            TestOperate.client.operate(None, llist)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_operate_append_withot_value_parameter_negative(self):
        """
        Invoke operate() with append operation and append val is not given
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}

        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name"},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}]

        try:
            TestOperate.client.operate(key, llist, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Value should be given"

    def test_operate_with_extra_parameter_negative2(self):
        """
        Invoke operate() with more than 3 parameters given
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}

        llist = [{
            "op": aerospike.OPERATOR_APPEND,
            "bin": "name",
            "val": 3,
            "aa": 89
        }, ]

        try:
            TestOperate.client.operate(key, llist, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "operation can contain only op, bin, index and val keys"

    def test_operate_append_value_integer_negative(self):
        """
        Invoke operate() with append value is of type integer
        """
        key = ('test', 'demo', 1)
        llist = [{"op": aerospike.OPERATOR_APPEND,
                  "bin": "name",
                  "val": 12},
                 {"op": aerospike.OPERATOR_INCR,
                  "bin": "age",
                  "val": 3}, {"op": aerospike.OPERATOR_READ,
                              "bin": "name"}]

        try:
            TestOperate.client.operate(key, llist)
        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Cannot concatenate 'str' and 'non-str' objects"

    def test_operate_increment_nonexistent_key(self):
        """
        Invoke operate() with increment with nonexistent_key
        """
        key = ('test', 'demo', "non_existentkey")
        llist = [{"op": aerospike.OPERATOR_INCR, "bin": "age", "val": 5}]

        TestOperate.client.operate(key, llist)

        (key, _, bins) = TestOperate.client.get(key)

        assert bins == {"age": 5}

        TestOperate.client.remove(key)

    def test_operate_increment_nonexistent_bin(self):
        """
        Invoke operate() with increment with nonexistent_bin
        """
        key = ('test', 'demo', 1)
        llist = [{"op": aerospike.OPERATOR_INCR, "bin": "my_age", "val": 5}]

        TestOperate.client.operate(key, llist)

        (key, _, bins) = TestOperate.client.get(key)

        assert bins == {"my_age": 5, "age": 1, "name": "name1"}

    def test_operate_with_write_positive_float_value(self):
        """
        Invoke operate() with write operation float value
        """
        if TestOperate.skip_old_server is True:
            pytest.skip("Server does not support operation")
        key = ('test', 'demo', 1)
        llist = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": {"no": 89.8}
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'write_bin': {u'no': 89.8}}

    def test_operate_with_write_positive(self):
        """
        Invoke operate() with write operation
        """
        key = ('test', 'demo', 1)
        llist = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": {"no": 89}
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'write_bin': {u'no': 89}}

    def test_operate_with_write_tuple_positive(self):
        """
        Invoke operate() with write operation
        """
        key = ('test', 'demo', 1)
        llist = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": tuple('abc')
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'write_bin': ('a', 'b', 'c')}

    def test_operate_with_correct_paramters_positive_without_connection(self):
        """
        Invoke operate() with correct parameters without connection
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
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            key, _, _ = client1.operate(key, llist)

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == 'No connection to aerospike cluster'

    def test_operate_with_incr_value_string(self):
        """
        Invoke operate() with incr value negative
        """
        try:
            key = ('test', 'demo', 1)
            policy = {
                'timeout': 1000,
                'key': aerospike.POLICY_KEY_SEND,
                'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
            }

            llist = [{"op": aerospike.OPERATOR_INCR,
                      "bin": "age",
                      "val": "3"}, {"op": aerospike.OPERATOR_READ,
                                    "bin": "age"}]

            key, _, _ = TestOperate.client.operate(key, llist, {}, policy)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Unsupported operand type(s) for +: only 'int' allowed"

        TestOperate.client.remove(key)

    def test_operate_with_bin_bytearray_positive(self):
        """
        Invoke operate() with correct parameters
        """
        key = ('test', 'demo', 1)
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": bytearray("asd[;asjk", "utf-8"),
             "val": u"ram"},
            {"op": aerospike.OPERATOR_READ,
                "bin": bytearray("asd[;asjk", "utf-8")}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'asd[;asjk': 'ram'}

    def test_operate_with_operatorappend_valbytearray(self):
        """
        Invoke operate() with operator as append and value is a bytearray
        """
        key = ('test', 'demo', 'bytearray_key')
        llist = [
            {"op": aerospike.OPERATOR_APPEND,
             "bin": "bytearray_bin",
             "val": bytearray("abc", "utf-8")},
            {"op": aerospike.OPERATOR_READ,
             "bin": "bytearray_bin"}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {
            'bytearray_bin': bytearray("asd;as[d'as;dabc", "utf-8")}

    def test_operate_with_operatorappend_valbytearray_newrecord(self):
        """
        Invoke operate() with operator as append and value is a bytearray and a
        new record(does not exist)
        """
        key = ('test', 'demo', 'bytearray_new')
        llist = [
            {"op": aerospike.OPERATOR_APPEND,
             "bin": "bytearray_bin",
             "val": bytearray("asd;as[d'as;d", "utf-8")},
            {"op": aerospike.OPERATOR_READ,
             "bin": "bytearray_bin"}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}

        TestOperate.client.remove(key)

    def test_operate_with_operatorprepend_valbytearray(self):
        """
        Invoke operate() with operator as prepend and value is a bytearray
        """
        key = ('test', 'demo', 'bytearray_key')
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "bytearray_bin",
             "val": bytearray("abc", "utf-8")},
            {"op": aerospike.OPERATOR_READ,
             "bin": "bytearray_bin"}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {
            'bytearray_bin': bytearray("abcasd;as[d'as;d", "utf-8")}

    def test_operate_with_operatorprepend_valbytearray_newrecord(self):
        """
        Invoke operate() with operator as prepend and value is a bytearray
        and a new record(does not exist)
        """
        key = ('test', 'demo', 'bytearray_new')
        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "bytearray_bin",
             "val": bytearray("asd;as[d'as;d", "utf-8")},
            {"op": aerospike.OPERATOR_READ,
             "bin": "bytearray_bin"}
        ]

        key, _, bins = TestOperate.client.operate(key, llist)

        assert bins == {'bytearray_bin': bytearray("asd;as[d'as;d", "utf-8")}

        TestOperate.client.remove(key)

    def test_operate_write_set_to_aerospike_null(self):
        """
        Invoke operate() with write command with bin set to aerospike_null
        """
        key = ('test', 'demo', 'null_record')

        bins = {"name": "John", "no": 3}

        assert 0 == TestOperate.client.put(key, bins)

        (key, _, bins) = TestOperate.client.get(key)

        assert {"name": "John", "no": 3} == bins

        llist = [
            {
                "op": aerospike.OPERATOR_WRITE,
                "bin": "no",
                "val": aerospike.null
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "no"
            }
        ]

        (key, _, bins) = TestOperate.client.operate(key, llist)

        assert {} == bins

        TestOperate.client.remove(key)

    def test_operate_prepend_with_int_new_record(self):
        """
        Invoke operate() with prepend command on a new record
        """
        key = ('test', 'demo', 'prepend_int')

        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "age",
                "val": 4
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "age"
            }
        ]

        (key, _, bins) = TestOperate.client_no_typechecks.operate(key, llist)

        assert {'age': 4} == bins

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_prepend_with_int_existing_record(self):
        """
        Invoke operate() with prepend command on a existing record
        """
        key = ('test', 'demo', 1)

        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "age",
                "val": 4
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "age"
            }
        ]

        try:
            (key, _, _) = TestOperate.client_no_typechecks.operate(key, llist)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_prepend_with_list_existing_record(self):
        """
        Invoke operate() with prepend command on a existing record
        """
        key = ('test', 'demo', 'existing_key')

        (key, _) = TestOperate.client_no_typechecks.exists(key)

        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "list",
                "val": ['c']
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "list"
            }
        ]

        exception_raised = False
        try:
            (key, _, _) = TestOperate.client_no_typechecks.operate(key, llist)
        except e.BinIncompatibleType as exception:
            assert exception.code == 12
            exception_raised = True
        assert exception_raised is True

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_append_with_dict_new_record(self):
        """
        Invoke operate() with append command on a new record
        """
        key = ('test', 'demo', 'append_dict')

        llist = [
            {
                "op": aerospike.OPERATOR_APPEND,
                "bin": "dict",
                "val": {"a": 1, "b": 2}
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "dict"
            }
        ]

        (key, _, bins) = TestOperate.client_no_typechecks.operate(key, llist)

        assert {'dict': {"a": 1, "b": 2}} == bins

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_append_with_dict_existing_record(self):
        """
        Invoke operate() with append command on a existing record
        """
        key = ('test', 'demo', 'existing_key')

        (key, _) = TestOperate.client_no_typechecks.exists(key)

        llist = [
            {
                "op": aerospike.OPERATOR_APPEND,
                "bin": "dict",
                "val": {"c": 2}
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "dict"
            }
        ]

        exception_raised = False
        try:
            (key, _, _) = TestOperate.client_no_typechecks.operate(key, llist)
        except e.BinIncompatibleType as exception:
            assert exception.code == 12
            exception_raised = True
        assert exception_raised is True

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_append_with_float_existing_record(self):
        """
        Invoke operate() with append command on a existing record
        """
        key = ('test', 'demo', 'existing_key')

        llist = [
            {
                "op": aerospike.OPERATOR_APPEND,
                "bin": "float",
                "val": 3.4
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "float"
            }
        ]

        try:
            (key, _, _) = TestOperate.client_no_typechecks.operate(key, llist)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_incr_with_string_new_record(self):
        """
        Invoke operate() with incr command on a new record
        """
        key = ('test', 'demo', 'incr_string')

        llist = [
            {
                "op": aerospike.OPERATOR_INCR,
                "bin": "name",
                "val": "aerospike"
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "name"
            }
        ]

        (key, _, bins) = TestOperate.client_no_typechecks.operate(key, llist)

        assert {'name': 'aerospike'} == bins

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_incr_with_string_existing_record(self):
        """
        Invoke operate() with incr command on a existing record
        """
        key = ('test', 'demo', 1)

        llist = [
            {
                "op": aerospike.OPERATOR_INCR,
                "bin": "name",
                "val": "aerospike"
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "name"
            }
        ]

        try:
            (key, _, _) = TestOperate.client_no_typechecks.operate(key, llist)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_incr_with_bytearray_existing_record(self):
        """
        Invoke operate() with incr command on a new record
        """
        key = ('test', 'demo', 'existing_key')

        llist = [
            {
                "op": aerospike.OPERATOR_INCR,
                "bin": "bytearray",
                "val": bytearray("abc", "utf-8")
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "bytearray"
            }
        ]

        try:
            (key, _, _) = TestOperate.client_no_typechecks.operate(key, llist)

        except e.BinIncompatibleType as exception:
            assert exception.code == 12

        TestOperate.client_no_typechecks.remove(key)

    def test_operate_incr_with_geospatial_new_record(self):
        """
        Invoke operate() with incr command on a new record
        """
        key = ('test', 'demo', 'geospatial_key')

        llist = [
            {
                "op": aerospike.OPERATOR_INCR,
                "bin": "geospatial",
                "val": aerospike.GeoJSON({"type": "Point", "coordinates":
                                          [42.34, 58.62]})
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "geospatial"
            }
        ]

        (key, _, bins) = TestOperate.client_no_typechecks.operate(key, llist)

        assert bins['geospatial'].unwrap() == {
            'coordinates': [42.34, 58.62], 'type': 'Point'}
        TestOperate.client_no_typechecks.remove(key)

    def test_operate_with_bin_length_extra(self):
        """
        Invoke operate() with bin length extra. Strict types enabled
        """
        key = ('test', 'demo', 1)

        max_length = 'a'
        for _ in range(20):
            max_length = max_length + 'a'

        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": max_length,
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            key, _, _ = TestOperate.client.operate(key, llist)

        except e.BinNameError as exception:
            assert exception.code == 21

    def test_operate_with_bin_length_extra_nostricttypes(self):
        """
        Invoke operate() with bin length extra. Strict types disabled
        """
        key = ('test', 'demo', 1)

        max_length = 'a'
        for _ in range(20):
            max_length = max_length + 'a'

        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": max_length,
             "val": 3}
        ]

        TestOperate.client_no_typechecks.operate(key, llist)

        (key, _, bins) = TestOperate.client_no_typechecks.get(key)

        assert bins == {"name": "ramname1", "age": 1}

    def test_operate_with_command_invalid(self):
        """
        Invoke operate() with an invalid command. Strict types enabled
        """
        key = ('test', 'demo', 1)

        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": 3,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            key, _, _ = TestOperate.client.operate(key, llist)

        except e.ParamError as exception:
            assert exception.code == -2

    def test_operate_with_command_invalid_nostricttypes(self):
        """
        Invoke operate() with an invalid command. Strict types disabled
        """
        key = ('test', 'demo', 1)

        llist = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": 3,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        key, _, bins = TestOperate.client_no_typechecks.operate(key, llist)

        assert bins == {'name': 'ramname1'}

    def test_operate_prepend_set_to_aerospike_null(self):
        """
        Invoke operate() with prepend command with bin set to aerospike_null
        """
        key = ('test', 'demo', 'null_record')

        bins = {"name": "John", "no": 3}

        assert 0 == TestOperate.client.put(key, bins)

        (key, _, bins) = TestOperate.client.get(key)

        assert {"name": "John", "no": 3} == bins

        llist = [
            {
                "op": aerospike.OPERATOR_PREPEND,
                "bin": "no",
                "val": aerospike.null
            },
            {
                "op": aerospike.OPERATOR_READ,
                "bin": "no"
            }
        ]

        try:
            (key, _, bins) = TestOperate.client.operate(key, llist)

        except e.InvalidRequest as exception:
            assert exception.code == 4
        TestOperate.client.remove(key)
