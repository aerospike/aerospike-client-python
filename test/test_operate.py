# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestOperate(object):
    def setup_class(cls):
        """
        Setup method.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user == None and password == None:
            TestOperate.client = aerospike.client(config).connect()
        else:
            TestOperate.client = aerospike.client(config).connect(user,
                                                                  password)
        TestOperate.skip_old_server = True
        versioninfo = TestOperate.client.info('version')
        for keys in versioninfo:
            for value in versioninfo[keys]:
                if value != None:
                    versionlist = value[value.find("build") + 6:value.find("\n")].split(".")
                    if int(versionlist[0]) >= 3 and int(versionlist[1]) >= 6:
                        TestOperate.skip_old_server = False

    def teardown_class(cls):
        TestOperate.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {'name': 'name%s' % (str(i)), 'age': i}
            TestOperate.client.put(key, rec)
        key = ('test', 'demo', 6)
        rec = {"age": 6.3}
        TestOperate.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            try:
                TestOperate.client.remove(key)
            except RecordNotFound as exception:
                pass

    def test_operate_with_no_parameters_negative(self):
        """
        Invoke opearte() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            TestOperate.client.operate()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_operate_with_correct_paramters_positive(self):
        """
        Invoke operate() with correct parameters
        """
        key = ('test', 'demo', 1)
        list = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        key, meta, bins = TestOperate.client.operate(key, list)

        assert bins == {'name': 'ramname1'}

    def test_operate_with_increment_positive_float_value(self):
        """
        Invoke operate() with correct parameters
        """
        if TestOperate.skip_old_server == True:
            pytest.skip("Server does not support increment on float type")
        key = ('test', 'demo', 6)
        list = [
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3.5}, 
            {"op": aerospike.OPERATOR_READ,
             "bin": "age"}
        ]

        key, meta, bins = TestOperate.client.operate(key, list)

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

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        key, meta, bins = TestOperate.client.operate(key, list, {}, policy)

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

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        key, meta, bins = TestOperate.client.operate(key, list, {}, policy)

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

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        key, meta, bins = TestOperate.client.operate(key, list, meta, policy)

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

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        (key, meta, bins) = TestOperate.client.operate(key, list, meta, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )

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
        list = [
                {
                    "op" : aerospike.OPERATOR_APPEND,
                    "bin" : "name",
                    "val" : "aa"
                    },
                {
                    "op" : aerospike.OPERATOR_INCR,
                    "bin" : "age",
                    "val" : 3
                    },
                {
                    "op" : aerospike.OPERATOR_READ,
                    "bin" : "name"
                    }
                ]
        try:
            key, meta, bins = TestOperate.client.operate(key, list, meta, policy)

        except RecordGenerationError as exception:
            assert exception.code == 3L
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"
       
        (key , meta, bins) = TestOperate.client.get(key)
        assert bins == { "age": 1, 'name': 'name1'}
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

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        try:
            (key, meta, bins) = TestOperate.client.operate(key, list, meta, policy)

        except RecordGenerationError as exception:
            assert exception.code == 3L
            assert exception.msg == "AEROSPIKE_ERR_RECORD_GENERATION"
        
        (key , meta, bins) = TestOperate.client.get(key)
        assert bins == { 'age' : 1, 'name': 'name1'}
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

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        (key, meta, bins) = TestOperate.client.operate(key, list, meta, policy)

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
        list = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            (bins) = TestOperate.client.operate(key, list, {}, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_opearte_on_same_bin_negative(self):
        """
        Invoke operate() on same bin
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 5000}
        list = [
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
            (bins) = TestOperate.client.operate(key, list, {}, policy)

        except InvalidRequest as exception:
            assert exception.code == 4L
            assert exception.msg == "AEROSPIKE_ERR_REQUEST_INVALID"

    def test_operate_with_nonexistent_key_positive(self):
        """
        Invoke operate() with non-existent key
        """
        key1 = ('test', 'demo', "key11")
        list = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "loc",
             "val": "mumbai"}, {"op": aerospike.OPERATOR_READ,
                                "bin": "loc"}
        ]
        key, meta, bins = TestOperate.client.operate(key1, list)

        assert bins == {'loc': 'mumbai'}
        TestOperate.client.remove(key1)

    def test_operate_with_nonexistent_bin_positive(self):
        """
        Invoke operate() with non-existent bin
        """
        key = ('test', 'demo', 1)
        list = [
            {"op": aerospike.OPERATOR_APPEND,
             "bin": "addr",
             "val": "pune"}, {"op": aerospike.OPERATOR_READ,
                              "bin": "addr"}
        ]
        key, meta, bins = TestOperate.client.operate(key, list)

        assert bins == {'addr': 'pune'}

    def test_operate_empty_string_key_negative(self):
        """
        Invoke operate() with empty string key
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 0.5}
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
                    }
                ]
        try:
            TestOperate.client.operate("", list)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_operate_with_extra_parameter_negative(self):
        """
        Invoke operate() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}
        list = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": "ram"}
        ]
        with pytest.raises(TypeError) as typeError:
            TestOperate.client.operate(key, list, {}, policy, "")

        assert "operate() takes at most 4 arguments (5 given)" in typeError.value

    def test_operate_policy_is_string_negative(self):
        """
        Invoke operate() with policy is string
        """
        key = ('test', 'demo', 1)
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
                    }
                ]
        try:
            TestOperate.client.operate(key, list, {}, "")

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "policy must be a dict"

    def test_operate_key_is_none_negative(self):
        """
        Invoke operate() with key is none
        """
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
                    }
                ]
        try:
            TestOperate.client.operate(None, list)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "key is invalid"

    def test_operate_append_withot_value_parameter_negative(self):
        """
        Invoke operate() with append operation and append val is not given
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}]

        try:
            TestOperate.client.operate(key, list, {}, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Value should be given"

    def test_operate_with_extra_parameter_negative(self):
        """
        Invoke operate() with more than 3 parameters given
        """
        key = ('test', 'demo', 1)
        policy = {'timeout': 1000}

        list = [{
            "op": aerospike.OPERATOR_APPEND,
            "bin": "name",
            "val": 3,
            "aa": 89
        }, ]

        try:
            TestOperate.client.operate(key, list, {}, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "operation can contain only op, bin and val keys"

    def test_operate_append_value_integer_negative(self):
        """
        Invoke operate() with append value is of type integer
        """
        key = ('test', 'demo', 1)
        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": 12},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": 3}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        try:
            TestOperate.client.operate(key, list)
        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Cannot concatenate 'str' and 'non-str' objects"

    def test_operate_increment_value_string_negative(self):
        """
        Invoke operate() with increment value is of type string
        """
        key = ('test', 'demo', 1)
        list = [
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": "lllllll"}, {"op": aerospike.OPERATOR_READ,
                                 "bin": "name"}
        ]

        try:
            TestOperate.client.operate(key, list)
        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Unsupported operand type(s) for +: 'int' and 'str'"

    def test_operate_increment_nonexistent_key(self):
        """
        Invoke operate() with increment with nonexistent_key
        """
        key = ('test', 'demo', "non_existentkey")
        list = [{"op": aerospike.OPERATOR_INCR, "bin": "age", "val": 5}]

        TestOperate.client.operate(key, list)

        (key, meta, bins) = TestOperate.client.get(key)

        assert bins == {"age": 5}

        TestOperate.client.remove(key)

    def test_operate_increment_nonexistent_bin(self):
        """
        Invoke operate() with increment with nonexistent_bin
        """
        key = ('test', 'demo', 1)
        list = [{"op": aerospike.OPERATOR_INCR, "bin": "my_age", "val": 5}]

        TestOperate.client.operate(key, list)

        (key, meta, bins) = TestOperate.client.get(key)

        assert bins == {"my_age": 5, "age": 1, "name": "name1"}

    def test_operate_with_write_positive_float_value(self):
        """
        Invoke operate() with write operation float value
        """
        if TestOperate.skip_old_server == True:
            pytest.skip("Server does not support operation")
        key = ('test', 'demo', 1)
        list = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": {"no": 89.8}
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, meta, bins = TestOperate.client.operate(key, list)

        assert bins == {'write_bin': {u'no': 89.8}}

    def test_operate_with_write_positive(self):
        """
        Invoke operate() with write operation
        """
        key = ('test', 'demo', 1)
        list = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": {"no": 89}
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, meta, bins = TestOperate.client.operate(key, list)

        assert bins == {'write_bin': {u'no': 89}}

    def test_operate_with_write_tuple_positive(self):
        """
        Invoke operate() with write operation
        """
        key = ('test', 'demo', 1)
        list = [{
            "op": aerospike.OPERATOR_WRITE,
            "bin": "write_bin",
            "val": tuple('abc')
        }, {"op": aerospike.OPERATOR_READ,
            "bin": "write_bin"}]

        key, meta, bins = TestOperate.client.operate(key, list)

        assert bins == {'write_bin': ('a', 'b', 'c')}

    def test_operate_with_correct_paramters_positive_without_connection(self):
        """
        Invoke operate() with correct parameters without connection
        """
        key = ('test', 'demo', 1)
        config = {'hosts': [('127.0.0.1', 3000)]}
        client1 = aerospike.client(config)
        list = [
            {"op": aerospike.OPERATOR_PREPEND,
             "bin": "name",
             "val": u"ram"},
            {"op": aerospike.OPERATOR_INCR,
             "bin": "age",
             "val": 3}, {"op": aerospike.OPERATOR_READ,
                         "bin": "name"}
        ]

        try:
            key, meta, bins = client1.operate(key, list)

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'

    def test_operate_with_incr_value_string(self):
        """
        Invoke operate() with incr value negative
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key': aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
        }

        list = [{"op": aerospike.OPERATOR_APPEND,
                 "bin": "name",
                 "val": "aa"},
                {"op": aerospike.OPERATOR_INCR,
                 "bin": "age",
                 "val": "3"}, {"op": aerospike.OPERATOR_READ,
                             "bin": "name"}]

        key, meta, bins = TestOperate.client.operate(key, list, {}, policy)

        assert bins == {'name': 'name1aa'}
        assert key == ('test', 'demo', 1, bytearray(
            b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8')
                      )

        TestOperate.client.remove(key)
