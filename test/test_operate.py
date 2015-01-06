# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestOperate(object):
    def setup_class(cls):
        """
        Setup method.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        TestOperate.client = aerospike.client(config).connect()

    def teardown_class(cls):
        TestOperate.client.close()

    def setup_method(self, method):
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name' : 'name%s' % (str(i)),
                'age' : i
            }
            TestOperate.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            TestOperate.client.remove(key)

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
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
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

        key, meta, bins = TestOperate.client.operate(key, list)


        assert bins == { 'name': 'ramname1'}

    def test_operate_with_correct_policy_positive(self):
        """
        Invoke operate() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_MASTER
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

        key, meta, bins = TestOperate.client.operate(key, list, {}, policy)


        assert bins == { 'name': 'name1aa'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_operate_with_policy_key_digest(self):
        """
        Invoke operate() with correct policy
        """
        key = ( 'test', 'demo', None, bytearray("asd;as[d'as;djk;uyfl",
               "utf-8"))
        rec = {
            'name' : 'name%s' % (str(1)),
            'age' : 1,
        }
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_DIGEST
        }
        TestOperate.client.put(key, rec)

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

        key, meta, bins = TestOperate.client.operate(key, list, {}, policy)


        assert bins == { 'name': 'name1aa'}
        assert key == ('test', 'demo', None,
                bytearray(b"asd;as[d\'as;djk;uyfl"))

    def test_operate_with_policy_gen_ignore(self):
        """
        Invoke operate() with gen ignore.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'gen' : aerospike.POLICY_GEN_IGNORE,
            'commit_level': aerospike.POLICY_COMMIT_LEVEL_ALL
        }

        meta = {
            'gen': 10,
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

        key, meta, bins = TestOperate.client.operate(key, list, meta, policy)


        assert bins == { 'name': 'name1aa'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_operate_with_policy_gen_EQ_positive(self):
        """
        Invoke operate() with gen EQ positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'gen' : aerospike.POLICY_GEN_EQ
        }
        (key, meta) = TestOperate.client.exists(key)
        gen = meta['gen']
        meta = {
            'gen': gen,
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

        (key, meta, bins) = TestOperate.client.operate(key, list, meta, policy)


        assert bins == { 'name': 'name1aa'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_operate_with_policy_gen_EQ_not_equal(self):
        """
        Invoke operate() with gen not equal.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'gen' : aerospike.POLICY_GEN_EQ
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

        with pytest.raises(Exception) as exception:
            key, meta, bins = TestOperate.client.operate(key, list, meta, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "expected 1 bins, got 0"
       
        (key , meta, bins) = TestOperate.client.get(key)
        assert bins == { "age": 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))
        
    def test_operate_with_policy_gen_GT_lesser(self):
        """
        Invoke operate() with gen GT lesser.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'gen' : aerospike.POLICY_GEN_GT
        }
        (key, meta) = TestOperate.client.exists(key)
        gen = meta['gen']
        meta = {
            'gen': gen,
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

        with pytest.raises(Exception) as exception:
            (key, meta, bins) = TestOperate.client.operate(key, list, meta, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "expected 1 bins, got 0"
        
        (key , meta, bins) = TestOperate.client.get(key)
        assert bins == { 'age' : 1, 'name': 'name1'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_operate_with_policy_gen_GT_positive(self):
        """
        Invoke operate() with gen GT positive.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND,
            'gen' : aerospike.POLICY_GEN_GT
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

        (key, meta, bins) = TestOperate.client.operate(key, list, meta, policy)


        assert bins == { 'name': 'name1aa'}
        assert key == ('test', 'demo', 1,
                bytearray(b'\xb7\xf4\xb88\x89\xe2\xdag\xdeh>\x1d\xf6\x91\x9a\x1e\xac\xc4F\xc8'))

    def test_opearte_with_incorrect_policy_negative(self):
        """
        Invoke operate() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
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

        with pytest.raises(Exception) as exception:
            (bins) = TestOperate.client.operate(key, list, {}, policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_opearte_on_same_bin_negative(self):
        """
        Invoke operate() on same bin
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 5000
        }
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
                    },
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

        with pytest.raises(Exception) as exception:
            (bins) = TestOperate.client.operate(key, list, {}, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "expected 1 bins, got 0"

    def test_operate_with_nonexistent_key_positive(self):
        """
        Invoke operate() with non-existent key
        """
        key1 = ('test', 'demo', "key78")
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "loc",
                    "val" : "mumbai"
                    },
                {
                    "op" : aerospike.OPERATOR_READ,
                    "bin" : "loc"
                    }
                ]
        key, meta, bins = TestOperate.client.operate(key1, list)


        assert bins == { 'loc' : 'mumbai'}
        TestOperate.client.remove(key1)

    def test_operate_with_nonexistent_bin_positive(self):
        """
        Invoke operate() with non-existent bin
        """
        key = ('test', 'demo', 1)
        list = [
                {
                    "op" : aerospike.OPERATOR_APPEND,
                    "bin" : "addr",
                    "val" : "pune"
                    },
                {
                    "op" : aerospike.OPERATOR_READ,
                    "bin" : "addr"
                    }
                ]
        key, meta, bins = TestOperate.client.operate(key, list)


        assert bins == { 'addr': 'pune'}

    def test_operate_empty_string_key_negative(self):
        """
        Invoke operate() with empty string key
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
                    }
                ]
        with pytest.raises(Exception) as exception:
            TestOperate.client.operate("", list)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_operate_with_extra_parameter_negative(self):
        """
        Invoke operate() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        list = [
                {
                    "op" : aerospike.OPERATOR_PREPEND,
                    "bin" : "name",
                    "val" : "ram"
                    }
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
        with pytest.raises(Exception) as exception:
            TestOperate.client.operate(key, list, {}, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

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
        with pytest.raises(Exception) as exception:
            TestOperate.client.operate(None, list)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

