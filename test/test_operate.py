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

    def setup_method(self, method):
        """
        Setup method.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        self.client = aerospike.client(config).connect()
        for i in xrange(5):
            key = ('test', 'demo', i)
            rec = {
                'name' : 'name%s' % (str(i)),
                'age' : i
            }
            self.client.put(key, rec)

    def teardown_method(self, method):
        """
        Teardoen method.
        """
        for i in xrange(5):
            key = ('test', 'demo', i)
            self.client.remove(key)

    def test_operate_with_no_parameters_negative(self):
        """
        Invoke opearte() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.operate()
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
        
        key, meta, bins = self.client.operate(key, list)

        time.sleep(2)

        assert bins == { 'name': 'ramname1'}

    def test_operate_with_correct_policy_positive(self):
        """
        Invoke operate() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND
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
        
        key, meta, bins = self.client.operate(key, list, policy)

        time.sleep(2)
        assert bins == { 'name': 'name1aa'}

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
            (bins) = self.client.operate(key, list, policy)

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
            (bins) = self.client.operate(key, list, policy)

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
        key, meta, bins = self.client.operate(key1, list)

        time.sleep(2)

        assert bins == { 'loc' : 'mumbai'}
        self.client.remove(key1)

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
        key, meta, bins = self.client.operate(key, list)

        time.sleep(2)

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
            self.client.operate("", list)

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
            self.client.operate(key, list, policy, "")

        assert "operate() takes at most 3 arguments (4 given)" in typeError.value

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
            self.client.operate(key, list, "")

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
            self.client.operate(None, list)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

