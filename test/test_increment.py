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

class TestIncrement(object):

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

    def test_increment_with_no_parameters(self):
        """
        Invoke increment() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.increment()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_increment_with_correct_parameters(self):
        """
        Invoke increment() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.client.increment(key, "age", 5)

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}

    def test_increment_with_correct_policy(self):
        """
        Invoke increment() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND
        }
        self.client.increment(key, "age", 5, 0, policy)

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'age': 6, 'name': 'name1'}

    def test_increment_with_incorrect_policy(self):
        """
        Invoke increment() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            self.client.increment(key, "age", 5, 0, policy)

        assert exception.value[0] == -1
        assert exception.value[1] == "Invalid value(type) for policy key"

    def test_increment_with_nonexistent_key(self):
        """
        Invoke increment() with non-existent key
        """
        key = ('test', 'demo', 1000)
        with pytest.raises(Exception) as exception:
            status = self.client.increment(key, "age", 5)

        assert exception.value[0] == 2
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_NOT_FOUND"


    def test_increment_with_nonexistent_bin(self):
        """
        Invoke increment() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = self.client.increment(key, "age1", 5, 2)

        assert status == 0L

    def test_increment_value_is_string(self):
        """
        Invoke increment() value is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            self.client.increment(key, "age", "str")

        assert "an integer is required" in typeError.value

    def test_increment_with_extra_parameter(self):
        """
        Invoke increment() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.increment(key, "age", 2, 0, policy, "")

        assert "increment() takes at most 5 arguments (6 given)" in typeError.value

    def test_increment_policy_is_string(self):
        """
        Invoke increment() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.increment(key, "age", 2, 0, "")

        assert exception.value[0] == -1
        assert exception.value[1] == "Invalid policy(type)"

    def test_increment_key_is_none(self):
        """
        Invoke increment() with key is none
        """
        with pytest.raises(Exception) as exception:
            self.client.increment(None, "age", 2)

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_increment_bin_is_none(self):
        """
        Invoke increment() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.increment(key, None, 2)

        assert exception.value[0] == -2
        assert exception.value[1] == "Bin should be a string"
