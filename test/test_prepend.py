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

class TestPrepend(object):

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

    def test_prepend_with_no_parameters(self):
        """
        Invoke prepend() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.prepend()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_prepend_with_correct_paramters(self):
        """
        Invoke prepend() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.client.prepend(key, "name", "str")

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1'}

    def test_prepend_with_correct_policy(self):
        """
        Invoke prepend() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'key' : aerospike.POLICY_KEY_SEND
        }
        self.client.prepend(key, "name", "str", policy)

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'age': 1, 'name': 'strname1'}

    def test_prepend_with_incorrect_policy(self):
        """
        Invoke prepend() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            self.client.prepend(key, "name", "str", policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_prepend_with_nonexistent_key(self):
        """
        Invoke prepend() with non-existent key
        """
        key = ('test', 'demo', 1000)
        status = self.client.prepend(key, "name", "str")

        assert status == 0L

    def test_prepend_with_nonexistent_bin(self):
        """
        Invoke prepend() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = self.client.prepend(key, "name1", "str")

        assert status == 0L

    def test_prepend_value_not_string(self):
        """
        Invoke prepend() not a string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            self.client.prepend(key, "name", 2)

        assert "prepend() argument 3 must be string, not int" in typeError.value

    def test_prepend_with_extra_parameter(self):
        """
        Invoke prepend() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.prepend(key, "name", "str", policy, "")
        
        assert "prepend() takes at most 4 arguments (5 given)" in typeError.value

    def test_prepend_policy_is_string(self):
        """
        Invoke prepend() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.prepend(key, "name", "abc", "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_prepend_key_is_none(self):
        """
        Invoke prepend() with key is none
        """
        with pytest.raises(Exception) as exception:
            self.client.prepend(None, "name", "str")

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_prepend_bin_is_none(self):
        """
        Invoke prepend() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.prepend(key, None, "str")

        assert exception.value[0] == -2
        assert exception.value[1] == "Bin should be a string"
