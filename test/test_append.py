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

class TestAppend(object):

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

    def test_append_with_no_parameters(self):
        """
        Invoke append() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.append()
        assert "Required argument 'key'" in typeError.value.message

    def test_append_with_correct_paramters(self):
        """
        Invoke append() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.client.append(key, "name", "str")

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1str'}

    def test_append_with_correct_policy(self):
        """
        Invoke append() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        self.client.append(key, "name", "str", policy)

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'age': 1, 'name': 'name1str'}

    def test_append_with_incorrect_policy(self):
        """
        Invoke append() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            self.client.append(key, "name", "str", policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_append_with_nonexistent_key(self):
        """
        Invoke append() with non-existent key
        """
        key = ('test', 'demo', 1000)
        status = self.client.append(key, "name", "str")

        assert status == 0L

    def test_append_with_nonexistent_bin(self):
        """
        Invoke append() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = self.client.append(key, "name1", "str")

        assert status == 0L

    def test_append_value_not_string(self):
        """
        Invoke append() not a string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            self.client.append(key, "name", 2)

        assert "append() argument 3 must be string, not int" in typeError.value.message

    def test_append_with_extra_parameter(self):
        """
        Invoke append() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.append(key, "name", "str", policy, "")

        assert "append() takes at most 4 arguments (5 given)" in typeError.value.message

    def test_append_policy_is_string(self):
        """
        Invoke append() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.append(key, "name", "pqr", "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"

    def test_append_key_is_none(self):
        """
        Invoke append() with key is none
        """
        with pytest.raises(Exception) as exception:
            self.client.append(None, "name", "str")

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_append_bin_is_none(self):
        """
        Invoke append() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.append(key, None, "str")

        assert exception.value[0] == -2
        assert exception.value[1] == "Bin should be a string"
