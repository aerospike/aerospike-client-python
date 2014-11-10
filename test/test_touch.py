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

class TestTouch(object):

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

    def test_touch_with_no_parameters(self):
        """
        Invoke touch() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.touch()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_touch_with_correct_paramters(self):
        """
        Invoke touch() with correct parameters
        """
        key = ('test', 'demo', 1)
        response = self.client.touch(key, 120)

        assert response == 0

    def test_touch_with_correct_policy(self):
        """
        Invoke touch() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000,
            'retry' : aerospike.POLICY_RETRY_ONCE 
        }
        response = self.client.touch(key, 120, policy)
        assert response == 0

    def test_touch_with_incorrect_policy(self):
        """
        Invoke touch() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            self.client.touch(key, 120, policy)

        assert exception.value[0] == -1
        #assert exception.value[1] == "timeout is invalid"
        assert exception.value[1] == "Invalid value(type) for policy"

    def test_touch_with_nonexistent_key(self):
        """
        Invoke touch() with non-existent key
        """
        key = ('test', 'demo', 1000)

        with pytest.raises(Exception) as exception:
            status = self.client.touch(key, 120)

        assert exception.value[0] == 2
        assert exception.value[1] == "AEROSPIKE_ERR_RECORD_NOT_FOUND"


    def test_touch_value_string(self):
        """
        Invoke touch() not a string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(TypeError) as typeError:
            self.client.touch(key, "name")

        assert "an integer is required" in typeError.value

    def test_touch_with_extra_parameter(self):
        """
        Invoke touch() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.touch(key, 120, policy, "")

        assert "touch() takes at most 3 arguments (4 given)" in typeError.value

    def test_touch_policy_is_string(self):
        """
        Invoke touch() with policy is string
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.touch(key, 120, "")

        assert exception.value[0] == -2
        assert exception.value[1] == "policy must be a dict"
