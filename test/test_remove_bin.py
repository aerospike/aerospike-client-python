# -*- coding: utf-8 -*-
import pytest
import sys
import time
import cPickle as pickle

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestRemovebin(object):
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
            (key , meta, bins) = self.client.get(key)
            if bins != None:
                self.client.remove(key)

    def test_remove_bin_with_no_parameters(self):
        """
        Invoke remove_bin() without any mandatory parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.client.remove_bin()
        assert "Required argument 'key' (pos 1) not found" in typeError.value

    def test_remove_bin_with_correct_parameters(self):
        """
        Invoke remove_bin() with correct parameters
        """
        key = ('test', 'demo', 1)
        self.client.remove_bin(key, ["age"])

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'name': 'name1'}

    def test_remove_bin_with_correct_policy(self):
        """
        Invoke remove_bin() with correct policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        self.client.remove_bin(key, ["age"], policy)

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'name': 'name1'}

    def test_remove_bin_with_incorrect_policy(self):
        """
        Invoke remove_bin() with incorrect policy
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            self.client.remove_bin(key, ["age"], policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_remove_bin_with_nonexistent_key(self):
        """
        Invoke remove_bin() with non-existent key
        """
        key = ('test', 'demo', "non-existent")
        status = self.client.remove_bin(key, ["age"])

        assert status == 0L

    def test_remove_bin_with_nonexistent_bin(self):
        """
        Invoke remove_bin() with non-existent bin
        """
        key = ('test', 'demo', 1)
        status = self.client.remove_bin(key, ["non-existent"])

        assert status == 0L

    def test_remove_bin_with_extra_parameter(self):
        """
        Invoke remove_bin() with extra parameter.
        """
        key = ('test', 'demo', 1)
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            self.client.remove_bin(key, ["age"], policy, "")

        assert "remove_bin() takes at most 3 arguments (4 given)" in typeError.value

    def test_remove_bin_key_is_none(self):
        """
        Invoke remove_bin() with key is none
        """
        with pytest.raises(Exception) as exception:
            self.client.remove_bin(None, ["age"])

        assert exception.value[0] == -2
        assert exception.value[1] == "key is invalid"

    def test_remove_bin_bin_is_none(self):
        """
        Invoke remove_bin() with bin is none
        """
        key = ('test', 'demo', 1)
        with pytest.raises(Exception) as exception:
            self.client.remove_bin(key, None)

        assert exception.value[0] == -2
        assert exception.value[1] == "Bins should be a list"

    def test_remove_bin_no_bin(self):
        """
        Invoke remove_bin() no bin
        """
        key = ('test', 'demo', 1)
        self.client.remove_bin(key, [])

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == { 'name': 'name1', 'age': 1}

    def test_remove_bin_all_bins(self):
        """
        Invoke remove_bin() all bins
        """
        key = ('test', 'demo', 1)
        self.client.remove_bin(key, ["name", "age"])

        time.sleep(2)

        (key , meta, bins) = self.client.get(key)

        assert bins == None
