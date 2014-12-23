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

class TestInfo(object):

    def setup_class(cls):
        """
        Setup class.
        """
        config = {
            'hosts': [('127.0.0.1', 3000)]
        }
        TestInfo.client = aerospike.client(config).connect()

    def teardown_class(cls):
        """
        Teardown class.
        """
        TestInfo.client.close()

    def test_info_no_parameters(self):
        """
        Test info with no parameters.
        """
        with pytest.raises(TypeError) as typeError:
            response = TestInfo.client.info()
        assert "Required argument 'req' (pos 1) not found" in typeError.value

    def test_info_positive(self):
        """
        Test info with correct arguments
        """
        response = TestInfo.client.info('bins')
        assert response != None

    def test_info_for_incorrect_command(self):
        """
        Test info for incorrect command
        """
        response = None
        with pytest.raises(Exception) as exception:
            response = TestInfo.client.info('abcd')

        assert exception.value[0] == -1
        assert exception.value[1] == "Info operation failed"

    def test_info_positive_with_correct_policy(self):
        """
        Test info with correct policy
        """
        host = {}
        policy = {
            'timeout': 1000
        }
        response = TestInfo.client.info('bins', host, policy)

        assert response != None

    def test_info_positive_with_incorrect_policy(self):
        """
        Test info with incorrect policy
        """
        host = {}
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(Exception) as exception:
            response = TestInfo.client.info('bins', host, policy)

        assert exception.value[0] == -2
        assert exception.value[1] == "timeout is invalid"

    def test_info_positive_with_host(self):
        """
        Test info with correct host
        """
        host = {"addr": "127.0.0.1", "port": 3000}
        response = TestInfo.client.info('bins', host)

        assert response != None

    def test_info_positive_with_incorrect_host(self):
        """
        Test info with incorrect host
        """
        host = {"addr": "122.0.0.1", "port": 3000}
        with pytest.raises(Exception) as exception:
            response = TestInfo.client.info('bins', host)

        assert exception.value[0] == -1
        assert exception.value[1] == "AEROSPIKE_ERR_CLIENT"

    def test_info_positive_with_all_parameters(self):
        """
        Test info with all parameters
        """
        host = {"addr": "127.0.0.1", "port": 3000}
        policy = {
            'timeout': 1000
        }
        response = TestInfo.client.info('logs', host, policy)

        assert response != None

    def test_info_positive_with_extra_parameters(self):
        """
        Test info with extra parameters
        """
        host = {"addr": "127.0.0.1", "port": 3000}
        policy = {
            'timeout': 1000
        }
        with pytest.raises(TypeError) as typeError:
            response = TestInfo.client.info('bins', host, policy, "")

        assert "info() takes at most 3 arguments (4 given)" in typeError.value

    def test_info_for_none_command(self):
        """
        Test info for None command
        """
        response = None
        with pytest.raises(TypeError) as typeError:
            response = TestInfo.client.info(None)

        assert "info() argument 1 must be string, not None" in typeError.value
