# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
import socket

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
        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }

        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('bins')
        TestInfo.client.remove(key)
        if 'names' in  response:
            assert True == True
        else:
            assert True == False

    def test_info_positive_for_namespace(self):
        """
        Test info with correct arguments
        """
        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }

        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('namespaces')
        TestInfo.client.remove(key)
        if 'test' in  response:
            assert True == True
        else:
            assert True == False

    def test_info_positive_for_sets(self):
        """
        Test info with correct arguments
        """
        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }

        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('sets')
        TestInfo.client.remove(key)
        if 'demo' in  response:
            assert True == True
        else:
            assert True == False

    def test_info_positive_for_sindex_creation(self):
        """
        Test info with correct arguments
        """
        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }
        policy = {}
        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('sindex-create:ns=test;set=demo;indexname=names_test_index;indexdata=names,string')
        time.sleep(2)
        TestInfo.client.remove(key)
        response = TestInfo.client.info('sindex')
        TestInfo.client.info('sindex-delete:ns=test;indexname=names_test_index')

        if 'names_test_index' in  response:
            assert True == True
        else:
            assert True == False

        

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
        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }
        TestInfo.client.put(key, rec)

        host = {}
        policy = {
            'timeout': 1000
        }
        response = TestInfo.client.info('bins', host, policy)
        TestInfo.client.remove(key)
        if 'names' in  response:
            assert True == True
        else:
            assert True == False

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
        key = ('test', 'demo', 'list_key')

        rec = {
                'names': ['John', 'Marlen', 'Steve']
            }
        TestInfo.client.put(key, rec)
        host = {"addr": "127.0.0.1", "port": 3000}
        response = TestInfo.client.info('bins', host)

        TestInfo.client.remove(key)
        if 'names' in  response:
            assert True == True
        else:
            assert True == False

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
