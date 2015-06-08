# -*- coding: utf-8 -*-
import pytest
import time
import sys
import cPickle as pickle
import socket
from test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    from aerospike.exception import *
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestInfoNode(object):
    def setup_class(cls):
        """
        Setup class.
        """
        TestInfoNode.hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': TestInfoNode.hostlist
                }
        TestInfoNode.config = config
        if user == None and password == None:
            TestInfoNode.client = aerospike.client(config).connect()
        else:
            TestInfoNode.client = aerospike.client(config).connect(user,
                                                                   password)

    def teardown_class(cls):
        """
        Teardown class.
        """
        TestInfoNode.client.close()

    def test_info_node_no_parameters(self):
        """
        Test info with no parameters.
        """
        with pytest.raises(TypeError) as typeError:
            response = TestInfoNode.client.info_node()
        assert "Required argument 'command' (pos 1) not found" in typeError.value

    def test_info_node_positive(self):
        """
        Test info with correct arguments
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}

        TestInfoNode.client.put(key, rec)
        response = TestInfoNode.client.info_node('bins', TestInfoNode.config['hosts'][0])
        TestInfoNode.client.remove(key)
        if 'names' in response:
            assert True == True
        else:
            assert True == False

    def test_info_node_positive_for_namespace(self):
        """
        Test info with correct arguments
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}

        TestInfoNode.client.put(key, rec)
        response = TestInfoNode.client.info_node('namespaces', TestInfoNode.config['hosts'][0])
        TestInfoNode.client.remove(key)
        if 'test' in response:
            assert True == True
        else:
            assert True == False

    def test_info_node_positive_for_sets(self):
        """
        Test info with correct arguments
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}

        TestInfoNode.client.put(key, rec)
        response = TestInfoNode.client.info_node('sets', TestInfoNode.config['hosts'][0])
        TestInfoNode.client.remove(key)
        if 'demo' in response:
            assert True == True
        else:
            assert True == False

    def test_info_node_positive_for_sindex_creation(self):
        """
        Test info with correct arguments
        """
        try:
            TestInfoNode.client.index_remove('test','names_test_index')
            time.sleep(2)
        except:
            pass
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}
        policy = {}
        TestInfoNode.client.put(key, rec)
        response = TestInfoNode.client.info_node('sindex-create:ns=test;set=demo;indexname=names_test_index;indexdata=names,string', TestInfoNode.config['hosts'][0])
        time.sleep(2)
        TestInfoNode.client.remove(key)
        response = TestInfoNode.client.info_node('sindex', TestInfoNode.config['hosts'][0])

        if 'names_test_index' in response:
            assert True == True
        else:
            assert True == False

    def test_info_node_for_incorrect_command(self):
        """
        Test info for incorrect command
        """
        response = None
        try:
            response = TestInfoNode.client.info_node('abcd', TestInfoNode.config['hosts'][0])

        except ClientError as exception:
            assert exception.code == -1
            assert exception.msg == "Invalid info operation"

    def test_info_node_positive_with_correct_policy(self):
        """
        Test info with correct policy
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}
        TestInfoNode.client.put(key, rec)

        host = ()
        policy = {'timeout': 1000}
        response = TestInfoNode.client.info_node('bins', host, policy)
        TestInfoNode.client.remove(key)
        if 'names' in response:
            assert True == True
        else:
            assert True == False

    def test_info_node_positive_with_incorrect_policy(self):
        """
        Test info with incorrect policy
        """
        host = ()
        policy = {
            'timeout': 0.5
        }
        try:
            response = TestInfoNode.client.info_node('bins', host, policy)

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "timeout is invalid"

    def test_info_node_positive_with_host(self):
        """
        Test info with correct host
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}
        TestInfoNode.client.put(key, rec)
        host = TestInfoNode.config['hosts'][0]
        response = TestInfoNode.client.info_node('bins', host)

        TestInfoNode.client.remove(key)
        if 'names' in response:
            assert True == True
        else:
            assert True == False

    def test_info_node_positive_with_incorrect_host(self):
        """
        Test info with incorrect host
        """
        host = ("122.0.0.1", 3000)
        try:
            response = TestInfoNode.client.info_node('bins', host)

        except TimeoutError as exception:
            assert exception.code == 9L
            assert exception.msg == ""

    def test_info_node_positive_with_all_parameters(self):
        """
        Test info with all parameters
        """
        policy = {
            'timeout': 1000
        }
        host = TestInfoNode.config['hosts'][0]
        response = TestInfoNode.client.info_node('logs', host, policy)

        assert response != None

    def test_info_node_positive_with_extra_parameters(self):
        """
        Test info with extra parameters
        """
        host = TestInfoNode.config['hosts'][0]
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            response = TestInfoNode.client.info_node('bins', host, policy, "")

        assert "info_node() takes at most 3 arguments (4 given)" in typeError.value

    def test_info_node_for_none_command(self):
        """
        Test info for None command
        """
        response = None
        try:
            response = TestInfoNode.client.info_node(None, TestInfoNode.config['hosts'][0])

        except ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Request should be of string type"

    def test_info_node_with_unicode_request_string_and_host_name(self):
        """
        Test info with all parameters
        """
        host = (unicode(TestInfoNode.config['hosts'][0][0]),
                TestInfoNode.config['hosts'][0][1])
        policy = {
            'timeout': 1000
        }
        response = TestInfoNode.client.info_node(u'logs', host, policy)

    def test_info_node_positive_with_unicode_request_string_and_host_name(self
                                                                         ):
        """
        Test info with all parameters
        """
        host = (unicode(TestInfoNode.config['hosts'][0][0]),
                TestInfoNode.config['hosts'][0][1])
        policy = {'timeout': 1000}
        response = TestInfoNode.client.info_node(u'logs', host, policy)

        assert response != None

    def test_info_node_positive_with_valid_host(self):
        """
        Test info with incorrect host
        """
        host = ( "192.168.244.244", 3000 )
        try:
            response = TestInfoNode.client.info_node('bins', host)
        except ClientError as exception:
            assert exception.code == -1L
        except TimeoutError as exception:
            assert exception.code == 9L
    
    def test_info_node_positive_invalid_host(self):
        """
        Test info with incorrect host
        """
        host = ("abcderp", 3000)
        with pytest.raises(Exception) as exception:
            response = TestInfoNode.client.info_node('bins', host)

        assert exception.value[0] == -4L

    def test_info_node_positive_with_dns(self):
        """
        Test info with incorrect host
        """
        host = ( "google.com", 3000 )
        try:
            response = TestInfoNode.client.info_node('bins', host)

        except TimeoutError as exception:
            assert exception.code == 9L
            assert exception.msg == ""

    def test_info_node_positive_without_connection(self):
        """
        Test info with correct arguments without connection
        """
        key = ('test', 'demo', 'list_key')

        client1 = aerospike.client(TestInfoNode.config)
        try:
            response = client1.info_node('bins', TestInfoNode.config['hosts'][0])

        except ClusterError as exception:
            assert exception.code == 11L
            assert exception.msg == 'No connection to aerospike cluster'
