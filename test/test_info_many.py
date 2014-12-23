# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
from test_base_class import TestBaseClass

try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestInfoMany(TestBaseClass):

    def setup_class(cls):

        """
        Setup class.
        """
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {
                'hosts': hostlist
                }
        if user == None and password == None:
            TestInfoMany.client = aerospike.client(config).connect()
        else:
            TestInfoMany.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):

        """
        Teardoen class.
        """

        TestInfoMany.client.close()

    def test_info_many_for_statistics(self):

        request = "statistics"

        nodes_info = TestInfoMany.client.info_many(request)

        assert nodes_info != None

        assert type(nodes_info) == dict

    def test_info_many_with_config_for_statistics(self):

        request = "statistics"

        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        nodes_info = TestInfoMany.client.info_many(request, config)

        assert nodes_info != None

        assert type(nodes_info) == dict

    def test_info_many_with_config_for_statistics_and_policy(self):

        request = "statistics"

        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        policy = {
                'timeout': 1000
        }
        nodes_info = TestInfoMany.client.info_many(request, config, policy)

        assert nodes_info != None

        assert type(nodes_info) == dict

    def test_info_many_for_invalid_request(self):

        request = "no_info"

        nodes_info = TestInfoMany.client.info_many(request)

        assert type(nodes_info) == dict

        assert nodes_info.values() != None

    def test_info_many_with_none_request(self):

        request = None

        with pytest.raises(Exception) as exception:
            nodes_info = TestInfoMany.client.info_many(request)

        assert exception.value[0] == -2
        assert exception.value[1] == "Request must be a string"

    def test_info_many_without_parameters(self):

        with pytest.raises(TypeError) as typeError:
            nodes_info = TestInfoMany.client.info_many()

        assert "Required argument 'req' (pos 1) not found" in typeError.value
