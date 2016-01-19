# -*- coding: utf-8 -*-

import pytest
import sys

from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestInfo(object):

    def setup_class(cls):
        """
        Setup class.
        """
        TestInfo.hostlist, user, password = TestBaseClass.get_hosts()
        config = {
            'hosts': TestInfo.hostlist
        }
        TestInfo.config = config
        if user is None and password is None:
            TestInfo.client = aerospike.client(config).connect()
        else:
            TestInfo.client = aerospike.client(config).connect(user, password)

    def teardown_class(cls):
        """
        Teardoen class.
        """

        TestInfo.client.close()

    def test_info_for_statistics(self):

        request = "statistics"

        nodes_info = TestInfo.client.info(request, TestInfo.config['hosts'])

        assert nodes_info is not None

        assert type(nodes_info) == dict

    def test_info_positive_for_namespace(self):
        """
        Test info positive for namespace
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}

        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('namespaces', TestInfo.config['hosts'])
        TestInfo.client.remove(key)
        flag = 0
        for keys in response.keys():
            for value in response[keys]:
                if value is not None:
                    if 'test' in value:
                        flag = 1
        if flag:
            assert True is True
        else:
            assert True is False

    def test_info_positive_for_sets(self):
        """
        Test info positive for sets
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}

        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('sets', TestInfo.config['hosts'])
        TestInfo.client.remove(key)
        flag = 0
        for keys in response.keys():
            for value in response[keys]:
                if value is not None:
                    if 'demo' in value:
                        flag = 1
        if flag:
            assert True is True
        else:
            assert True is False

    def test_info_positive_for_bins(self):
        """
        Test info positive for bins
        """
        key = ('test', 'demo', 'list_key')

        rec = {'names': ['John', 'Marlen', 'Steve']}

        TestInfo.client.put(key, rec)
        response = TestInfo.client.info('bins', TestInfo.config['hosts'])
        TestInfo.client.remove(key)
        flag = 0
        for keys in response.keys():
            for value in response[keys]:
                if value is not None:
                    if 'names' in value:
                        flag = 1
        if flag:
            assert True is True
        else:
            assert True is False

    def test_info_with_config_for_statistics(self):

        request = u"statistics"

        config = [(127, 3000)]

        try:
            TestInfo.client.info(request, config)

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Host address is of type incorrect"

    def test_info_with_config_for_statistics_and_policy(self):

        request = "statistics"
        policy = {'timeout': 1000}
        nodes_info = TestInfo.client.info(
            request, TestInfo.config['hosts'], policy)

        assert nodes_info is not None

        assert type(nodes_info) == dict

    def test_info_for_invalid_request(self):

        request = "no_info"

        nodes_info = TestInfo.client.info(request, TestInfo.config['hosts'])

        assert type(nodes_info) == dict

        assert nodes_info.values() != None

    def test_info_with_none_request(self):

        request = None

        try:
            TestInfo.client.info(request, TestInfo.config['hosts'])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Request must be a string"

    def test_info_without_parameters(self):

        with pytest.raises(TypeError) as typeError:
            TestInfo.client.info()

        assert "Required argument 'command' (pos 1) not found" in str(
            typeError.value)

    def test_info_positive_for_sets_without_connection(self):
        """
        Test info positive for sets without connection
        """
        client1 = aerospike.client(TestInfo.config)
        try:
            client1.info('sets', TestInfo.config['hosts'])

        except e.ClusterError as exception:
            assert exception.code == 11
            assert exception.msg == 'No connection to aerospike cluster'
