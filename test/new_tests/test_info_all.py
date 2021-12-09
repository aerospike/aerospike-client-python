# -*- coding: utf-8 -*-

import pytest
import sys

from aerospike import exception as e
from .test_base_class import TestBaseClass

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfo(object):

    def test_info_all(self):
        request = "statistics"
        nodes_info = self.as_connection.info_all(request)

        assert nodes_info is not None

        assert type(nodes_info) == dict
        
    def test_info_all_with_None_policy(self):
        request = "statistics"
        nodes_info = self.as_connection.info_all(request, None)

        assert nodes_info is not None

        assert type(nodes_info) == dict

    @pytest.mark.parametrize(
        "container_type, container_name",
        [
            ('namespaces', 'test'),
            ('sets', 'demo'),
            ('bins', 'names')
        ],
        ids=("namespace", "sets", "bins")
    )
    def test_positive_info_all(self, container_type, container_name):
        """
        Test to see whether a namespace, set,
        and bin exist after a key is added
        """
        key = ('test', 'demo', 'list_key')
        rec = {'names': ['John', 'Marlen', 'Steve']}

        self.as_connection.put(key, rec)

        response = self.as_connection.info_all(container_type)
        self.as_connection.remove(key)
        found = False
        for keys in response.keys():
            for value in response[keys]:
                if value is not None:
                    if container_name in value:
                        found = True

        assert found

    def test_info_all_with_config_for_statistics_and_policy(self):

        request = "statistics"
        policy = {'timeout': 1000}
        hosts = [host for host in self.connection_config['hosts']]

        nodes_info = self.as_connection.info_all(
            request, policy)

        assert nodes_info is not None
        assert isinstance(nodes_info, dict)

    def test_info_all_for_invalid_request(self):

        request = "fake_request_string_not_real"
        hosts = [host for host in self.connection_config['hosts']]
        nodes_info = self.as_connection.info_all(request)

        assert isinstance(nodes_info, dict)
        assert nodes_info.values() is not None

    def test_info_all_with_none_request(self):
        '''
        Test that sending None as the request raises an error
        '''
        request = None

        with pytest.raises(e.ParamError):
            self.as_connection.info_all(None)

    def test_info_all_without_parameters(self):

        with pytest.raises(TypeError) as err_info:
            self.as_connection.info_all()

    def test_info_all_without_connection(self):
        """
        Test info positive for sets without connection
        """
        client1 = aerospike.client(self.connection_config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.info_all('sets')

    def test_info_all_with_invalid_policy_type(self):
        '''
        Test that sending a non dict/None as policy raises an error
        '''
        request = None

        with pytest.raises(e.ParamError):
            self.as_connection.info_all(None, [])
