# -*- coding: utf-8 -*-

import pytest
import sys

from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfo(object):

    def test_info_for_statistics(self):
        request = "statistics"

        nodes_info = self.as_connection.info(request,
                                             self.connection_config['hosts'])

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
    def test_positive_info(self, container_type, container_name):
        """
        Test to see whether a namespace, set,
        and bin exist after a key is added
        """
        key = ('test', 'demo', 'list_key')
        rec = {'names': ['John', 'Marlen', 'Steve']}

        self.as_connection.put(key, rec)
        response = self.as_connection.info(container_type,
                                           self.connection_config['hosts'])
        self.as_connection.remove(key)
        found = False
        for keys in response.keys():
            for value in response[keys]:
                if value is not None:
                    if container_name in value:
                        found = True

        assert found

    def test_info_with_config_for_statistics_and_policy(self):

        request = "statistics"
        policy = {'timeout': 1000}
        nodes_info = self.as_connection.info(
            request, self.connection_config['hosts'], policy)

        assert nodes_info is not None
        assert isinstance(nodes_info, dict)

    def test_info_for_invalid_request(self):

        request = "no_info"

        nodes_info = self.as_connection.info(request,
                                             self.connection_config['hosts'])

        assert isinstance(nodes_info, dict)
        assert nodes_info.values() is not None

    def test_info_with_none_request(self):
        '''
        Test that sending None as the request raises an error
        '''
        request = None

        try:
            self.as_connection.info(request, self.connection_config['hosts'])

        except e.ParamError as exception:
            assert exception.code == -2
            assert exception.msg == "Request must be a string"

    def test_info_without_parameters(self):

        with pytest.raises(TypeError) as err_info:
            self.as_connection.info()

        assert "Required argument 'command' (pos 1) not found" in str(
            err_info.value)

    def test_info_positive_for_sets_without_connection(self):
        """
        Test info positive for sets without connection
        """
        client1 = aerospike.client(self.connection_config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.info('sets', self.connection_config['hosts'])

        assert err_info.value.code == 11
        assert err_info.value.msg == 'No connection to aerospike cluster'

    @pytest.mark.parametrize(
        "host_arg",
        [
            None,
            5,
            False,
            (),
            {},
        ],
        ids=['None', 'int', 'bool', 'tuple', 'dict']
    )
    def test_info_incorrect_host_type(self, host_arg):
        request = "statistics"

        with pytest.raises(e.ParamError):
            nodes_info = self.as_connection.info(request, host_arg)

    def test_info_with_host_as_tuple(self):
        request = "statistics"
        hosts_tuple = tuple(self.connection_config['hosts'])
        with pytest.raises(e.ParamError):
            nodes_info = self.as_connection.info(request, hosts_tuple)

    @pytest.mark.skip("This returns an empty dict",
                      "unsure if this is correct behavior")
    def test_info_host_as_list_of_list(self):
        """
        This currently segfaults
        """
        request = "statistics"
        hosts_l_o_l = [['127.0.0.1', 3000]]

        nodes_info = self.as_connection.info(request, hosts_l_o_l)

        assert nodes_info is not None
        assert isinstance(nodes_info, dict)

    def test_incorrect_host_address_type(self):

        request = u"statistics"

        config = [(127, 3000)]

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info(request, config)

        assert err_info.value.code == -2
        assert err_info.value.msg == "Host address is of type incorrect"
