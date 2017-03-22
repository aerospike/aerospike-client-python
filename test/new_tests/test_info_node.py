# -*- coding: utf-8 -*-
import pytest
import time
import sys

from .as_status_codes import AerospikeStatus
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfoNode(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, connection_config):
        key = ('test', 'demo', 'list_key')
        rec = {'names': ['John', 'Marlen', 'Steve']}
        self.as_connection.put(key, rec)

        yield

        self.as_connection.remove(key)

    def test_info_node_positive(self):
        """
        Test info with correct arguments
        """

        response = self.as_connection.info_node(
            'bins', self.connection_config['hosts'][0])

        # This should probably make sure that a bin is actually named 'names'
        assert 'names' in response

    def test_info_node_positive_for_namespace(self):
        """
        Test info with 'namespaces' as the command
        """

        response = self.as_connection.info_node(
            'namespaces', self.connection_config['hosts'][0])

        assert 'test' in response

    def test_info_node_positive_for_sets(self):
        """
        Test info with 'sets' as the command
        """

        response = self.as_connection.info_node(
            'sets', self.connection_config['hosts'][0])

        assert 'demo' in response

    def test_info_node_positive_for_sindex_creation(self):
        """
        Test creating an index through an info call
        """
        try:
            self.as_connection.index_remove('test', 'names_test_index')
            time.sleep(2)
        except:
            pass

        self.as_connection.info_node(
            'sindex-create:ns=test;set=demo;indexname=names_test_index;indexdata=names,string',
            self.connection_config['hosts'][0])
        time.sleep(2)

        response = self.as_connection.info_node(
            'sindex', self.connection_config['hosts'][0])
        self.as_connection.index_remove('test', 'names_test_index')
        assert 'names_test_index' in response

    def test_info_node_positive_with_correct_policy(self):
        """
        Test info call with bins as command and a timeout policy
        """

        host = ()
        policy = {'timeout': 1000}
        response = self.as_connection.info_node('bins', host, policy)

        assert 'names' in response

    def test_info_node_positive_with_host(self):
        """
        Test info with correct host
        """

        host = self.connection_config['hosts'][0]
        response = self.as_connection.info_node('bins', host)

        assert 'names' in response

    def test_info_node_positive_with_all_parameters(self):
        """
        Test info with all parameters
        """
        policy = {
            'timeout': 1000
        }
        host = self.connection_config['hosts'][0]
        response = self.as_connection.info_node('logs', host, policy)

        assert response is not None

    def test_info_node_with_unicode_request_string_and_host_name(self):
        """
        Test info with all parameters
        """
        host = ((self.connection_config['hosts'][0][0]).encode("utf-8"),
                self.connection_config['hosts'][0][1])
        policy = {
            'timeout': 1000
        }
        response = self.as_connection.info_node(u'logs', host, policy)
        assert response is not None

    def test_info_node_positive_with_unicode_request_string_and_host_name(self
                                                                          ):
        """
        Test info with all parameters
        """
        host = ((self.connection_config['hosts'][0][0]).encode("utf-8"),
                self.connection_config['hosts'][0][1])
        policy = {'timeout': 1000}
        response = self.as_connection.info_node(u'logs', host, policy)

        assert response is not None


# Tests for incorrect usage
@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfoNodeIncorrectUsage(object):
    """
    Tests for invalid usage of the the info_node method
    """
    def test_info_node_no_parameters(self):
        """
        Test info with no parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.info_node()
        assert "Required argument 'command' (pos 1) not found" in str(
            typeError.value)

    def test_info_node_for_incorrect_command(self):
        """
        Test info for incorrect command
        """
        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.info_node(
                'abcd', self.connection_config['hosts'][0])

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_CLIENT
        assert err_info.value.msg == "Invalid info operation"

    @pytest.mark.skip(reason=("This goes to intranet ip"))
    def test_info_node_positive_with_valid_host(self):
        """
        Test info with incorrect host????
        #         """
        host = ("192.168.244.244", 3000)
        try:
            self.as_connection.info_node('bins', host)
        except e.ClientError as exception:
            assert exception.code == -1
        except e.TimeoutError as exception:
            assert exception.code == 9

    def test_info_node_positive_invalid_host(self):
        """
        Test info with incorrect host
        """
        host = ("abcderp", 3000)
        with pytest.raises(e.InvalidHostError) as err_info:
            self.as_connection.info_node('bins', host)

        assert (err_info.value.code ==
                AerospikeStatus.AEROSPIKE_INVALID_HOST)

    @pytest.mark.skip(reason="This goes to google's website")
    def test_info_node_positive_with_dns(self):
        """
        Test info with incorrect host
        # why is this hitting google's website?
        """
        host = ("google.com", 3000)
        try:
            self.as_connection.info_node('bins', host)

        except e.TimeoutError as exception:
            assert exception.code == 9
        except e.InvalidHostError as exception:
            assert exception.code == -4

    def test_info_node_positive_without_connection(self):
        """
        Test info with correct arguments without connection
        """
        client1 = aerospike.client(self.connection_config)
        with pytest.raises(e.ClusterError) as err_info:
            client1.info_node('bins', self.connection_config['hosts'][0])

        assert err_info.value.code == 11
        assert err_info.value.msg == 'No connection to aerospike cluster'

    def test_info_node_positive_with_extra_parameters(self):
        """
        Test info with extra parameters
        """
        host = self.connection_config['hosts'][0]
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.info_node('bins', host, policy, "")

        assert "info_node() takes at most 3 arguments (4 given)" in str(
            typeError.value)

    @pytest.mark.skip("This doesn't always raise the same error")
    def test_info_node_positive_with_incorrect_host(self):
        """
        Test info with incorrect host
        """
        host = ("127.0.0.1", 4500)
        with pytest.raises(e.InvalidHostError) as err_info:
            self.as_connection.info_node('bins', host)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_SERVER_ERROR

    def test_info_node_for_none_command(self):
        """
        Test info for None command
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info_node(
                None, self.connection_config['hosts'][0])

        assert err_info.value.code == -2
        assert err_info.value.msg == "Request should be of string type"

    def test_info_node_positive_with_incorrect_policy(self):
        """
        Test info with incorrect policy
        """
        host = ()
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info_node('bins', host, policy)

        assert err_info.value.code == -2
        assert err_info.value.msg == "timeout is invalid"
