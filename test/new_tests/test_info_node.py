# -*- coding: utf-8 -*-
import pytest
import time
import sys

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


def as_unicode(string):
    try:
        string = unicode(string)
    except:
        pass
    return string


@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfoNode(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, connection_config):
        key = ('test', 'demo', 'list_key')
        rec = {'names': ['John', 'Marlen', 'Steve']}
        self.host_name = self.connection_config['hosts'][0]
        self.as_connection.put(key, rec)

        yield

        self.as_connection.remove(key)

    def test_info_node_positive(self):
        """
        Test info with correct arguments
        """
        response = self.as_connection.info_node(
            'bins', self.host_name)

        # This should probably make sure that a bin is actually named 'names'
        assert 'names' in response

    def test_info_node_positive_for_namespace(self):
        """
        Test info with 'namespaces' as the command
        """

        response = self.as_connection.info_node(
            'namespaces', self.host_name)

        assert 'test' in response

    def test_info_node_positive_for_sets(self):
        """
        Test info with 'sets' as the command
        """

        response = self.as_connection.info_node(
            'sets', self.host_name)

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
            self.host_name)
        time.sleep(2)

        response = self.as_connection.info_node(
            'sindex', self.host_name)
        self.as_connection.index_remove('test', 'names_test_index')
        assert 'names_test_index' in response

    def test_info_node_positive_with_correct_policy(self):
        """
        Test info call with bins as command and a timeout policy
        """

        host = self.host_name
        policy = {'timeout': 1000}
        response = self.as_connection.info_node('bins', host, policy)

        assert 'names' in response

    def test_info_node_positive_with_host(self):
        """
        Test info with correct host
        """

        host = self.host_name
        response = self.as_connection.info_node('bins', host)

        assert 'names' in response

    def test_info_node_positive_with_all_parameters(self):
        """
        Test info with all parameters
        """
        policy = {
            'timeout': 1000
        }
        host = self.host_name
        response = self.as_connection.info_node('logs', host, policy)

        assert response is not None

    def test_info_node_with_unicode_request_string_and_host_name(self):
        """
        Test info with all parameters
        """
        if TestBaseClass.tls_in_use():
            host = (as_unicode(self.connection_config['hosts'][0][0]),
                    self.connection_config['hosts'][0][1],
                    as_unicode(self.connection_config['hosts'][0][2]))
        else:
            host = (as_unicode(self.connection_config['hosts'][0][0]),
                    self.connection_config['hosts'][0][1])
        policy = {
            'timeout': 1000
        }
        response = self.as_connection.info_node(u'logs', host, policy)
        assert response is not None


# Tests for incorrect usage
@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
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

    def test_info_node_positive_invalid_host(self):
        """
        Test info with incorrect host
        """
        host = ("abcderp", 3000)
        with pytest.raises(e.InvalidHostError) as err_info:
            self.as_connection.info_node('bins', host)

        assert (err_info.value.code ==
                AerospikeStatus.AEROSPIKE_INVALID_HOST)

    def test_info_node_hostname_too_long(self):
        """
        Test info with incorrect host
        """
        host = ("localhost" * 100, 3000)
        with pytest.raises(e.InvalidHostError) as err_info:
            self.as_connection.info_node('bins', host)

    def test_info_node_positive_without_connection(self):
        """
        Test info with correct arguments without connection
        """
        client1 = aerospike.client(self.connection_config)
        with pytest.raises(e.ClusterError) as err_info:
            client1.info_node('bins', self.connection_config['hosts'][0][:2])

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

    def test_info_node_positive_with_incorrect_host(self):
        """
        Test info with incorrect host
        """
        host = ("127.0.0.1", 4500)
        with pytest.raises(e.ConnectionError) as err_info:
            self.as_connection.info_node('bins', host)

    @pytest.mark.parametrize(
        "command",
        (None, 5, ["info"], {}, False))
    def test_info_node_for_invalid_command_type(self, command):
        """
        Test info for None command
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info_node(
                command, self.connection_config['hosts'][0][:2])

    @pytest.mark.skipif(TestBaseClass.auth_in_use(), reason="this will cause an auth error")
    @pytest.mark.parametrize(
        "port",
        (None, "5", [5], {}, 3000.0)
    )
    def test_info_node_for_invalid_port_type(self, port):
        """
        Test info for invalid port types
        """
        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.info_node(
                "info", ("localhost", port))

    @pytest.mark.parametrize(
        "hostname",
        (None, 5, ["localhost"], {}, 3000.0)
    )
    def test_info_node_for_invalid_hostname_type(self, hostname):
        """
        Test info for invalid hostname types
        """
        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.info_node(
                "info", (hostname, 3000))

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

    @pytest.mark.parametrize("host",
                             ([(3000, 3000)], [], '123.456:1000', 3000, None))
    def test_info_node_positive_with_incorrect_host_type(self, host):
        """
        Test info with incorrect policy
        """

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info_node('bins', host)
