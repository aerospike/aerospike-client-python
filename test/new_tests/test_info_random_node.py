# -*- coding: utf-8 -*-
import pytest
import time
import sys
import socket

from .test_base_class import TestBaseClass
from .as_status_codes import AerospikeStatus
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)

@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfoSingleNode(object):
    pytest_skip = False

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection, connection_config):
        key = ('test', 'demo', 'list_key')
        rec = {'names': ['John', 'Marlen', 'Steve']}
        self.as_connection.put(key, rec)

        yield

        self.as_connection.remove(key)

    def test_info_random_node_positive(self):
        """
        Test info with correct arguments.
        """
        if self.pytest_skip:
            pytest.xfail()
        response = self.as_connection.info_random_node('bins')

        assert 'names' in response

    def test_info_random_node_positive_for_namespace(self):
        """
        Test info with 'namespaces' as the command.
        """
        if self.pytest_skip:
            pytest.xfail()
        response = self.as_connection.info_random_node('namespaces')

        assert 'test' in response

    def test_info_random_node_positive_for_sets(self):
        """
        Test info with 'sets' as the command.
        """
        if self.pytest_skip:
            pytest.xfail()
        response = self.as_connection.info_random_node('sets')

        assert 'demo' in response

    def test_info_random_node_positive_for_sindex_creation(self):
        """
        Test creating an index through an info call.
        """
        if self.pytest_skip:
            pytest.xfail()
        try:
            self.as_connection.index_remove('test', 'names_test_index')
            time.sleep(2)
        except:
            pass

        self.as_connection.info_random_node(
            'sindex-create:ns=test;set=demo;indexname=names_test_index;indexdata=names,string')
        time.sleep(2)

        response = self.as_connection.info_random_node('sindex')
        self.as_connection.index_remove('test', 'names_test_index')
        assert 'names_test_index' in response

    def test_info_random_node_positive_with_correct_policy(self):
        """
        Test info call with bins as command and a timeout policy.
        """
        if self.pytest_skip:
            pytest.xfail()
        policy = {'timeout': 1000}
        response = self.as_connection.info_random_node('bins', policy=policy)

        assert 'names' in response

    def test_info_random_node_positive_with_host(self):
        """
        Test info with correct host.
        """
        if self.pytest_skip:
            pytest.xfail()
        response = self.as_connection.info_random_node('bins')

        assert 'names' in response

    def test_info_random_node_positive_with_all_parameters(self):
        """
        Test info with all parameters.
        """
        if self.pytest_skip:
            pytest.xfail()
        policy = {
            'timeout': 1000
        }
        response = self.as_connection.info_random_node('logs', policy=policy)

        assert response is not None

# Tests for incorrect usage
@pytest.mark.xfail(TestBaseClass.temporary_xfail(), reason="xfail variable set")
@pytest.mark.usefixtures("as_connection", "connection_config")
class TestInfoRandomNodeIncorrectUsage(object):
    """
    Tests for invalid usage of the the info_random_node method.
    """
    def test_info_random_node_no_parameters(self):
        """
        Test info with no parameters.
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.info_random_node()
        assert "argument 'command' (pos 1)" in str(
            typeError.value)

    def test_info_random_node_for_incorrect_command(self):
        """
        Test info for incorrect command.
        """
        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.info_random_node(
                'abcd')

    def test_info_random_node_positive_without_connection(self):
        """
        Test info with correct arguments without connection.
        """
        client1 = aerospike.client(self.connection_config)
        with pytest.raises(e.ClusterError) as err_info:
            client1.info_random_node('bins')

        assert err_info.value.code == 11
        assert err_info.value.msg == 'No connection to aerospike cluster.'

    def test_info_random_node_positive_with_extra_parameters(self):
        """
        Test info with extra parameters.
        """
        host = self.connection_config['hosts'][0]
        policy = {'timeout': 1000}
        with pytest.raises(TypeError) as typeError:
            self.as_connection.info_random_node('bins', policy, "")

        assert "info_random_node() takes at most 2 arguments (3 given)" in str(
            typeError.value)

    @pytest.mark.parametrize(
        "command",
        (None, 5, ["info"], {}, False))
    def test_info_random_node_for_invalid_command_type(self, command):
        """
        Test info for None command.
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info_random_node(command)

    def test_info_random_node_positive_with_incorrect_policy(self):
        """
        Test info with incorrect policy.
        """
        policy = {
            'timeout': 0.5
        }
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.info_random_node('bins', policy)

        assert err_info.value.code == -2
        assert err_info.value.msg == "timeout is invalid"