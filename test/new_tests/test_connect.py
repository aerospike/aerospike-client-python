# -*- coding: utf-8 -*-

import pytest
import sys
import json
from contextlib import contextmanager
from .test_base_class import TestBaseClass
from aerospike import exception as e
aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@contextmanager
def open_as_connection(config):
    """
    Context manager to let us open aerospike connections with
    specified config
    """
    as_connection = TestBaseClass.get_new_connection(config)

    # Connection is setup, so yield it
    yield as_connection

    # close the connection
    as_connection.close()


# adds cls.connection_config to this class
@pytest.mark.usefixtures("connection_config")
class TestConnect(object):

    def test_version(self):
        """
            Check for aerospike vrsion
        """
        assert aerospike.__version__ is not None

    def test_connect_positive(self):
        """
            Invoke connect() with positive parameters.
        """
        config = self.connection_config.copy()

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()

    def test_connect_positive_with_policy(self):
        """
            Invoke connect() with positive parameters and policy.
        """

        config = self.connection_config.copy()
        config['policies'] = {'read': {'total_timeout': 10000}}

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()

    @pytest.mark.skip(reason="This doesn't actually use multiple hosts")
    def test_connect_positive_with_multiple_hosts(self):
        """
            Invoke connect() with multiple hosts.
        """
        config = self.connection_config.copy()
        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()

    def test_connect_positive_unicode_hosts(self):
        """
            Invoke connect() with unicode hosts.
        """

        #  Convert to and from json, to force unicode
        #  This works in py 2.7 and 3.5
        uni = json.dumps(self.connection_config['hosts'][0])
        hostlist = json.loads(uni)
        config = {
            'hosts': [tuple(hostlist)]
        }
        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()

    @pytest.mark.skip(TestBaseClass.tls_in_use(),
                      reason="no default port for tls")
    def test_connect_hosts_missing_port(self):
        """
            Invoke connect() with missing port in config dict.
        """
        config = {
            'hosts': [('127.0.0.1')]
        }

        with open_as_connection(config) as client:
            assert client.is_connected()

    def test_connect_positive_shm_key(self):
        """
            Invoke connect() with shm_key specified
        """
        config = self.connection_config.copy()
        config['shm'] = {'shm_key': 3}

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()
            assert client.shm_key() == 3

    def test_connect_positive_shm_key_default(self):
        """
            Invoke connect() with shm enabled but shm_key not specified
        """
        config = self.connection_config.copy()
        config['shm'] = {'shm_max_nodes': 5}

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()
            assert client.shm_key() is not None

    def test_connect_positive_shm_not_enabled(self):
        """
            Invoke connect() with shm not anabled
        """
        config = self.connection_config.copy()

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()
            assert client.shm_key() is None

    @pytest.mark.skip(reason=("This raises an error," +
                              " but it isn't clear whether it should"))
    def test_connect_positive_cluster_name(self):
        """
            Invoke connect() giving a cluster name
        """
        config = self.connection_config.copy()
        config['cluster_name'] = 'test-cluster'

        with pytest.raises(e.ClientError) as err_info:
            self.client = aerospike.client(config).connect()

        assert err_info.value.code == -1

    def test_connect_positive_reconnect(self):
        """
            Connect/Close/Connect to client
        """
        config = self.connection_config.copy()

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()
            client.close()
            assert client.is_connected() is False
            if TestBaseClass.user is None and TestBaseClass.password is None:
                client.connect()
            else:
                client.connect(TestBaseClass.user, TestBaseClass.password)
            assert client.is_connected()

    def test_connect_on_connected_client(self):
        """
            Invoke connect on a client that is already connected.
        """
        config = self.connection_config.copy()

        with open_as_connection(config) as client:
            assert client is not None
            assert client.is_connected()
            client.connect()
            assert client.is_connected()

    # Tests for invalid Usage

    def test_connect_with_extra_args(self):
        with pytest.raises(TypeError):
            client = aerospike.client(self.connection_config)
            client.connect("username", "password", "extra arg")

    @pytest.mark.parametrize(
        "config, err, err_code, err_msg",
        [
            (1, e.ParamError, -2, "Config must be a dict"),
            ({}, e.ParamError, -2, "Hosts must be a list"),
            ({'': [('127.0.0.1', 3000)]},
             e.ParamError, -2, "Hosts must be a list"),
            ({'hosts': [3000]}, e.ParamError, -2, "Invalid host"),

            ({'hosts': [('127.0.0.1', 2000)]}, e.ClientError, -10,
             "Failed to connect"),
            ({'hosts': [('127.0.0.1', '3000')]}, e.ClientError, -10,
             "Failed to connect")
        ],
        ids=[
            "config not dict",
            "config empty dict",
            "config missing hosts key",
            "hosts missing address",
            "hosts port is incorrect",
            "hosts port is string"
        ]
    )
    def test_connect_invalid_configs(self, config, err, err_code, err_msg):
        with pytest.raises(err) as err_info:
            self.client = aerospike.client(config).connect()

        assert err_info.value.code == err_code
        assert err_info.value.msg == err_msg
