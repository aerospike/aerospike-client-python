
# -*- coding: utf-8 -*-

import pytest
import sys
import cPickle as pickle
try:
    import aerospike
except:
    print "Please install aerospike python client."
    sys.exit(1)

class TestConnect(object):

    def test_connect_positive(self):
        """
            Invoke connect() with positive parameters.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)]
                }
        self.client = aerospike.client(config).connect()
        assert self.client != None
        self.client.close()

    def test_connect_positive_with_policy(self):
        """
            Invoke connect() with positive parameters and policy.
        """
        config = {
                'hosts': [('127.0.0.1', 3000)],
                'policies': {
                   'timeout': 10000
                }
                }
        self.client = aerospike.client(config).connect()
       
        assert self.client != None
        self.client.close()

    def test_connect_positive_with_multiple_hosts(self):
        """
            Invoke connect() with multiple hosts.
        """
        config = {
                'hosts': [('127.0.0.1', 3000),('127.0.0.1', 3010)],
                }
        self.client = aerospike.client(config).connect()
        assert self.client != None
        self.client.close()

    def test_connect_config_not_dict(self):
        """
            Invoke connect with config as Integer (non-dict)
        """
        config = 1
        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()

        assert exception.value[0] == -1
        assert exception.value[1] == "Parameters are incorrect"
    
    def test_connect_config_empty_dict(self):
        """
            Invoke connect() with config as empty dict.
        """
        config = {
                }
        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()

        assert exception.value[0] == 11
        assert exception.value[1] == 'no hosts provided'

    def test_connect_missing_hosts_key(self):
        """
            Invoke connect() with host key missing in config dict.
        """
        config = {
                '': [('127.0.0.1', 3000)]
                }
        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()

        assert exception.value[0] == 11
        assert exception.value[1] == 'no hosts provided'

    def test_connect_missing_address(self):
        """
            Invoke connect() with missing address in config dict.
        """
        config = {
                'hosts': [3000]
                }
        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()

        assert exception.value[0] == 11
        assert exception.value[1] == 'no hosts provided'

    def test_connect_missing_port(self):
        """
            Invoke connect() with missing port in config dict.
        """
        config = {
                'hosts': ['127.0.0.1']
                }
        self.client = aerospike.client(config).connect()

        assert self.client != None
        self.client.close()

    def test_connect_incorrect_port(self):
        """
            Invoke connect() with incorrect port in config dict.
        """
        config = {
                'hosts': [('127.0.0.1', 2000)]
                }
        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()

        assert exception.value[0] == -1
        assert exception.value[1] == 'AEROSPIKE_ERR_CLIENT'

    def test_connect_port_is_string(self):
        """
            Invoke connect() with port as string in config dict.
        """
        config = {
                'hosts': [('127.0.0.1', '3000')]
                }
        with pytest.raises(Exception) as exception:
            self.client = aerospike.client(config).connect()

        assert exception.value[0] == -1
        assert exception.value[1] == 'AEROSPIKE_ERR_CLIENT'
