# -*- coding: utf-8 -*-
import pytest
import sys
from aerospike import exception as e
from .test_base_class import TestBaseClass
from datetime import datetime

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


class TestUdfList(object):

    def setup_class(cls):
        """
        Setup class
        """
        cls.client = TestBaseClass.get_new_connection()
        cls.client.udf_put('example.lua', 0, {})

    def teardown_class(cls):
        """
        Remove the UDF we added, and close the connection
        """
        cls.client.udf_remove("example.lua")
        cls.client.close()

    def test_udf_list_without_any_parameters(self):
        """
        Test successful call to udf_list with no parameters
        """
        udf_list = self.client.udf_list()
        assert type(udf_list) is list
        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert present

    def test_udf_list_with_proper_parameters(self):
        """
        Test successful call to udf_list with a policy supplied
        """
        policy = {'timeout': 0}
        udf_list = self.client.udf_list(policy)

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert present

    def test_udf_list_with_none_type_policy(self):
        '''
        Test calling udf_list with a None type policy
        '''
        policy = None
        udf_list = self.client.udf_list(policy)

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert present

    def test_udf_list_with_timeout_policy(self):
        """
        Test successful call to udf_list with a timeout of 1s
        """
        policy = {'timeout': 1000}

        udf_list = self.client.udf_list(policy)

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert present

    def test_udf_list_with_invalid_timeout_policy_value(self):
        """
        Test to verify error raised by passing an invalid timeout value
        to udf_list
        """
        policy = {'timeout': 0.1}

        with pytest.raises(e.ParamError) as param_error:
            self.client.udf_list(policy)

        assert param_error.value.code == -2
        assert param_error.value.msg == 'timeout is invalid'

    def test_udf_list_with_extra_arg(self):
        policy = {'timeout': 3000}
        with pytest.raises(TypeError) as err_info:
            self.client.udf_list(policy, 'extra_parameter')

    @pytest.mark.parametrize(
        'policy',
        [
            False,
            1000,
            [],
            (),
            'timeout:5000'
        ]
    )
    def test_udf_list_with_wrong_policy_type(self, policy):
        '''
        Tests for behavior caused by calling udf_list with
        the wrong type of policy
        '''
        with pytest.raises(e.ParamError):
            self.client.udf_list(policy)

    def test_udf_list_with_proper_parameters_without_connection(self):
        """
        Test to verify error raised by trying to call udf_list without
        first calling connect
        """
        config = {'hosts': [('127.0.0.1', 3000)]}

        client1 = aerospike.client(config)

        policy = {'timeout': 0}

        with pytest.raises(e.ClusterError) as cluster_error:
            client1.udf_list(policy)

        assert cluster_error.value.code == 11
        assert cluster_error.value.msg == 'No connection to aerospike cluster'
