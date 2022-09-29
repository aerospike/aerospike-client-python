# -*- coding: utf-8 -*-
from __future__ import print_function

import sys

from distutils.version import LooseVersion
import pytest
from .as_status_codes import AerospikeStatus
from .udf_helpers import wait_for_udf_removal, wait_for_udf_to_exist
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except ImportError:
    print("Please install aerospike python client.")
    sys.exit(1)


def is_greater_451(version_str):
    '''
    Is the server version 4.5.1.0-pre or newer
    '''
    return LooseVersion(version_str) >= LooseVersion("4.5.1")


class TestUdfRemove(object):

    @pytest.fixture(autouse=True)
    def setup(self, request, as_connection):
        """
        Setup Method, adds a UDF and waits for the UDF to exist
        """
        as_connection.udf_put(self.udf_name, 0, {})
        wait_for_udf_to_exist(as_connection, self.udf_name)

        def teardown():
            """
            Teardown Method, Checks to see if the UDF is on the server,
            if it is: removes it and waits for the removal
            process to complete
            """
            udf_name = TestUdfRemove.udf_name
            udf_list = as_connection.udf_list({'timeout': 100})

            for udf in udf_list:
                if udf['name'] == udf_name:
                    as_connection.udf_remove(udf_name)
                    wait_for_udf_removal(as_connection, udf_name)
                    break

        request.addfinalizer(teardown)

    def setup_class(cls):
        """
        Setup class, sets the name of the example UDF used in the tests
        """
        cls.udf_name = u'example.lua'

    def test_udf_remove_with_no_policy(self):
        """
        Test to verify a proper call to udf_remove will  remove a UDF
        """
        module = "example.lua"

        status = self.as_connection.udf_remove(module)
        assert status == AerospikeStatus.AEROSPIKE_OK

        wait_for_udf_removal(self.as_connection, module)
        udf_list = self.as_connection.udf_list({'timeout': 100})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert not present

    @pytest.mark.xfail(reason="This is the only method which allows" +
                       " invalid timeout")
    def test_udf_remove_with_invalid_timeout_policy_value(self):
        """
        Verify that an incorrect timeout policy will not prevent UDF removal
        """
        policy = {'timeout': 0.1}
        module = "example.lua"

        with pytest.raises(e.ParamError):
            status = self.as_connection.udf_remove(module, policy)

        #  Wait for the removal to take place
        wait_for_udf_removal(self.as_connection, module)

        assert status == 0

    def test_udf_remove_with_proper_timeout_policy_value(self):
        """
        Verify that udf_remove with a correct timeout policy argument
        functions.
        """
        policy = {'timeout': 1000}
        module = "example.lua"

        status = self.as_connection.udf_remove(module, policy)

        assert status == AerospikeStatus.AEROSPIKE_OK

        #  Wait for the removal to take place
        wait_for_udf_removal(self.as_connection, module)

        udf_list = self.as_connection.udf_list({'timeout': 0})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert not present

    def test_udf_remove_with_unicode_filename(self):
        """
        Test to ensure that unicode filenames may be used to remove UDFs
        """
        policy = {'timeout': 100}
        module = u"example.lua"
        status = self.as_connection.udf_remove(module, policy)

        assert status == AerospikeStatus.AEROSPIKE_OK

        #  Wait for the removal to take place
        wait_for_udf_removal(self.as_connection, module)

        udf_list = self.as_connection.udf_list({'timeout': 100})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert not present


@pytest.mark.usefixtures("connection_with_udf")
class TestIncorrectCallsToUDFRemove(object):
    """
    These are all tests where udf_remove fails for various reasons,
    So we skip removing and re-adding the UDF before and after each test
    """
    def setup_class(cls):
        """
        setup the class attribute indicating the udf to load
        """
        cls.udf_to_load = "example.lua"

    def test_udf_remove_with_proper_parameters_without_connection(self):
        """
        Test to verify that attempting to remove a UDF before connection
        raises an error
        """
        config = {'hosts': [('127.0.0.1', 3000)]}

        client1 = aerospike.client(config)
        policy = {'timeout': 100}
        module = "example.lua"

        with pytest.raises(e.ClusterError) as err_info:
            client1.udf_remove(module, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_udf_remove_with_non_existent_module(self):
        """
        Test to ensure that the removal of a non existant UDF raises an Error
        """
        policy = {}
        module = "some_fake_module_that_does_not_exist"

        if is_greater_451(self.string_server_version):
            self.as_connection.udf_remove(module, policy)
        else:
            with pytest.raises(e.UDFError) as err_info:
                self.as_connection.udf_remove(module, policy)


    def test_udf_remove_without_parameters(self):
        """
        Test to verify that udf_remove raises an error with no parameters
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.udf_remove()
        assert "argument 'filename' (pos 1)" in str(
            typeError.value)

    def test_udf_remove_with_none_as_parameters(self):
        """
        Test to verify that udf_remove raises and error when None is
        used as parameters
        """
        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.udf_remove(None, None)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    @pytest.mark.parametrize('udf', [None, False, 1, 1.5,
                                     ('cool', 'other')])
    def test_udf_with_non_string_module_name(self, udf):
        '''
        Tests for incorrect udf module name types
        '''
        with pytest.raises(e.ParamError):
            self.as_connection.udf_remove(udf)

    @pytest.mark.xfail(reason="These do not raise errors")
    @pytest.mark.parametrize(
        "policy",
        [False, 'policy', 5, (), []]
    )
    def test_udf_remove_with_invalid_policy_type(self, policy):
        '''
        Tests for incorrect policy argument types
        '''
        with pytest.raises(TypeError):
            self.as_connection.udf_remove('example.lua', policy)
