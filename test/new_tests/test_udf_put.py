# -*- coding: utf-8 -*-

import pytest
import sys
from .as_status_codes import AerospikeStatus
from .udf_helpers import wait_for_udf_removal, wait_for_udf_to_exist
from .test_base_class import TestBaseClass
from aerospike import exception as e

aerospike = pytest.importorskip("aerospike")
try:
    import aerospike
except:
    print("Please install aerospike python client.")
    sys.exit(1)


@pytest.mark.usefixtures("as_connection")
class TestUdfPut(TestBaseClass):

    def setup_class(cls):
        cls.udf_name = 'example.lua'

    def teardown_method(self, method):
        """
        Teardown method
        """
        udf_list = self.as_connection.udf_list()
        for udf in udf_list:
            if udf['name'] == self.udf_name:
                self.as_connection.udf_remove(self.udf_name)
                wait_for_udf_removal(
                    self.as_connection, self.udf_name)

    def test_udf_put_with_proper_parameters_no_policy(self):
        """
        Test to verify execution of udf_put with proper parameters,
        and an empty policy dict
        """

        filename = self.udf_name
        udf_type = 0

        status = self.as_connection.udf_put(filename, udf_type)

        assert status == 0
        udf_list = self.as_connection.udf_list({})

        present = False
        for udf in udf_list:
            if self.udf_name == udf['name']:
                present = True
                break

        assert present

    def test_udf_put_with_proper_timeout_policy_value(self):
        """
        Test that calling udf_put with the proper arguments will add the udf
        to the server
        """

        policy = {'timeout': 1000}
        filename = self.udf_name
        udf_type = 0

        status = self.as_connection.udf_put(filename, udf_type, policy)

        assert status == 0

        udf_list = self.as_connection.udf_list({})

        present = False
        for udf in udf_list:
            if udf['name'] == filename:
                present = True
                break

        assert present

    def test_udf_put_with_filename_unicode(self):

        policy = {}
        filename = u"example.lua"
        udf_type = 0

        status = self.as_connection.udf_put(filename, udf_type, policy)

        assert status == 0

        # wait for the udf to propagate to the server
        wait_for_udf_to_exist(self.as_connection, filename)
        udf_list = self.as_connection.udf_list({})

        present = False
        for udf in udf_list:
            if 'example.lua' == udf['name']:
                present = True

        assert present

    def test_udf_put_empty_script_file(self):

        policy = {}
        filename = "empty.lua"
        udf_type = 0
        with pytest.raises(e.LuaFileNotFound):
            status = self.as_connection.udf_put(filename, udf_type, policy)

    def test_udf_put_with_filename_too_long(self):

        policy = {}
        filename = "a" * 510 + ".lua"
        udf_type = 0
        with pytest.raises(e.ParamError):
            status = self.as_connection.udf_put(filename, udf_type, policy)

    def test_udf_put_with_empty_filename(self):

        policy = {}
        filename = ""
        udf_type = 0
        with pytest.raises(e.ParamError):
            status = self.as_connection.udf_put(filename, udf_type, policy)

    def test_udf_put_with_empty_filename_beginning_with_slash(self):

        policy = {}
        filename = "/"
        udf_type = 0
        with pytest.raises(e.ParamError):
            status = self.as_connection.udf_put(filename, udf_type, policy)

    def test_udf_put_with_proper_parameters_without_connection(self):

        policy = {}
        filename = self.udf_name
        udf_type = 0

        config = {'hosts': [('127.0.0.1', 3000)]}

        client1 = aerospike.client(config)

        with pytest.raises(e.ClusterError) as err_info:
            client1.udf_put(filename, udf_type, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_CLUSTER_ERROR

    def test_udf_put_with_invalid_timeout_policy_value(self):
        """
        Test that invalid timeout policy causes an error on udf put
        """
        policy = {'timeout': 0.1}
        filename = self.udf_name
        udf_type = 0

        with pytest.raises(e.ParamError) as err_info:
            self.as_connection.udf_put(filename, udf_type, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_PARAM

    def test_udf_put_without_parameters(self):
        """
        Test that calling udf_put without parameters raises
        an error
        """
        with pytest.raises(TypeError) as typeError:
            self.as_connection.udf_put()

        assert "argument 'filename' (pos 1)" in str(
            typeError.value)

    def test_udf_put_with_non_existent_filename(self):
        """
        Test that an error is raised when an invalid filename
        is given to udf_put
        """
        policy = {}
        filename = "somefile_that_does_not_exist"
        udf_type = 0

        with pytest.raises(e.LuaFileNotFound) as err_info:
            self.as_connection.udf_put(filename, udf_type, policy)

        assert err_info.value.code == AerospikeStatus.LUA_FILE_NOT_FOUND

    def test_udf_put_with_non_lua_udf_type_and_lua_script_file(self):
        """
        Test to verify that an invalid udf_type causes an error
        """
        policy = {'timeout': 0}
        filename = self.udf_name
        udf_type = 1

        with pytest.raises(e.ClientError) as err_info:
            self.as_connection.udf_put(filename, udf_type, policy)

        assert err_info.value.code == AerospikeStatus.AEROSPIKE_ERR_CLIENT

    def test_udf_put_with_all_none_parameters(self):
        """
        Test to verify that calling udf_put with all 3 parameters as None
        will raise an error
        """

        with pytest.raises(TypeError) as exception:
            self.as_connection.udf_put(None, None, None)

        assert "an integer is required" in str(exception.value)

    @pytest.mark.parametrize(
        "filename, ftype, policy",
        (
            (1, 0, {}),
            (None, 0, {}),
            ((), 0, {}),
            ('example.lua', '0', {}),
            ('example.lua', (), {}),
            ('example.lua', None, {}),
            ('example.lua', 0, []),
            ('example.lua', 0, 'policy'),
            ('example.lua', 0, 5)

        )
    )
    def test_udf_put_invalid_arg_types(self, filename, ftype, policy):
        '''
        Incorrect type for second argument raise a type error,
        the others cause a param error
        '''
        with pytest.raises((e.ParamError, TypeError)):
            self.as_connection.udf_put(filename, ftype, policy)

    def test_udf_put_with_extra_arg(self):
        policy = {}
        with pytest.raises(TypeError):
            self.as_connection.udf_put(
                self.udf_name, 1, policy, 'extra_arg')
